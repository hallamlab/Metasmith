#!/usr/bin/env python3
"""Low-effort probes over the collected outputs, for the separate test session. None of them is the test.

    probes.py <repo root>

Writes data/mm_compare/probes/:
  ecoli_gem_anchor.tsv     CarveMe against the curated GEM by BiGG reaction id, the 4-lane against it by MNXR
  ecoli_strain_pairs.tsv   per strain pair, the size of each method's set difference
  ecoli_memote_flip.tsv    the MEMOTE tests behind each strain's total
  cami_sample_overlap.tsv  per sample, CarveMe's MetaCyc and EC ids (all MAGs) inside the community PGDB's
  cami_gpr_vs_carveme.tsv  per MAG, the 4-lane and CarveMe on the same ORFs (once the 4-lane tables exist)
  gene_overlap.tsv         per model, CarveMe's genes against the ORFs the 4-lane called, by ORF id
  ecoli_pgdb_routes.tsv    per strain and PGDB route pair, the overlap of gene-linked MetaCyc reactions,
                           and each route's and CarveMe's recall of EcoCyc's for K-12
  ecoli_pgdb_strain_pairs.tsv  per strain pair, each PGDB route's set difference
  ecoli_pgdb_genes.tsv     the metasmith PGDB's enzyme genes against CarveMe's and the 4-lane's, by protein id
Prints the headline numbers.
"""
import gzip
import itertools
import json
import sys
from pathlib import Path

import re
import xml.etree.ElementTree as ET

import pandas as pd

STRAINS = ("k12", "w3110", "bw25113", "dh1", "dh10b", "epi300")
GEM_STRAINS = ("k12", "dh10b", "epi300")
LANES = ("clean", "kofam", "pbert", "uniref50")


def ids(series, strip=None):
    s = series.dropna().str.split("|").explode()
    return set(s.str.replace(strip, "", regex=True) if strip else s)


def spearman(a, b):
    return a.rank().corr(b.rank())


def ecoli(repo, mm, out):
    rx = pd.read_csv(mm / "inventory" / "carveme_reactions.tsv.gz", sep="\t")
    rx = rx[rx.dataset == "ecoli"].assign(bigg=lambda d: d.reaction.str.replace(r"^R_", "", regex=True))
    runs = repo / "data" / "fabfos" / "runs"
    gpr = {s: pd.read_parquet(runs / f"e_coli_{s}" / "gpr" / "gpr_denovo.parquet") for s in STRAINS}
    carve = {s: set(rx[rx.subject == s].bigg) for s in STRAINS}
    carve_ruled = {s: set(rx[(rx.subject == s) & (rx.gene_rule == 1)].bigg) for s in STRAINS}

    rows = []
    for s in GEM_STRAINS:
        gem = pd.read_parquet(runs / f"e_coli_{s}" / "gpr" / "gpr_gem.parquet")
        ruled = set(gem[gem.feature_kind == "gem_gene"].intermediate_id)
        gem_mnxr = set(gem.mnxr.dropna())
        row = {"strain": s, "gem": gem.source.iloc[0], "gem_gene_ruled": len(ruled),
               "carveme_gene_ruled": len(carve_ruled[s]), "shared": len(ruled & carve_ruled[s]),
               "of_gem": len(ruled & carve_ruled[s]) / len(ruled),
               "of_carveme": len(ruled & carve_ruled[s]) / len(carve_ruled[s]), "gem_mnxr": len(gem_mnxr)}
        for lane in (*LANES, "all"):
            calls = gpr[s] if lane == "all" else gpr[s][gpr[s].channel == lane]
            row[f"4lane_{lane}_of_gem_mnxr"] = len(set(calls.mnxr) & gem_mnxr) / len(gem_mnxr)
        cm = ids(rx[rx.subject == s].mnxr)
        row |= {"carveme_mnxr": len(cm), "carveme_mnxr_in_gem": len(cm & gem_mnxr),
                "carveme_mnxr_in_4lane": len(cm & set(gpr[s].mnxr))}
        rows.append(row)
    anchor = pd.DataFrame(rows)
    anchor.to_csv(out / "ecoli_gem_anchor.tsv", sep="\t", index=False)

    sets = {"carveme": carve, "carveme_gene_ruled": carve_ruled,
            "4lane_all": {s: set(g.mnxr) for s, g in gpr.items()},
            **{f"4lane_{l}": {s: set(g[g.channel == l].mnxr) for s, g in gpr.items()} for l in LANES}}
    pairs = pd.DataFrame([{"pair": f"{a}-{b}", **{k: len(v[a] ^ v[b]) for k, v in sets.items()}}
                          for a, b in itertools.combinations(STRAINS, 2)])
    pairs.to_csv(out / "ecoli_strain_pairs.tsv", sep="\t", index=False)

    flip = []
    for s in STRAINS:
        t = json.load(gzip.open(mm / "ecoli_carveme" / s / "memote_results.json.gz"))["tests"]
        flip.append({"strain": s, "memote_total": json.loads((mm / "ecoli_carveme" / s / "memote.json").read_text())["total_score"],
                     "stoichiometric_consistency": t["test_stoichiometric_consistency"]["result"],
                     "unconserved_metabolites": "|".join(sorted(t["test_unconserved_metabolites"]["data"])),
                     "atp_generating_cycle": t["test_detect_energy_generating_cycles"]["result"].get("MNXM3")})
    pd.DataFrame(flip).to_csv(out / "ecoli_memote_flip.tsv", sep="\t", index=False)

    print(anchor[["strain", "gem", "of_gem", "of_carveme", "4lane_all_of_gem_mnxr", "carveme_mnxr_in_4lane"]].round(2).to_string(index=False))
    print("strain-pair Spearman, CarveMe gene-ruled vs 4-lane all:",
          round(spearman(pairs.carveme_gene_ruled, pairs["4lane_all"]), 2))
    print(pairs.sort_values("4lane_all").head(3).to_string(index=False))


def cami(mm, out):
    sm = pd.read_csv(mm / "sample_map.tsv", sep="\t")
    rx = pd.read_csv(mm / "inventory" / "carveme_reactions.tsv.gz", sep="\t")
    rx = rx[rx.dataset == "cami_hmp"]
    pg = pd.read_csv(mm / "inventory" / "pgdb_models.tsv", sep="\t")
    rows = []
    for _, s in sm.iterrows():
        c = rx[rx["sample"] == s.e5_sample]
        c_meta, c_ec = ids(c[c.gene_rule == 1].metacyc), ids(c.ec)
        tables = mm / "ryan_pgdb_tables"
        prx = pd.read_csv(tables / f"{s.ryan_sample}.reactions.tsv.gz", sep="\t")
        pgn = pd.read_csv(tables / f"{s.ryan_sample}.reaction_genes.tsv.gz", sep="\t")
        p_meta = set(pgn[pgn.pgdb == "community"].reaction)
        p_ec = ids(prx[prx.pgdb == "community"].ec, strip=r"^EC-")
        rows.append({"sample": s.ryan_sample, "mags": s.e5_mags,
                     "genomes": int(((pg["sample"] == s.ryan_sample) & (pg.level == "genome")).sum()),
                     "carveme_reactions": c.reaction.nunique(), "pgdb_community_reactions_with_gene": len(p_meta),
                     "carveme_metacyc": len(c_meta), "metacyc_shared": len(c_meta & p_meta),
                     "metacyc_carveme_in_pgdb": len(c_meta & p_meta) / len(c_meta),
                     "carveme_ec": len(c_ec), "pgdb_ec": len(p_ec), "ec_shared": len(c_ec & p_ec),
                     "ec_carveme_in_pgdb": len(c_ec & p_ec) / len(c_ec)})
    d = pd.DataFrame(rows)
    d.to_csv(out / "cami_sample_overlap.tsv", sep="\t", index=False)
    print(d[["metacyc_carveme_in_pgdb", "ec_carveme_in_pgdb"]].describe().round(3).T[["min", "50%", "max"]].to_string())
    for a, b in [("mags", "genomes"), ("carveme_reactions", "pgdb_community_reactions_with_gene"),
                 ("mags", "carveme_reactions"), ("genomes", "pgdb_community_reactions_with_gene")]:
        print(f"Spearman {a} vs {b}: {spearman(d[a], d[b]):.2f}")


def cami_gpr(mm, out):
    gdir = mm / "gpr_4lane"
    if not gdir.exists():
        print("cami_gpr: no 4-lane tables yet")
        return
    rx = pd.read_csv(mm / "inventory" / "carveme_reactions.tsv.gz", sep="\t")
    rx = rx[rx.dataset == "cami_hmp"]
    rows = []
    for f in sorted(gdir.glob("toy_hmp_*.parquet")):
        g = pd.read_parquet(f)
        for mag, calls in g.groupby("mag"):
            c = rx[(rx["sample"] == f.stem) & (rx.subject == mag)]
            c_mnxr = ids(c.mnxr)
            row = {"sample": f.stem, "mag": mag, "carveme_mnxr": len(c_mnxr), "4lane_mnxr": calls.mnxr.nunique(),
                   "carveme_mnxr_in_4lane": len(c_mnxr & set(calls.mnxr)) / max(1, len(c_mnxr))}
            for lane in LANES:
                row[f"carveme_mnxr_in_{lane}"] = len(c_mnxr & set(calls[calls.channel == lane].mnxr)) / max(1, len(c_mnxr))
            rows.append(row)
    d = pd.DataFrame(rows)
    d.to_csv(out / "cami_gpr_vs_carveme.tsv", sep="\t", index=False)
    print(d.drop(columns=["sample", "mag"]).describe().round(3).T[["min", "50%", "max"]].to_string())


FBC = "{http://www.sbml.org/sbml/level3/version1/fbc/version2}"


def norm(orf):
    return re.sub(r"[^A-Za-z0-9]", "_", orf)


def carveme_genes(sbml: Path):
    return {norm(g.get(f"{FBC}id")[2:]) for g in ET.parse(sbml).getroot().iter(f"{FBC}geneProduct")}


def gene_overlap(repo, mm, out):
    rows = []
    runs = repo / "data" / "fabfos" / "runs"
    for s in STRAINS:
        g = pd.read_parquet(runs / f"e_coli_{s}" / "gpr" / "gpr_denovo.parquet", columns=["orf", "channel"])
        rows.append(("ecoli", s, carveme_genes(mm / "ecoli_carveme" / s / "carveme_sbml.xml"), g))
    for f in sorted((mm / "gpr_4lane").glob("toy_hmp_*.parquet")):
        g = pd.read_parquet(f, columns=["orf", "channel", "mag"])
        for mag, calls in g.groupby("mag"):
            rows.append(("cami_hmp", mag, carveme_genes(mm / "e5_hmp_carveme" / f.stem / mag / "carveme_sbml.xml"), calls))
    out_rows = []
    for ds, subject, genes, calls in rows:
        called = {norm(o) for o in calls.orf}
        row = {"dataset": ds, "subject": subject, "carveme_genes": len(genes), "4lane_orfs": len(called),
               "carveme_genes_called": len(genes & called) / max(1, len(genes)),
               "4lane_orfs_in_carveme": len(genes & called) / max(1, len(called))}
        for lane in LANES:
            lane_called = {norm(o) for o in calls[calls.channel == lane].orf}
            row[f"carveme_genes_called_{lane}"] = len(genes & lane_called) / max(1, len(genes))
        out_rows.append(row)
    d = pd.DataFrame(out_rows)
    d.to_csv(out / "gene_overlap.tsv", sep="\t", index=False)
    print(d.groupby("dataset").median(numeric_only=True).round(3).T.to_string())


def tsv(path):
    return pd.read_csv(path, sep="\t", dtype=str)


def ecoli_pgdb(repo, mm, out):
    pg = mm / "ecoli_pgdb"
    routes = sorted(d.name for d in pg.iterdir() if d.is_dir())
    linked = {(r, s): set(tsv(pg / r / f"{s}.reaction_genes.tsv.gz").reaction)
              for r in routes for s in STRAINS if (pg / r / f"{s}.reaction_genes.tsv.gz").exists()}
    rx = pd.read_csv(mm / "inventory" / "carveme_reactions.tsv.gz", sep="\t")
    rx = rx[rx.dataset == "ecoli"]
    carve = {s: ids(rx[(rx.subject == s) & (rx.gene_rule == 1)].metacyc) for s in STRAINS}

    rows = []
    for s in STRAINS:
        have = [r for r in routes if (r, s) in linked]
        for a, b in itertools.combinations(have, 2):
            A, B = linked[(a, s)], linked[(b, s)]
            rows.append({"strain": s, "a": a, "b": b, "a_reactions": len(A), "b_reactions": len(B),
                         "shared": len(A & B), "jaccard": len(A & B) / len(A | B)})
        for r in have:
            A = linked[(r, s)]
            rows.append({"strain": s, "a": r, "b": "carveme_metacyc", "a_reactions": len(A),
                         "b_reactions": len(carve[s]), "shared": len(A & carve[s]),
                         "jaccard": len(A & carve[s]) / len(A | carve[s])})
    routes_df = pd.DataFrame(rows)
    eco = linked.get(("biocyc", "k12"), set())
    recall = [{"strain": "k12", "a": r, "b": "ecocyc_recall", "a_reactions": len(linked[(r, "k12")]),
               "b_reactions": len(eco), "shared": len(linked[(r, "k12")] & eco),
               "jaccard": len(linked[(r, "k12")] & eco) / len(eco)}
              for r in routes if r != "biocyc" and (r, "k12") in linked]
    recall.append({"strain": "k12", "a": "carveme_metacyc", "b": "ecocyc_recall", "a_reactions": len(carve["k12"]),
                   "b_reactions": len(eco), "shared": len(carve["k12"] & eco),
                   "jaccard": len(carve["k12"] & eco) / len(eco)})
    routes_df = pd.concat([routes_df, pd.DataFrame(recall)], ignore_index=True)
    routes_df.to_csv(out / "ecoli_pgdb_routes.tsv", sep="\t", index=False)
    print(routes_df.round(3).to_string(index=False))

    full = [r for r in routes if all((r, s) in linked for s in STRAINS)]
    pairs = pd.DataFrame([{"pair": f"{a}-{b}", **{r: len(linked[(r, a)] ^ linked[(r, b)]) for r in full}}
                          for a, b in itertools.combinations(STRAINS, 2)])
    prior = out / "ecoli_strain_pairs.tsv"
    if prior.exists():
        pairs = pairs.merge(pd.read_csv(prior, sep="\t")[["pair", "carveme_gene_ruled", "4lane_all"]], on="pair")
    pairs.to_csv(out / "ecoli_pgdb_strain_pairs.tsv", sep="\t", index=False)
    print(pairs.sort_values(full[0]).to_string(index=False))
    print(pairs.drop(columns="pair").rank().corr().round(2).to_string())

    if "metasmith_pathologic" in routes:
        runs = repo / "data" / "fabfos" / "runs"
        grows = []
        for s in STRAINS:
            pgdb_genes = {re.sub(r"[^A-Z0-9]", "", g.upper())
                          for g in tsv(pg / "metasmith_pathologic" / f"{s}.reaction_genes.tsv.gz").gene}
            cm = {re.sub(r"[^A-Z0-9]", "", g.upper()) for g in carveme_genes(mm / "ecoli_carveme" / s / "carveme_sbml.xml")}
            lane = {re.sub(r"[^A-Z0-9]", "", o.upper())
                    for o in pd.read_parquet(runs / f"e_coli_{s}" / "gpr" / "gpr_denovo.parquet", columns=["orf"]).orf}
            grows.append({"strain": s, "pgdb_enzyme_genes": len(pgdb_genes), "carveme_genes": len(cm),
                          "4lane_orfs": len(lane), "pgdb_in_carveme": len(pgdb_genes & cm) / len(pgdb_genes),
                          "carveme_in_pgdb": len(pgdb_genes & cm) / len(cm),
                          "pgdb_in_4lane": len(pgdb_genes & lane) / len(pgdb_genes)})
        g = pd.DataFrame(grows)
        g.to_csv(out / "ecoli_pgdb_genes.tsv", sep="\t", index=False)
        print(g.round(3).to_string(index=False))


def main(repo: Path):
    mm = repo / "data" / "mm_compare"
    out = mm / "probes"
    out.mkdir(exist_ok=True)
    for f in out.glob("*.tsv"):
        f.unlink()
    ecoli(repo, mm, out)
    cami(mm, out)
    cami_gpr(mm, out)
    gene_overlap(repo, mm, out)
    ecoli_pgdb(repo, mm, out)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
