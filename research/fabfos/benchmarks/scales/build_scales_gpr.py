#!/usr/bin/env python3
"""Per-gene edge tables and the reach census for the SCALEs benchmark.

    mamba run -n build-refs-cobra python research/fabfos/benchmarks/scales/build_scales_gpr.py
    ... --publish        # writes data/fabfos/runs/scales/gpr/

THE POPULATION IS THE COMPANION ARM. `scales_tol` names all 4,225 genes it measured, so it
is the arm a classifier can be scored on. `scales_prod` measured 4,103 and can name three:
its gene symbols lived in a workbook that survives only as a dead OLE link. A census over
4,100 synthetic identifiers would resolve to nothing by construction and would say nothing
about reach, so this file builds no census for that arm -- its three named genes ride in
`gof_scales.tsv` as a cited contrast instead. The production arm's contribution to T4 is
the engineered insertion, below.

EVERY MEASURED GENE GETS A CENSUS ROW, INCLUDING ONE THAT RESOLVES TO NOTHING. That is the
point of the file. "How much of the screen can this method see at all" has to be a
published number before "how well does it rank what it sees" means anything, and dropping
the unresolvable silently redefines the population as the genes the model happens to carry.

RESOLUTION, AND THE TWO TRAPS THE 2012 SHEET CARRIES
----------------------------------------------------
The host GEM keys `orf` on the b-number, so a b-number join is direct -- but the
sheet's own b-number column is NOT a key: 33 b-numbers appear on two rows each, carrying
different fitness values (b3256 is written for both accC and fabG). `gene_norm` is unique
across all 4,225 rows and is the condition key here; the sheet's b-number is only ever a
fallback, and the count of rows where the name-derived and sheet-written b-numbers disagree
is reported rather than reconciled.

The second trap is age: the 2012 sheet still writes `yfbE`/`yfbF` for arnB/arnC. That is
why the chain runs through MG1655's GenBank synonym table (name OR synonym -> b-number)
before it tries any symbol match, and why a plain symbol join would silently drop genes
that a confirmed clone was selected on.

A GENE RESOLVING TO MORE THAN ONE FEATURE IS COLLAPSED, not fanned out: one condition per
measured gene, its reaction set the union. Fanning out would let one gene carry two rows
through every downstream statistic and be counted twice.

THE ENGINEERED INSERTION IS A SEPARATE TABLE. `host_gpr_gem.py` only subtracts, so LW06's
attTn7::PLlacO-1 pdcZm adhBZm cannot be expressed as a host edit. It rides as study GPR
rows that concatenate with the host's at solve time. betI is deliberately absent from that
table and present in its census: a transcriptional repressor has no reaction, and inventing
one is exactly the fabrication the unresolved count exists to measure.

THE DE-NOVO CHANNEL IS OPTIONAL HERE. It needs the BW25113 proteome annotated on a cluster
that carries the lane databases; until that table lands this script builds the curated
channel and writes a census whose de-novo columns are empty, and says so in the manifest.
Re-run after the annotation to fill them.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def _repo_root(start: Path) -> Path:
    for d in (start, *start.parents):
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


REPO = _repo_root(Path(__file__).resolve())
HERE = Path(__file__).resolve().parent

sys.path.insert(0, str(REPO / "research" / "fabfos" / "benchmarks" / "aska"))
from build_extraction import gene_to_bnumber                          # noqa: E402

sys.path.insert(0, str(REPO / "src/fabfos/build_references/resources/buildlib"))
import bench_universe as bu                                           # noqa: E402

sys.path.insert(0, str(REPO / "src/metasmith_libraries/resources/lib"))
import fabfos_evidence as fe                                          # noqa: E402

EXTRACTION = REPO / "data/fabfos/benchmarks/_extractions/scales_tol/extraction.tsv"
CLONES = REPO / "data/fabfos/benchmarks/_extractions/scales_tol/clones.tsv"
GOF = REPO / "data/fabfos/originals/benchmarks/scales/gof_scales.tsv"
MG1655_GBK = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.gbk"
MG1655_FAA = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.faa"
BW_GBK = REPO / "data/fabfos/originals/genomes/e_coli_bw25113/genome/CP193896.1.gbk"
BW_FAA = REPO / "data/fabfos/originals/genomes/e_coli_bw25113/genome/CP193896.1.faa"
HOST_GEM = REPO / "data/fabfos/runs/e_coli_bw25113/gpr/gpr_gem.parquet"
HOST_DENOVO = REPO / "data/fabfos/runs/e_coli_bw25113/gpr/gpr_denovo.parquet"
BAKE = REPO / "data/fabfos/processed/metabolism_bake"
METANETX = REPO / "data/fabfos/originals/metanetx"
OUT = REPO / "data/fabfos/runs/scales/gpr"

HOST = "e_coli_bw25113"
COHORT = "scales_tol"
SOURCE_ORGANISM = "e_coli_bw25113"

PROD_HOST = "e_coli_lw06"
PROD_COHORT = "scales_prod"
INSERT_SOURCE_ORGANISM = "z_mobilis"

EXTENSIONS = ("attribution", "feature", "universe", "cohort")
COLS = fe.schema_for(EXTENSIONS)
CURATED_LANE_SET = "curated"
DENOVO_LANE_SET = "chosen_4"


def assemble(parts, like) -> pd.DataFrame:
    df = (pd.concat(parts, ignore_index=True) if parts else like.reindex(columns=COLS))
    df = df[COLS]
    if len(df):
        df["raw_score"] = df["raw_score"].astype(np.float32)
    return (df.sort_values(fe.grain_key(EXTENSIONS), kind="mergesort")
              .reset_index(drop=True))


def faa_index(faa: Path) -> tuple[dict[str, str], dict[str, str]]:
    gene, by_tag = {}, {}
    for line in faa.open():
        if not line.startswith(">"):
            continue
        fid = line[1:].split(None, 1)[0]
        g = re.search(r"\[gene=([^\]]+)\]", line)
        t = re.search(r"\[locus_tag=([^\]]+)\]", line)
        if g:
            gene[fid] = g.group(1)
        if t:
            by_tag.setdefault(t.group(1), fid)
    return gene, by_tag


def symbol_for_bnumber(faa: Path) -> dict[str, str]:
    out = {}
    for line in faa.open():
        if not line.startswith(">"):
            continue
        loc = re.search(r"\[locus_tag=([^\]]+)\]", line)
        gene = re.search(r"\[gene=([^\]]+)\]", line)
        if loc and gene:
            out.setdefault(loc.group(1), gene.group(1))
    return out


def build_curated(ext: pd.DataFrame, gem: pd.DataFrame, universe: set) -> tuple:
    gem_id = str(gem["unit_id"].iloc[0])
    genes = gem[gem.feature_kind == "gem_gene"]
    by_fid = {f: g for f, g in genes.groupby("orf")}
    by_symbol: dict[str, set[str]] = {}
    for name, fid in zip(genes["feature_name"], genes["orf"]):
        if name:
            by_symbol.setdefault(str(name), set()).add(str(fid))
    by_symbol_lower = {k.lower(): v for k, v in by_symbol.items()}

    to_bnum = gene_to_bnumber(MG1655_GBK)
    to_bnum_lower = {k.lower(): v for k, v in to_bnum.items()}
    sym_for_b = symbol_for_bnumber(MG1655_FAA)

    parts, record, disagree = [], [], []
    for r in ext.itertuples(index=False):
        gene, norm, sheet_b = str(r.gene), str(r.gene_norm), str(r.bnum or "")
        name_b = to_bnum.get(gene) or to_bnum_lower.get(norm) or ""
        if name_b and sheet_b and name_b != sheet_b:
            disagree.append(dict(gene=gene, sheet=sheet_b, from_name=name_b))
        current = sym_for_b.get(name_b) or sym_for_b.get(sheet_b) or ""

        fids, how = set(), "unresolved"
        if name_b and name_b in by_fid:
            fids, how = {name_b}, "name_bnumber"
        elif not name_b and sheet_b and sheet_b in by_fid:
            fids, how = {sheet_b}, "sheet_bnumber"
        if not fids:
            for key, route in ((current, "current_symbol"), (gene, "sheet_symbol")):
                if key and key in by_symbol:
                    fids, how = by_symbol[key], route
                    break
        if not fids and norm in by_symbol_lower:
            fids, how = by_symbol_lower[norm], "symbol_casefold"

        hit = (pd.concat([by_fid[f] for f in sorted(fids) if f in by_fid])
               if any(f in by_fid for f in fids) else genes.iloc[0:0])
        cond = f"{COHORT}:{norm}"
        if len(hit):
            parts.append(hit.assign(
                source=gem_id, build_id=f"direct_{COHORT}_{gem_id}", host=HOST,
                unit_id=gem_id, feature_kind="clone_gene", condition_id=cond,
                cohort=COHORT, action="add", source_organism=SOURCE_ORGANISM))
        record.append(dict(
            gene=gene, gene_norm=norm, condition_id=cond,
            bnum_sheet=sheet_b, bnum_from_name=name_b, current_symbol=current,
            fitness_15=r.fitness_15, fitness_30=r.fitness_30, phenotype=r.phenotype,
            gem_feature=";".join(sorted(fids)), gem_resolved_via=how,
            gem_n_mnxr=int(hit["mnxr"].nunique()),
            gem_n_in_universe=int(hit[hit["in_atom_universe"].fillna(False)]["mnxr"].nunique()),
        ))
    df = assemble(parts, genes)
    fe.validate_gpr(df, CURATED_LANE_SET, None, gem_id, EXTENSIONS)
    return df, pd.DataFrame(record), disagree, gem_id


def build_denovo(ext: pd.DataFrame, rec: pd.DataFrame, universe: set) -> tuple:
    dn = pd.read_parquet(HOST_DENOVO)
    dn = dn.assign(in_atom_universe=dn["mnxr"].isin(universe))
    lanes = sorted(dn["channel"].astype(str).unique())
    dn_gene, dn_by_tag = faa_index(BW_FAA)
    bw_to_tag = gene_to_bnumber(BW_GBK)
    bw_to_tag_lower = {k.lower(): v for k, v in bw_to_tag.items()}
    by_symbol: dict[str, set[str]] = {}
    for fid, g in dn_gene.items():
        by_symbol.setdefault(g, set()).add(fid)
    by_symbol_lower = {k.lower(): v for k, v in by_symbol.items()}
    by_fid = {f: g for f, g in dn.groupby("orf")}

    parts, cols = [], []
    cur = dict(zip(rec.gene_norm, rec.current_symbol))
    for r in ext.itertuples(index=False):
        gene, norm = str(r.gene), str(r.gene_norm)
        current = cur.get(norm, "")
        fids, how = set(), "unresolved"
        for key, route in ((current, "current_symbol"), (gene, "sheet_symbol")):
            if key and key in by_symbol:
                fids, how = by_symbol[key], route
                break
        if not fids and norm in by_symbol_lower:
            fids, how = by_symbol_lower[norm], "symbol_casefold"
        if not fids:
            tag = bw_to_tag.get(gene) or bw_to_tag.get(current) or bw_to_tag_lower.get(norm)
            if tag and tag in dn_by_tag:
                fids, how = {dn_by_tag[tag]}, "genome_synonym"

        hit = (pd.concat([by_fid[f] for f in sorted(fids) if f in by_fid])
               if any(f in by_fid for f in fids) else dn.iloc[0:0])
        cond = f"{COHORT}:{norm}"
        if len(hit):
            parts.append(hit.assign(
                source="bw25113_orfs",
                build_id="denovo_" + COHORT + "_" + "+".join(lanes), host=HOST,
                unit_id="bw25113_orfs", feature_kind="clone_gene",
                feature_name=gene, condition_id=cond, cohort=COHORT, action="add",
                source_organism=SOURCE_ORGANISM))
        cols.append(dict(
            gene_norm=norm,
            denovo_feature=";".join(sorted(fids)), denovo_resolved_via=how,
            denovo_n_mnxr=int(hit["mnxr"].nunique()),
            denovo_n_in_universe=int(hit[hit["in_atom_universe"]]["mnxr"].nunique())))
    df = assemble(parts, dn)
    fe.validate_gpr(df, DENOVO_LANE_SET, None, "bw25113_orfs", EXTENSIONS)
    return df, pd.DataFrame(cols), lanes


def build_insertion(universe: set) -> tuple:
    gof = pd.read_csv(GOF, sep="\t", dtype=str).fillna("")
    add = gof[gof.role == "add"]
    rows, unresolved = [], []
    for r in add.itertuples(index=False):
        gene, mnxr = str(r.gene), str(r.mnxr).strip()
        if not mnxr:
            unresolved.append(dict(gene=gene, why="no reaction of its own",
                                   note=str(r.note)[:120]))
            continue
        rows.append(dict(
            source="attTn7_pdc_adhB", orf=gene, channel="curated_insertion", mnxr=mnxr,
            intermediate_id=gene, intermediate_name=str(r.ec),
            raw_score=np.float32(1.0), score_kind="presence", projection_via="manual",
            evidence_quality="unknown", lane_set="curated",
            build_id="insertion_scales_prod", host=PROD_HOST, unit_id="attTn7_pdc_adhB",
            feature_kind="insertion_gene", feature_name=gene, gpr_rule=gene,
            in_atom_universe=mnxr in universe,
            condition_id=f"{PROD_COHORT}:{gene}", cohort=PROD_COHORT, action="add",
            source_organism=INSERT_SOURCE_ORGANISM))
    df = pd.DataFrame(rows, columns=COLS)
    if len(df):
        df["raw_score"] = df["raw_score"].astype(np.float32)
        fe.validate_gpr(df, "curated", None, "attTn7_pdc_adhB", EXTENSIONS)
    return df, unresolved


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {OUT.relative_to(REPO)} rather than beside this script")
    a = ap.parse_args()
    out_dir = OUT if a.publish else (HERE / "out" / "scales_gpr")

    ext = pd.read_csv(EXTRACTION, sep="\t", dtype=str).fillna("")
    if ext.gene_norm.duplicated().any():
        raise SystemExit("gene_norm is not unique in the extraction; it is the condition key")
    print(f"population: {len(ext):,} measured genes ({EXTRACTION.name})")

    universe, ustats = bu.atom_universe(
        BAKE / "vocab.parquet", BAKE / "atom_pairs.parquet",
        exclude=bu.transport_mnxrs(bu.reac_prop_path(METANETX)))
    print(bu.universe_line(ustats, "scales"))

    gem = pd.read_parquet(HOST_GEM)
    gem_df, rec, disagree, gem_id = build_curated(ext, gem, universe)
    print(f"curated : {len(gem_df):,} rows over {gem_id}")

    if HOST_DENOVO.exists():
        dn_df, dn_cols, lanes = build_denovo(ext, rec, universe)
        rec = rec.merge(dn_cols, on="gene_norm", how="left")
    else:
        dn_df, lanes = pd.DataFrame(columns=COLS), []
        for c, v in (("denovo_feature", ""), ("denovo_resolved_via", "not_built"),
                     ("denovo_n_mnxr", 0), ("denovo_n_in_universe", 0)):
            rec[c] = v
        print(f"de-novo : NOT BUILT -- {HOST_DENOVO.relative_to(REPO)} does not exist")

    ins_df, ins_unresolved = build_insertion(universe)

    out_dir.mkdir(parents=True, exist_ok=True)
    gem_df.to_parquet(out_dir / "gpr_gem.parquet", index=False, compression="zstd")
    if len(dn_df):
        dn_df.to_parquet(out_dir / "gpr_denovo.parquet", index=False, compression="zstd")
    ins_df.to_parquet(out_dir / "gpr_insertion.parquet", index=False, compression="zstd")
    rec.to_csv(out_dir / "gene_census.tsv", sep="\t", index=False)

    clones = pd.read_csv(CLONES, sep="\t", dtype=str).fillna("")
    conf_genes, conf_bnums = set(), set()
    for r in clones.itertuples(index=False):
        if str(r.confirmed).strip().upper() != "TRUE":
            continue
        conf_genes.update(g.lower() for g in re.split(r"[;,\s]+", r.genes) if g)
        conf_bnums.update(b for b in re.split(r"[;,\s]+", r.bnums) if b)
    seen = rec[rec.bnum_from_name.isin(conf_bnums) | rec.bnum_sheet.isin(conf_bnums)
               | rec.gene_norm.isin(conf_genes)]
    if len(seen) != len(conf_bnums):
        raise SystemExit(
            f"confirmed-clone join is wrong: {len(conf_bnums)} b-numbers matched "
            f"{len(seen)} census rows ({sorted(seen.gene)}). Every confirmed gene was "
            f"measured by the screen, so a short join is a resolver bug, not a real gap.")

    def leg(df: pd.DataFrame, prefix: str) -> dict:
        return dict(
            resolved=int((df[f"{prefix}_feature"] != "").sum()),
            with_mnxr=int((df[f"{prefix}_n_mnxr"] > 0).sum()),
            in_universe=int((df[f"{prefix}_n_in_universe"] > 0).sum()),
            via={k: int(v) for k, v in
                 df[f"{prefix}_resolved_via"].value_counts().to_dict().items()})

    manifest = dict(
        population=dict(source=str(EXTRACTION.relative_to(REPO)), genes=int(len(ext)),
                        cohort=COHORT, host=HOST),
        universe=ustats,
        weight_dict_convention=(
            "in_atom_universe filter, uniform weight 1.0 -- the same convention "
            "sink_panel.py used for T5, so the mechanistic and classifier arms share a "
            "background"),
        gem=dict(rows=int(len(gem_df)), unit_id=gem_id,
                 mnxr=int(gem_df.mnxr.nunique()), **leg(rec, "gem")),
        denovo=dict(rows=int(len(dn_df)), lanes=lanes,
                    mnxr=int(dn_df.mnxr.nunique()) if len(dn_df) else 0,
                    **leg(rec, "denovo")),
        insertion=dict(rows=int(len(ins_df)), host=PROD_HOST,
                       genes=sorted(ins_df.feature_name.unique().tolist()),
                       in_universe=int(ins_df.in_atom_universe.sum()) if len(ins_df) else 0,
                       unresolved=ins_unresolved),
        bnumber_disagreements=dict(n=len(disagree), rows=disagree),
        positives=dict(
            tolerant_15=int((rec.phenotype.isin(["tolerant_both", "tolerant_15"])).sum()),
            tolerant_30=int((rec.phenotype.isin(["tolerant_both", "tolerant_30"])).sum()),
            confirmed_clone_genes=sorted(conf_genes),
            confirmed_n=int(len(seen)),
            confirmed_census_names=sorted(seen.gene.tolist()),
            confirmed_seen_curated=int((seen.gem_n_in_universe > 0).sum()),
            confirmed_seen_denovo=int((seen.denovo_n_in_universe > 0).sum()),
            confirmed_curated_detail={
                str(g): int(n) for g, n in zip(seen.gene, seen.gem_n_in_universe)}),
    )
    (out_dir / "BUILD_scales_gpr.json").write_text(json.dumps(manifest, indent=2))

    g, d = manifest["gem"], manifest["denovo"]
    print(f"\ncurated : {g['resolved']:,}/{len(ext):,} genes named in {gem_id}, "
          f"{g['with_mnxr']:,} with a reaction, {g['in_universe']:,} atom-mapped")
    print(f"          via {g['via']}")
    print(f"de-novo : {d['resolved']:,}/{len(ext):,} genes matched an ORF, "
          f"{d['with_mnxr']:,} with a reaction, {d['in_universe']:,} atom-mapped")
    print(f"insertion: {len(ins_df)} rows {manifest['insertion']['genes']}, "
          f"{manifest['insertion']['in_universe']} in the atom universe; "
          f"unresolved {[u['gene'] for u in ins_unresolved]}")
    print(f"b-number disagreements (name-derived vs sheet): {len(disagree)}")
    print(f"positives: 15 g/L {manifest['positives']['tolerant_15']}, "
          f"30 g/L {manifest['positives']['tolerant_30']}, "
          f"confirmed-clone genes {len(conf_genes)} of which "
          f"{manifest['positives']['confirmed_seen_curated']} atom-mapped curated")
    print(f"\n-> {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
