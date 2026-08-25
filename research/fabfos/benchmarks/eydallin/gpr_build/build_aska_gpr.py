#!/usr/bin/env python3
"""The WHOLE ASKA library's edges, on both channels. The population Eydallin screened.

    mamba run -n msm python research/fabfos/benchmarks/eydallin/build_aska_gpr.py
    ... --publish        # writes data/fabfos/runs/aska/gpr/

`build_clone_gpr.py` and `build_clone_gpr_denovo.py` do this for the 86 genes Eydallin
REPORTED. This does it for the 4,123 clones Eydallin ASSAYED. That difference is the whole
reason the file exists: a clone absent from the paper's tables was built, transformed and
stained, and did not move glycogen, so it is a measured negative rather than an unlabelled
one -- and a negative set of four thousand is what turns a rank into an AUC.

NEITHER CHANNEL RUNS AN ANNOTATOR. Both are joins over tables already on disk:

  * curated -- the AG1 model's own GPR, keyed by gene symbol.
  * de-novo -- the AG1 proteome's four-lane de-novo GPR, keyed by ORF id.

That is possible here and was not possible for the eydallin cohort because an ASKA clone
is a chromosomal E. coli ORF: AG1 already carries every gene the library overexpresses, so
"what reactions does this clone add" is answerable from the host's own annotation. The
clone is a second copy, not a new gene.

RESOLUTION IS WHERE THE ANSWER'S HONESTY LIVES. The roster writes 2005 gene names and both
annotations write current ones, so a plain symbol join drops several hundred clones and
several of Eydallin's own hits. The chain is the one `build_clone_gpr.py` uses --
name -> b-number (MG1655 GenBank, synonyms included) -> MG1655's current symbol -> the
target annotation's gene of that name -- with DH1's own GenBank as a second synonym source
for the de-novo side. What still does not resolve is written out and counted; it is a bound
on the sweep, not a rounding error.

EVERY CLONE GETS A ROW IN THE CENSUS, including one that resolves to nothing. Dropping the
unresolvable makes the population "clones the model can see", and a hit rate over that
population answers a different question than the one the screen asked.

A GENE NAME RESOLVING TO MORE THAN ONE FEATURE IS COLLAPSED, not fanned out: one
`condition_id` per clone gene, its reaction set the union. Fanning out would let one clone
carry two rows through every statistic downstream and be counted twice.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[5]
HERE = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(REPO / "research" / "fabfos" / "benchmarks" / "aska"))
from build_extraction import gene_to_bnumber                          # noqa: E402

sys.path.insert(0, str(REPO / "src/fabfos/build_references/resources/buildlib"))
import bench_universe as bu                                           # noqa: E402

sys.path.insert(0, str(REPO / "src/metasmith_libraries/resources/lib"))
import fabfos_evidence as fe                                          # noqa: E402

ROSTER = REPO / "data/fabfos/originals/benchmarks/aska/library/aska_clone_minus.tsv"
MG1655_GBK = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.gbk"
MG1655_FAA = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.faa"
DH1_GBK = REPO / "data/fabfos/originals/genomes/e_coli_dh1/genome/NC_017638.1.gbk"
DH1_FAA = REPO / "data/fabfos/originals/genomes/e_coli_dh1/genome/NC_017638.1.faa"
HOST_GEM = REPO / "data/fabfos/runs/e_coli_ag1/gpr/gpr_gem.parquet"
HOST_DENOVO = REPO / "data/fabfos/runs/e_coli_ag1/gpr/gpr_denovo.parquet"
BAKE = REPO / "data/fabfos/processed/metabolism_bake"
METANETX = REPO / "data/fabfos/originals/metanetx"
EXTRACTION = REPO / "data/fabfos/benchmarks/eydallin/extraction.tsv"
OUT = REPO / "data/fabfos/runs/aska/gpr"

HOST = "e_coli_ag1"
COHORT = "aska"
SOURCE_ORGANISM = "e_coli_w3110"

EXTENSIONS = ("attribution", "feature", "universe", "cohort")
GEM_LANE_SET = "curated"
DENOVO_LANE_SET = "chosen_4"


def faa_index(faa: Path) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    gene, tag, by_tag = {}, {}, {}
    for line in faa.open():
        if not line.startswith(">"):
            continue
        fid = line[1:].split(None, 1)[0]
        g = re.search(r"\[gene=([^\]]+)\]", line)
        t = re.search(r"\[locus_tag=([^\]]+)\]", line)
        if g:
            gene[fid] = g.group(1)
        if t:
            tag[fid] = t.group(1)
            by_tag.setdefault(t.group(1), fid)
    return gene, tag, by_tag


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


def read_roster(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t", dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]
    df["gene"] = df["Gene Name"].str.strip()
    df["jw_id"] = df["JW ID"].str.strip()
    return df[df.gene != ""].reset_index(drop=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {OUT.relative_to(REPO)} rather than beside this script")
    a = ap.parse_args()
    out_dir = OUT if a.publish else (HERE / "out" / "aska_gpr")

    roster = read_roster(ROSTER)
    genes = sorted(roster.gene.unique())
    print(f"roster {ROSTER.name}: {len(roster):,} clones, {len(genes):,} distinct gene names")

    to_bnum = gene_to_bnumber(MG1655_GBK)
    sym_for_b = symbol_for_bnumber(MG1655_FAA)
    dh1_to_tag = gene_to_bnumber(DH1_GBK)
    dn_gene, _dn_tag, dn_by_tag = faa_index(DH1_FAA)
    print(f"MG1655: {len(to_bnum):,} names/synonyms -> b-number, "
          f"{len(sym_for_b):,} b-numbers -> current symbol")
    print(f"DH1:    {len(dh1_to_tag):,} names/synonyms -> locus_tag, "
          f"{len(dn_by_tag):,} locus_tags -> ORF id, {len(dn_gene):,} ORFs carry a symbol")

    gem = pd.read_parquet(HOST_GEM)
    gem_id = str(gem["unit_id"].iloc[0])
    gem_by_symbol: dict[str, set[str]] = {}
    for name, fid in zip(gem[gem.feature_kind == "gem_gene"]["feature_name"],
                         gem[gem.feature_kind == "gem_gene"]["orf"]):
        if name:
            gem_by_symbol.setdefault(str(name), set()).add(str(fid))

    dn_by_symbol: dict[str, set[str]] = {}
    for fid, g in dn_gene.items():
        dn_by_symbol.setdefault(g, set()).add(fid)

    def resolve(gene: str, current: str, by_symbol: dict, by_tag=None, to_tag=None):
        for key, how in ((current, "current_symbol"), (gene, "roster_symbol")):
            if key and key in by_symbol:
                return by_symbol[key], how
        if by_tag is not None and to_tag is not None:
            tag = to_tag.get(gene) or (to_tag.get(current) if current else None)
            if tag and tag in by_tag:
                return {by_tag[tag]}, "genome_synonym"
        return set(), "unresolved"

    universe, ustats = bu.atom_universe(
        BAKE / "vocab.parquet", BAKE / "atom_pairs.parquet",
        exclude=bu.transport_mnxrs(bu.reac_prop_path(METANETX)))
    print(bu.universe_line(ustats, "aska"))

    dn = pd.read_parquet(HOST_DENOVO)
    dn = dn.assign(in_atom_universe=dn["mnxr"].isin(universe))
    dn_lanes = sorted(dn["channel"].astype(str).unique())

    gem_parts, dn_parts, census = [], [], []
    gem_idx = {f: g for f, g in gem.groupby("orf")}
    dn_idx = {f: g for f, g in dn.groupby("orf")}

    per_gene = {}
    for gene in genes:
        b = to_bnum.get(gene, "")
        current = sym_for_b.get(b, "")
        cond = f"{COHORT}:{gene}"

        g_fids, g_how = resolve(gene, current, gem_by_symbol)
        d_fids, d_how = resolve(gene, current, dn_by_symbol, dn_by_tag, dh1_to_tag)

        g_hit = (pd.concat([gem_idx[f] for f in sorted(g_fids) if f in gem_idx])
                 if any(f in gem_idx for f in g_fids) else gem.iloc[0:0])
        d_hit = (pd.concat([dn_idx[f] for f in sorted(d_fids) if f in dn_idx])
                 if any(f in dn_idx for f in d_fids) else dn.iloc[0:0])

        if len(g_hit):
            gem_parts.append(g_hit.assign(
                source=gem_id, build_id=f"direct_{COHORT}_{gem_id}", host=HOST,
                unit_id=gem_id, feature_kind="clone_gene", condition_id=cond,
                cohort=COHORT, action="add", source_organism=SOURCE_ORGANISM))
        if len(d_hit):
            dn_parts.append(d_hit.assign(
                source="aska_orfs",
                build_id=f"denovo_{COHORT}_" + "+".join(dn_lanes), host=HOST,
                unit_id="aska_orfs", feature_kind="clone_gene",
                feature_name=gene, condition_id=cond, cohort=COHORT, action="add",
                source_organism=SOURCE_ORGANISM))

        per_gene[gene] = dict(
            gene=gene, condition_id=cond, b_number=b, current_symbol=current,
            gem_feature=";".join(sorted(g_fids)), gem_resolved_via=g_how,
            gem_n_mnxr=int(g_hit["mnxr"].nunique()),
            gem_n_in_universe=int(g_hit[g_hit["in_atom_universe"].fillna(False)]["mnxr"].nunique()),
            denovo_feature=";".join(sorted(d_fids)), denovo_resolved_via=d_how,
            denovo_n_mnxr=int(d_hit["mnxr"].nunique()),
            denovo_n_in_universe=int(d_hit[d_hit["in_atom_universe"]]["mnxr"].nunique()))

    for r in roster.itertuples(index=False):
        census.append(dict(jw_id=r.jw_id, **per_gene[r.gene]))
    cen = pd.DataFrame(census)

    cols = fe.schema_for(EXTENSIONS)

    def assemble(parts, like):
        df = (pd.concat(parts, ignore_index=True) if parts
              else like.reindex(columns=cols))
        return (df[cols].sort_values(fe.grain_key(EXTENSIONS), kind="mergesort")
                        .reset_index(drop=True))

    gem_df = assemble(gem_parts, gem)
    dn_df = assemble(dn_parts, dn)
    dn_df["raw_score"] = dn_df["raw_score"].astype(np.float32)
    fe.validate_gpr(gem_df, GEM_LANE_SET, None, gem_id, EXTENSIONS)
    fe.validate_gpr(dn_df, DENOVO_LANE_SET, None, "aska_orfs", EXTENSIONS)

    out_dir.mkdir(parents=True, exist_ok=True)
    gem_df.to_parquet(out_dir / "gpr_gem.parquet", index=False, compression="zstd")
    dn_df.to_parquet(out_dir / "gpr_denovo.parquet", index=False, compression="zstd")
    cen.to_csv(out_dir / "clone_census.tsv", sep="\t", index=False)

    g1 = cen.drop_duplicates("gene")
    manifest = dict(
        roster=str(ROSTER.relative_to(REPO)), roster_clones=int(len(roster)),
        roster_genes=int(len(genes)), host=HOST, cohort=COHORT, gem_unit_id=gem_id,
        denovo_lanes=dn_lanes, universe=ustats,
        gem=dict(rows=int(len(gem_df)),
                 genes_resolved=int((g1.gem_feature != "").sum()),
                 genes_with_mnxr=int((g1.gem_n_mnxr > 0).sum()),
                 genes_in_universe=int((g1.gem_n_in_universe > 0).sum()),
                 mnxr=int(gem_df.mnxr.nunique())),
        denovo=dict(rows=int(len(dn_df)),
                    genes_resolved=int((g1.denovo_feature != "").sum()),
                    genes_with_mnxr=int((g1.denovo_n_mnxr > 0).sum()),
                    genes_in_universe=int((g1.denovo_n_in_universe > 0).sum()),
                    mnxr=int(dn_df.mnxr.nunique())),
        resolved_via={k: dict(v) for k, v in
                      dict(gem=g1.gem_resolved_via.value_counts().to_dict(),
                           denovo=g1.denovo_resolved_via.value_counts().to_dict()).items()},
    )

    ey = pd.read_csv(EXTRACTION, sep="\t", dtype=str).fillna("")
    roster_by_b, roster_lower = {}, {}
    for g, b in zip(g1.gene, g1.b_number.fillna("")):
        if b:
            roster_by_b.setdefault(b, g)
        roster_lower.setdefault(g.lower(), g)
    label, unlabelled = {}, []
    for r in ey.itertuples(index=False):
        name = r.gene.strip()
        norm = (r.gene_norm or "").strip() or name
        hit = next((h for h in (name if name in roster_lower.values() else None,
                                roster_by_b.get(to_bnum.get(norm, "")),
                                roster_by_b.get(to_bnum.get(name, "")),
                                roster_lower.get(name.lower())) if h), None)
        if hit is None:
            unlabelled.append(name)
        else:
            label.setdefault(hit, (name, r.phenotype))
    cen["eydallin_gene"] = cen.gene.map(lambda g: label.get(g, ("", ""))[0])
    cen["eydallin_phenotype"] = cen.gene.map(lambda g: label.get(g, ("", ""))[1])
    cen.to_csv(out_dir / "clone_census.tsv", sep="\t", index=False)
    g1 = cen.drop_duplicates("gene")

    pos = g1[g1.eydallin_gene != ""]
    manifest["eydallin"] = dict(
        n=int(len(ey)), labelled=int(len(pos)), unlabelled=sorted(unlabelled),
        collapsed=int(len(ey) - len(label) - len(unlabelled)),
        gem_in_universe=int((pos.gem_n_in_universe > 0).sum()),
        denovo_in_universe=int((pos.denovo_n_in_universe > 0).sum()))
    (out_dir / "BUILD_aska_gpr.json").write_text(json.dumps(manifest, indent=2))

    print(f"\ncurated : {len(gem_df):,} rows | {manifest['gem']['genes_resolved']:,} genes "
          f"named in {gem_id}, {manifest['gem']['genes_with_mnxr']:,} with a reaction, "
          f"{manifest['gem']['genes_in_universe']:,} atom-mapped")
    print(f"de-novo : {len(dn_df):,} rows | {manifest['denovo']['genes_resolved']:,} genes "
          f"matched an AG1 ORF, {manifest['denovo']['genes_with_mnxr']:,} with a reaction, "
          f"{manifest['denovo']['genes_in_universe']:,} atom-mapped")
    print(f"resolved via: {manifest['resolved_via']}")
    e = manifest["eydallin"]
    print(f"eydallin: {e['labelled']}/{e['n']} positives labelled on the roster; "
          f"{e['gem_in_universe']} atom-mapped curated, {e['denovo_in_universe']} de-novo")
    if e["unlabelled"]:
        print(f"    NOT on the roster under any name/b-number: {e['unlabelled']}")
    if e["collapsed"]:
        print(f"    {e['collapsed']} positive(s) collapsed onto an already-labelled clone")
    print(f"\n-> {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
