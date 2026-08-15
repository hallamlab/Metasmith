#!/usr/bin/env python3
"""The eydallin clones' edges, read off the curated model. The DIRECT route.

    mamba run -n figure-net python main/benchmarks/eydallin/build_clone_gpr.py
    mamba run -n figure-net python main/benchmarks/eydallin/build_clone_gpr.py --publish

Two lines of evidence answer "what reactions does this clone add", and this is the one
that asks a curated genome-scale model. The other reads the sequence -- the four
annotation lanes over `eydallin_clones.faa` -- and the comparison between them is the
point, which is why they are separate files on one schema rather than one merged table.

IT READS THE HOST'S OWN TABLE, NOT THE MODEL. `data/fabfos/benchmarks/hosts/e_coli_ag1/
gpr_gem.parquet` already carries every (model gene -> reaction -> MNXR) row for AG1,
crosswalked once and labelled with `in_atom_universe` against one bake. Re-deriving that
here from the JSON would put a second resolver in the tree, and two resolvers over one
model is how a clone's edge and the background's edge for the same reaction come to
disagree. Subsetting is also what makes the AG1 edit list bind: `GTPDPK` is absent from
the background because relA1 broke it, so a relA clone cannot silently add it back
through a path the host table never had.

A CLONE'S NAME IS RESOLVED THROUGH THE B-NUMBER, exactly as the ORF set is. The model
names its genes by current symbol and the paper writes 2010 symbols, so `erfK` has to
become `ldtA` before it will match, and `pfs` `mtnN`. The chain is
name -> b-number (MG1655 GenBank, synonyms included) -> MG1655's current symbol ->
the model's gene of that name.

COVERAGE IS THE RESULT, NOT A PROBLEM. A curated model is a claim about central
metabolism, and this cohort is a genome-wide screen: most of its 86 genes are
regulators, transporters and y-genes the model never claimed. The count that comes out
of here is what the de-novo half is measured against, so it is printed and written and
never filled in.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent

sys.path.insert(0, str(REPO / "main" / "benchmarks" / "aska"))
from build_extraction import gene_to_bnumber                          # noqa: E402

EXTRACTION = REPO / "data/fabfos/benchmarks/eydallin/extraction.tsv"
MG1655_GBK = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.gbk"
MG1655_FAA = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.faa"
HOST_GPR = REPO / "data/fabfos/benchmarks/hosts/e_coli_ag1/gpr_gem.parquet"
OUT = REPO / "data/fabfos/runs/eydallin_clones/gpr"

HOST = "e_coli_ag1"
COHORT = "eydallin"
# An ASKA clone is a W3110 ORF on a high-copy plasmid in an AG1 host, so the edges are
# the host lineage's own -- but the sequence that was cloned is W3110's, and the column
# exists to say which organism an edge's gene came from.
SOURCE_ORGANISM = "e_coli_w3110"

GPR_COLS = (
    "build_id", "host", "unit_id", "feature_id", "feature_kind", "feature_name",
    "mnxr", "channel", "evidence_id", "evidence_name", "raw_score",
    "projection_via", "in_atom_universe", "gpr_rule",
    "condition_id", "cohort", "action", "source_organism",
)


def mg1655_symbol_for_bnumber(faa: Path) -> dict[str, str]:
    """b-number -> the symbol MG1655's CURRENT annotation uses.

    The hinge of the whole resolution: the paper's name and the model's name are both
    symbols, from fifteen years apart, and the b-number is the only thing that survived
    unchanged between them.
    """
    import re
    out = {}
    for line in faa.open():
        if not line.startswith(">"):
            continue
        loc = re.search(r"\[locus_tag=([^\]]+)\]", line)
        gene = re.search(r"\[gene=([^\]]+)\]", line)
        if loc and gene:
            out.setdefault(loc.group(1), gene.group(1))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {OUT.relative_to(REPO)} rather than beside this "
                         f"script")
    a = ap.parse_args()
    out_dir = OUT if a.publish else (HERE / "out")

    rows = list(csv.DictReader(EXTRACTION.open(), delimiter="\t"))
    to_bnum = gene_to_bnumber(MG1655_GBK)
    sym_for_b = mg1655_symbol_for_bnumber(MG1655_FAA)
    host = pd.read_parquet(HOST_GPR)
    gem_id = host["unit_id"].iloc[0]
    genes = host[host["feature_kind"] == "gem_gene"]
    # symbol -> the model's gene id. The model's own `feature_name` is its symbol; a
    # symbol shared by two model genes would make this ambiguous, so it is counted.
    by_symbol: dict[str, set[str]] = {}
    for name, fid in zip(genes["feature_name"], genes["feature_id"]):
        if name:
            by_symbol.setdefault(str(name), set()).add(str(fid))
    ambiguous = {k: v for k, v in by_symbol.items() if len(v) > 1}
    print(f"{HOST}: {len(host):,} rows, {genes['feature_id'].nunique():,} model genes, "
          f"{len(by_symbol):,} symbols ({len(ambiguous)} ambiguous)")

    out_rows, census = [], []
    for r in rows:
        gene = (r["gene"] or "").strip()
        norm = (r["gene_norm"] or "").strip() or gene
        cond = f"{COHORT}:{gene}"
        b = to_bnum.get(norm) or to_bnum.get(gene) or ""
        current = sym_for_b.get(b, "")
        fids = set()
        for key in (current, norm, gene):
            if key and key in by_symbol:
                fids = by_symbol[key]
                break
        hit = host[host["feature_id"].isin(fids)] if fids else host.iloc[0:0]
        census.append(dict(gene=gene, b_number=b, current_symbol=current,
                           model_gene=";".join(sorted(fids)),
                           n_reactions=int(hit["evidence_id"].nunique()),
                           n_mnxr=int(hit["mnxr"].nunique()),
                           n_in_universe=int(hit[hit["in_atom_universe"]]["mnxr"]
                                             .nunique())))
        for _, h in hit.iterrows():
            out_rows.append(dict(
                build_id=f"direct_{COHORT}_{gem_id}",
                host=HOST, unit_id=gem_id,
                feature_id=h["feature_id"], feature_kind="clone_gene",
                feature_name=h["feature_name"], mnxr=h["mnxr"],
                channel=h["channel"], evidence_id=h["evidence_id"],
                evidence_name=h["evidence_name"], raw_score=h["raw_score"],
                projection_via=h["projection_via"],
                in_atom_universe=h["in_atom_universe"], gpr_rule=h["gpr_rule"],
                # The paper measured overexpression, so a clone ADDS its reactions. The
                # study tier currently labels this cohort `del`, which is the opposite
                # perturbation; see the cohort README.
                condition_id=cond, cohort=COHORT, action="add",
                source_organism=SOURCE_ORGANISM))

    df = pd.DataFrame(out_rows, columns=list(GPR_COLS))
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / "gpr_gem.parquet", index=False, compression="zstd")
    cen = pd.DataFrame(census)
    cen.to_csv(out_dir / "clone_gem_census.tsv", sep="\t", index=False)

    with_rxn = cen[cen["n_reactions"] > 0]
    in_uni = cen[cen["n_in_universe"] > 0]
    print(f"\n{len(df):,} rows over {len(with_rxn)}/{len(cen)} clones with at least one "
          f"reaction\n"
          f"    {len(in_uni)} of them have one inside the atom universe\n"
          f"    {int(cen['n_mnxr'].sum())} gene-reaction claims, "
          f"{df['mnxr'].nunique()} distinct MNXR")
    unmapped = cen[cen["model_gene"] == ""]
    print(f"    {len(unmapped)} clones name no gene in {gem_id} at all")
    named_no_rxn = cen[(cen["model_gene"] != "") & (cen["n_reactions"] == 0)]
    if len(named_no_rxn):
        print(f"    {len(named_no_rxn)} name a model gene that carries no reaction: "
              f"{sorted(named_no_rxn['gene'])}")
    print(f"\n-> {out_dir}/gpr_gem.parquet and clone_gem_census.tsv")
    return 0


if __name__ == "__main__":
    sys.exit(main())
