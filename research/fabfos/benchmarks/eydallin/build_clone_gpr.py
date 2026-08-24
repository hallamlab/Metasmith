#!/usr/bin/env python3
"""The eydallin clones' edges, read off the curated model. The DIRECT route.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/eydallin/build_clone_gpr.py
    ... --publish        # writes data/fabfos/runs/eydallin_clones/gpr/

Two lines of evidence answer "what reactions does this clone add", and this is the one
that asks a curated genome-scale model. The other reads the sequence -- the four
annotation lanes over `eydallin_clones.faa` -- and the comparison between them is the
point, which is why they are separate files on one schema rather than one merged table.

IT READS THE HOST'S OWN TABLE, NOT THE MODEL. `data/fabfos/runs/e_coli_dh1/gpr/
gpr_gem.parquet` already carries every (model gene -> reaction -> MNXR) row for DH1,
crosswalked once and labelled with `in_atom_universe` against one bake. Re-deriving that
here from the JSON would put a second resolver in the tree, and two resolvers over one
model is how a clone's edge and the background's edge for the same reaction come to
disagree. Subsetting is also what makes an edit list bind: AG1's `GTPDPK` is absent from
ITS background because relA1 broke it, so a relA clone read against AG1 cannot silently
add it back through a path that host table never had -- DH1 carries no such edit, and is
the host this cohort's notebook actually runs against, so this reads DH1 directly rather
than through AG1's borrowed subset.

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

sys.path.insert(0, str(REPO / "research" / "fabfos" / "benchmarks" / "aska"))
from build_extraction import gene_to_bnumber                          # noqa: E402

sys.path.insert(0, str(REPO / "src/metasmith_libraries/resources/lib"))
import fabfos_evidence as fe                                          # noqa: E402

EXTRACTION = REPO / "data/fabfos/benchmarks/eydallin/extraction.tsv"
MG1655_GBK = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.gbk"
MG1655_FAA = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.faa"
HOST_GPR = REPO / "data/fabfos/runs/e_coli_dh1/gpr/gpr_gem.parquet"
OUT = REPO / "data/fabfos/runs/eydallin_clones/gpr"

HOST = "e_coli_dh1"
COHORT = "eydallin"
SOURCE_ORGANISM = "e_coli_w3110"

EXTENSIONS = ("attribution", "feature", "universe", "cohort")
LANE_SET = "curated"


def mg1655_symbol_for_bnumber(faa: Path) -> dict[str, str]:
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
    by_symbol: dict[str, set[str]] = {}
    for name, fid in zip(genes["feature_name"], genes["orf"]):
        if name:
            by_symbol.setdefault(str(name), set()).add(str(fid))
    ambiguous = {k: v for k, v in by_symbol.items() if len(v) > 1}
    print(f"{HOST}: {len(host):,} rows, {genes['orf'].nunique():,} model genes, "
          f"{len(by_symbol):,} symbols ({len(ambiguous)} ambiguous)")

    parts, census = [], []
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
        hit = host[host["orf"].isin(fids)] if fids else host.iloc[0:0]
        census.append(dict(gene=gene, b_number=b, current_symbol=current,
                           model_gene=";".join(sorted(fids)),
                           n_reactions=int(hit["intermediate_id"].nunique()),
                           n_mnxr=int(hit["mnxr"].nunique()),
                           n_in_universe=int(hit[hit["in_atom_universe"]]["mnxr"]
                                             .nunique())))
        if len(hit):
            parts.append(hit.assign(
                source=gem_id, build_id=f"direct_{COHORT}_{gem_id}", host=HOST,
                unit_id=gem_id, feature_kind="clone_gene",
                condition_id=cond, cohort=COHORT, action="add",
                source_organism=SOURCE_ORGANISM))

    cols = fe.schema_for(EXTENSIONS)
    df = (pd.concat(parts, ignore_index=True) if parts else host.reindex(columns=cols))
    df = (df[cols].sort_values(fe.grain_key(EXTENSIONS), kind="mergesort")
                  .reset_index(drop=True))
    fe.validate_gpr(df, LANE_SET, None, gem_id, EXTENSIONS)
    out_dir.mkdir(parents=True, exist_ok=True)
    # DVC checks data out as read-only hardlinks into its cache -- opening one for
    # write would mutate the shared cache object under every other checkout of the
    # same content. Unlink first so the write lands on a fresh inode.
    (out_dir / "gpr_gem.parquet").unlink(missing_ok=True)
    df.to_parquet(out_dir / "gpr_gem.parquet", index=False, compression="zstd")
    # No `clone_gem_census.tsv` any more -- `resolve_gene_manual.py`'s report and
    # `gpr_manual.parquet` are the per-gene record now, and a live table beside a
    # frozen TSV saying the same thing is how the two come to disagree.
    cen = pd.DataFrame(census)

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
    print(f"\n-> {out_dir}/gpr_gem.parquet")
    return 0


if __name__ == "__main__":
    sys.exit(main())
