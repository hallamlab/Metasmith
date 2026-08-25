#!/usr/bin/env python3
"""The eydallin clones' edges as the annotation lanes infer them. The DE-NOVO route.

    mamba run -n msm-fabfos python main/benchmarks/eydallin/build_clone_gpr_denovo.py \\
        --mapper data/scratch/clone_gpr_sockeye/results/annotation-gpr_table/*.parquet
    ... --publish

`examples/clone_gpr_on_hpc.py` runs the shipped 4-lane mapper over
`eydallin_clones.faa` on a cluster and lands one `annotation::gpr_table`. This is the
thin step after it -- the same one `benchmark/host_gpr_denovo.py` is for a host: attach
the cohort's attribution and re-emit on the shared schema, so the de-novo table and the
GEM table concatenate column for column and the comparison between them is a subtraction
rather than a reshaping exercise.

TWO THINGS ARE ADDED TO THE MAPPER'S OUTPUT AND NEITHER IS COSMETIC.

`in_atom_universe` is COMPUTED here, where the host de-novo step leaves it null. That
step runs on a compute node with no bake staged and null means "not asserted"; here the
bake is on disk, and the column is the whole point of the comparison -- the GEM route
gives 34 clones edges and the lanes give 85, but a row for a reaction with no atom-pair
coverage carries no edge either way, so an in-universe count is the only one that says
what either route BUYS. Same universe as B1's, transport excluded, through the same
`bench_universe` module: two definitions of "in universe" over one cohort would make the
two tables incomparable in exactly the way this file exists to avoid.

The condition columns come from the ORF ID, and that is why the FASTA's headers are the
paper's gene names: `orf` IS `gene`, so a lane's claim about a sequence lands on the
condition that measured it with no join in between and nothing to get wrong.
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[5]
HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src" / "fabfos" / "build_references" / "resources" / "buildlib"))
import bench_universe as bu                                            # noqa: E402
sys.path.insert(0, str(REPO / "src" / "metasmith_libraries" / "resources" / "lib"))
import fabfos_evidence as fe                                           # noqa: E402

MAPPER = REPO / "data/scratch/clone_gpr_sockeye/results/annotation-gpr_table"
BAKE = REPO / "data/fabfos/processed/metabolism_bake"
METANETX = REPO / "data/fabfos/originals/metanetx"
EXTRACTION = REPO / "data/fabfos/benchmarks/eydallin/extraction.tsv"
OUT = REPO / "data/fabfos/runs/eydallin_clones/gpr"

HOST = "e_coli_ag1"
COHORT = "eydallin"
SOURCE_ORGANISM = "e_coli_w3110"
LANE_SET = "chosen_4"
EXTENSIONS = ("attribution", "feature", "universe", "cohort")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mapper", default=None,
                    help="the annotation::gpr_table parquet; defaults to the one the "
                         "sockeye run retrieved")
    ap.add_argument("--publish", action="store_true")
    a = ap.parse_args()

    hits = ([Path(a.mapper)] if a.mapper else
            [Path(p) for p in sorted(glob.glob(str(MAPPER / "*.parquet")))])
    if len(hits) != 1:
        raise SystemExit(f"expected one mapper table, found {[str(p) for p in hits]}")
    g = pd.read_parquet(hits[0])
    lanes = sorted(g["channel"].unique())
    lane_set = sorted(g["lane_set"].unique())
    print(f"{hits[0].name}: {len(g):,} rows, {g['orf'].nunique()} ORFs, "
          f"{g['mnxr'].nunique():,} MNXR, lanes {lanes} ({lane_set})")

    reac_prop = bu.reac_prop_path(METANETX)
    universe, stats = bu.atom_universe(BAKE / "vocab.parquet",
                                       BAKE / "atom_pairs.parquet",
                                       exclude=bu.transport_mnxrs(reac_prop))
    print(bu.universe_line(stats, "clones"))

    genes = {r.strip().split("\t")[0] for r in EXTRACTION.read_text().splitlines()[1:]
             if r.strip()}
    unknown = sorted(set(g["orf"]) - genes)
    if unknown:
        raise SystemExit(f"{len(unknown)} ORF id(s) in the mapper table are not genes "
                         f"of this cohort: {unknown[:8]} -- the table was built from a "
                         f"different ORF set")

    df = g.copy()
    df["build_id"] = f"denovo_{COHORT}_" + "+".join(lanes)
    df["host"] = HOST
    df["unit_id"] = "clones"
    df["feature_kind"] = "clone_gene"
    df["feature_name"] = df["intermediate_name"]
    df["gpr_rule"] = None
    df["in_atom_universe"] = df["mnxr"].isin(universe)
    df["condition_id"] = COHORT + ":" + df["orf"].astype(str)
    df["cohort"] = COHORT
    df["action"] = "add"
    df["source_organism"] = SOURCE_ORGANISM
    df = df[fe.schema_for(EXTENSIONS)]
    df = df.sort_values(fe.grain_key(EXTENSIONS),
                        kind="mergesort").reset_index(drop=True)
    fe.validate_gpr(df, LANE_SET, None, df["source"].iat[0], EXTENSIONS)

    out_dir = OUT if a.publish else (HERE / "out")
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / "gpr_denovo.parquet", index=False, compression="zstd")
    (out_dir / "BUILD_denovo.json").write_text(json.dumps(dict(
        lane_set=lane_set, lanes=lanes, universe=stats, host=HOST, cohort=COHORT,
        rows=len(df), clones=int(df["orf"].nunique()),
        mnxr=int(df["mnxr"].nunique()),
        clones_in_universe=int(df[df["in_atom_universe"]]["orf"].nunique()),
        mnxr_in_universe=int(df[df["in_atom_universe"]]["mnxr"].nunique()),
    ), indent=2))

    in_uni = df[df["in_atom_universe"]]
    print(f"\n{len(df):,} rows over {df['orf'].nunique()} clones, "
          f"{df['mnxr'].nunique():,} distinct MNXR")
    print(f"    inside the atom universe: {in_uni['orf'].nunique()} clones, "
          f"{in_uni['mnxr'].nunique():,} MNXR")
    per_lane = (df.groupby("channel")
                  .agg(rows=("mnxr", "size"), clones=("orf", "nunique"),
                       mnxr=("mnxr", "nunique")))
    print(per_lane.to_string())
    print(f"\n-> {out_dir}/gpr_denovo.parquet")
    return 0


if __name__ == "__main__":
    sys.exit(main())
