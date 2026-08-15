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

REPO = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO / "build_references" / "resources" / "buildlib"))
import bench_universe as bu                                            # noqa: E402

MAPPER = REPO / "data/scratch/clone_gpr_sockeye/results/annotation-gpr_table"
BAKE = REPO / "data/fabfos/processed/metabolism_bake"
METANETX = REPO / "data/fabfos/originals/metanetx"
EXTRACTION = REPO / "data/fabfos/benchmarks/eydallin/extraction.tsv"
OUT = REPO / "data/fabfos/runs/eydallin_clones/gpr"

HOST = "e_coli_ag1"
COHORT = "eydallin"
SOURCE_ORGANISM = "e_coli_w3110"
PREFIX = "denovo"

GPR_COLS = (
    "build_id", "host", "unit_id", "feature_id", "feature_kind", "feature_name",
    "mnxr", "channel", "evidence_id", "evidence_name", "raw_score",
    "projection_via", "in_atom_universe", "gpr_rule",
    "condition_id", "cohort", "action", "source_organism",
)


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

    # The same universe B1 marked its rows against: the bake's coverage less transport.
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

    df = pd.DataFrame({
        "build_id": f"denovo_{COHORT}_" + "+".join(lanes),
        "host": HOST,
        # Not a model: this table's claim is "the lanes infer these reactions from the
        # clone's sequence", and naming a GEM here would imply one was consulted.
        "unit_id": "clones",
        "feature_id": g["orf"],
        "feature_kind": "clone_gene",
        "feature_name": g["intermediate_name"],
        "mnxr": g["mnxr"],
        "channel": PREFIX + "_" + g["channel"].astype(str),
        "evidence_id": g["intermediate_id"],
        "evidence_name": g["intermediate_name"],
        # Carried through, unlike the GEM table's uniform 1.0: here the score IS evidence
        # strength.
        "raw_score": g["raw_score"].astype(np.float32),
        "projection_via": g["projection_via"],
        "in_atom_universe": g["mnxr"].isin(universe),
        # No boolean rule: a de-novo call is per ORF, and inventing a one-gene rule would
        # make the two tables look like the same kind of claim.
        "gpr_rule": None,
        "condition_id": COHORT + ":" + g["orf"].astype(str),
        "cohort": COHORT,
        "action": "add",
        "source_organism": SOURCE_ORGANISM,
    })[list(GPR_COLS)]
    df = df.sort_values(["condition_id", "channel", "mnxr"],
                        kind="mergesort").reset_index(drop=True)

    out_dir = OUT if a.publish else (HERE / "out")
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / "gpr_denovo.parquet", index=False, compression="zstd")
    (out_dir / "BUILD_denovo.json").write_text(json.dumps(dict(
        lane_set=lane_set, lanes=lanes, universe=stats, host=HOST, cohort=COHORT,
        rows=len(df), clones=int(df["feature_id"].nunique()),
        mnxr=int(df["mnxr"].nunique()),
        clones_in_universe=int(df[df["in_atom_universe"]]["feature_id"].nunique()),
        mnxr_in_universe=int(df[df["in_atom_universe"]]["mnxr"].nunique()),
    ), indent=2))

    in_uni = df[df["in_atom_universe"]]
    print(f"\n{len(df):,} rows over {df['feature_id'].nunique()} clones, "
          f"{df['mnxr'].nunique():,} distinct MNXR")
    print(f"    inside the atom universe: {in_uni['feature_id'].nunique()} clones, "
          f"{in_uni['mnxr'].nunique():,} MNXR")
    per_lane = (df.groupby("channel")
                  .agg(rows=("mnxr", "size"), clones=("feature_id", "nunique"),
                       mnxr=("mnxr", "nunique")))
    print(per_lane.to_string())
    print(f"\n-> {out_dir}/gpr_denovo.parquet")
    return 0


if __name__ == "__main__":
    sys.exit(main())
