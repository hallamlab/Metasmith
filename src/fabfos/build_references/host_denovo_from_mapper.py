"""One host's de-novo GPR table, from one mapper table. B2's collector, by hand.

    mamba run -n figure-net python build_references/host_denovo_from_mapper.py \\
        --host e_coli_dh1 --mapper data/scratch/clone_gpr_sockeye_NC_017638.1/results \\
        [--publish]

`benchmark/host_gpr_denovo.py` does this inside the graph, over the whole host set at
once, and that is still where it belongs. This exists because the fan-out route it sits
on could not be trusted on the run that produced these tables: with five proteomes in one
workflow, the recorded lineage does not distinguish them -- `--collect` found three of
five lane outputs attributed to the wrong proteome, and the assemble driver's own check
refused the staged lineage outright ("its 5 files name only 1 distinct parent"). Content
attribution rescued the lane outputs; it cannot rescue a mapper table that was built from
the wrong pairing in the first place.

SO THE FAN-OUT IS AVOIDED RATHER THAN PATCHED. `examples/clone_gpr_on_hpc.py` runs the
same four lanes and the same mapper over ONE ORF set, where there is no pairing to get
wrong -- five jobs and one table, and the table can only describe the FASTA that went in.
This step then does what the collector would have: attach the host and re-emit on the
frozen 14-column schema.

THE COLUMN MAPPING IS THE TRANSFORM'S, and it is a third copy of it (the study-side
`main/benchmarks/eydallin/build_clone_gpr_denovo.py` is the second). That is a cost worth
naming: if the schema moves, these move together or the two lines of evidence stop lining
up column for column, which is the only thing they are jointly good for.
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
GENOMES = REPO / "data" / "fabfos" / "originals" / "genomes"

PREFIX = "denovo"
GPR_COLS = (
    "build_id", "host", "unit_id", "feature_id", "feature_kind", "feature_name",
    "mnxr", "channel", "evidence_id", "evidence_name", "raw_score",
    "projection_via", "in_atom_universe", "gpr_rule",
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", required=True)
    ap.add_argument("--mapper", type=Path, required=True,
                    help="a retrieved results tree, or the parquet itself")
    ap.add_argument("--publish", action="store_true")
    a = ap.parse_args()

    src = a.mapper
    if src.is_dir():
        hits = sorted(glob.glob(str(src / "annotation-gpr_table" / "*.parquet")))
        if len(hits) != 1:
            raise SystemExit(f"expected one mapper table under {src}, found {hits}")
        src = Path(hits[0])
    g = pd.read_parquet(src)

    # THE ORFS MUST BE THIS HOST'S, checked rather than trusted. The whole reason this
    # file exists is a run whose bookkeeping paired tables with the wrong proteome, so
    # the one check worth having is that every ORF in the table is a record of the
    # proteome being claimed.
    faa = sorted((GENOMES / a.host / "genome").glob("*.faa"))
    if len(faa) != 1:
        raise SystemExit(f"expected one proteome under {a.host}/genome, found {faa}")
    ids = {ln[1:].split()[0] for ln in faa[0].open() if ln.startswith(">")}
    stray = sorted(set(g["orf"]) - ids)
    if stray:
        raise SystemExit(f"{len(stray)} ORF(s) in {src.name} are not records of "
                         f"{faa[0].name}: {stray[:5]} -- this table is not {a.host}'s")

    lanes = sorted(g["channel"].unique())
    print(f"{src.name}: {len(g):,} rows, {g['orf'].nunique():,} ORFs, "
          f"{g['mnxr'].nunique():,} MNXR, lanes {lanes}")

    df = pd.DataFrame({
        "build_id": f"denovo_{a.host}_" + "+".join(lanes),
        "host": a.host,
        "unit_id": "proteome",
        "feature_id": g["orf"],
        "feature_kind": "orf",
        "feature_name": g["intermediate_name"],
        "mnxr": g["mnxr"],
        "channel": PREFIX + "_" + g["channel"].astype(str),
        "evidence_id": g["intermediate_id"],
        "evidence_name": g["intermediate_name"],
        "raw_score": g["raw_score"].astype(np.float32),
        "projection_via": g["projection_via"],
        # Null, exactly as the transform leaves it: the bake is not staged here either,
        # and a guessed `in_atom_universe` is worse than an absent one because the
        # consumer trusts it.
        "in_atom_universe": pd.Series([None] * len(g), dtype="object"),
        "gpr_rule": None,
    })[list(GPR_COLS)]
    df = df.sort_values(["feature_kind", "feature_id", "mnxr", "channel"],
                        kind="mergesort", na_position="last").reset_index(drop=True)

    out = (REPO / "data" / "fabfos" / a.host / "gpr" if a.publish
           else Path(__file__).resolve().parent / "out" / a.host)
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out / "gpr_denovo.parquet", index=False, compression="zstd")
    (out / "BUILD_denovo.json").write_text(json.dumps(dict(
        host=a.host, source=str(src), proteome=faa[0].name, lanes=lanes,
        n_lanes=len(lanes), rows=len(df), orfs=int(df["feature_id"].nunique()),
        mnxr=int(df["mnxr"].nunique()),
        route="clone_gpr_on_hpc.py per ORF set; see this module's docstring",
    ), indent=2))
    print(f"{len(df):,} rows -> {out}/gpr_denovo.parquet")
    return 0


if __name__ == "__main__":
    sys.exit(main())
