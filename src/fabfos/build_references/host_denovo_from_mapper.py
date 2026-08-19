"""One host's de-novo GPR table, from one mapper table. B2's collector, by hand.

    mamba run -n msm-fabfos python src/fabfos/build_references/host_denovo_from_mapper.py \\
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
This step then does what the collector would have: attach the host and re-emit with the
attribution, feature and universe blocks the benchmark layer needs.

THE SCHEMA IS READ, NOT RESTATED. The columns, the grain and the validator all come from
`lib::fabfos_evidence`, which is the only reason this step and the in-graph collector
cannot drift apart -- they used to carry a copy of the mapping each, and a third lived on
the study side.
"""
from __future__ import annotations

import argparse
import glob
import json
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
GENOMES = REPO / "data" / "fabfos" / "originals" / "genomes"

sys.path.insert(0, str(REPO / "src" / "metasmith_libraries" / "resources" / "lib"))
import fabfos_evidence as fe                                          # noqa: E402

LANE_SET = "chosen_4"
EXTENSIONS = ("attribution", "feature", "universe")


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

    expected = sorted(fe.LANE_SETS[LANE_SET])
    if lanes != expected:
        raise SystemExit(
            f"{src.name}: lane set is {lanes}, not {expected}. Missing "
            f"{sorted(set(expected) - set(lanes))}; unexpected "
            f"{sorted(set(lanes) - set(expected))}. A lane that contributed no rows is a "
            f"broken join or an unstaged reference.")

    df = g.copy()
    df["build_id"] = f"denovo_{a.host}_" + "+".join(lanes)
    df["host"] = a.host
    df["unit_id"] = df["source"]
    df["feature_kind"] = "orf"
    df["feature_name"] = df["intermediate_name"]
    df["gpr_rule"] = None
    df["in_atom_universe"] = pd.Series([None] * len(df), dtype="object")
    df = df[fe.schema_for(EXTENSIONS)]
    df = df.sort_values(fe.grain_key(EXTENSIONS), kind="mergesort",
                        na_position="last").reset_index(drop=True)
    fe.validate_gpr(df, LANE_SET, ids, df["source"].iat[0], EXTENSIONS)

    out = (REPO / "data" / "fabfos" / "runs" / a.host / "gpr" if a.publish
           else Path(__file__).resolve().parent / "out" / a.host)
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out / "gpr_denovo.parquet", index=False, compression="zstd")
    (out / "BUILD_denovo.json").write_text(json.dumps(dict(
        host=a.host, source=str(src), proteome=faa[0].name, lane_set=LANE_SET,
        lanes=lanes, n_lanes=len(lanes), extensions=list(EXTENSIONS),
        rows=len(df), orfs=int(df["orf"].nunique()),
        mnxr=int(df["mnxr"].nunique()),
        route="clone_gpr_on_hpc.py per ORF set; see this module's docstring",
    ), indent=2))
    print(f"{len(df):,} rows -> {out}/gpr_denovo.parquet")
    return 0


if __name__ == "__main__":
    sys.exit(main())
