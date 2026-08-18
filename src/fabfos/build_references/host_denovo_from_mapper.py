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

def _repo_root(start: Path) -> Path:
    """Walk up until a directory holding `data/fabfos` is found."""
    for d in (start, *start.parents):
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


REPO = _repo_root(Path(__file__).resolve())
GENOMES = REPO / "data" / "fabfos" / "originals" / "genomes"

# The declared lane set is read from the library that declares it, never restated here.
sys.path.insert(0, str(REPO / "src" / "metasmith_libraries" / "resources" / "lib"))
import fabfos_evidence as fe                                          # noqa: E402

LANE_SET = "chosen_4"
# The host de-novo layer's blocks. `cohort` is a study's, not a host's.
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

    # The same gate the in-graph collector applies, because this is the same claim by
    # another route. A lane short of the declared set rescales every belief weight
    # downstream, so it is refused here rather than recorded and shipped.
    expected = sorted(fe.LANE_SETS[LANE_SET])
    if lanes != expected:
        raise SystemExit(
            f"{src.name}: lane set is {lanes}, not {expected}. Missing "
            f"{sorted(set(expected) - set(lanes))}; unexpected "
            f"{sorted(set(lanes) - set(expected))}. A lane that contributed no rows is a "
            f"broken join or an unstaged reference.")

    # The mapper's own columns are the core, carried through unchanged -- the channel
    # keeps the frozen spelling, and `lane_set` is what says these rows are de-novo
    # evidence rather than a curated assertion. Attribution is what this step adds.
    df = g.copy()
    df["build_id"] = f"denovo_{a.host}_" + "+".join(lanes)
    df["host"] = a.host
    # The unit is the proteome the lanes were keyed on, not a model: naming a GEM here
    # would imply a curated model was consulted, which is the whole thing the de-novo
    # line is not.
    df["unit_id"] = df["source"]
    df["feature_kind"] = "orf"
    df["feature_name"] = df["intermediate_name"]
    # There is no boolean rule: a de-novo call is per ORF, and inventing "orf" as a
    # one-gene rule would make this look like the same kind of claim as a GEM's.
    df["gpr_rule"] = None
    # Null, exactly as the in-graph collector leaves it: the bake is not staged here,
    # and a guessed `in_atom_universe` is worse than an absent one because the consumer
    # trusts it.
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
