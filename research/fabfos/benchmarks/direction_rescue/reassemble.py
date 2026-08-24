#!/usr/bin/env python3
"""Re-run the direction assembly locally and check it against a bake's own annotation.

`direction_ensemble` is three commands -- `curated`, `calibrate`, `combine` -- and none of
them imports heavy chemistry, so all three run here off artifacts already on disk. Against
r8's own inputs the result is frame-identical to the deployed
`seams/direction_annotation.parquet`, whose sha256 IS `src_direction_sha256`, in about
fifteen seconds.

WHAT THAT BUYS IS ATTRIBUTION. A re-bake moves three things at once: the member tables,
the curated calibration fitted on them, and sigma_0. Run this with one input swapped at a
time and each arm becomes its own row-count delta instead of a caveat on a single number.
The members still have to run on the cluster; everything downstream of them does not.

    # reproduce the deployed bake from its own seams -- the instrument's own check
    PYTHONPATH=src mamba run -n msm python \\
        research/fabfos/benchmarks/direction_rescue/reassemble.py --out <dir>

    # r9 members, r8's calibration and sigma_0: the chemistry arm alone
    ... --eq <r9_eq.parquet> --dgbyg <r9_dgbyg.parquet> --expect <r9_annotation.parquet>

    # the curated crosswalk arm alone: r8's members, r9's curated table
    ... --supplementary-crosswalk

`curated` needs the licensed MetaCyc drop-in; without it, pass `--curated` a table from a
previous run (the lane leaves one under `data/fabfos/temp/metacyc_direction/`).
"""
from __future__ import annotations

import argparse
import resource
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[4]
MNX = REPO / "data/fabfos/originals/metanetx/4.5"
PROCESSED = REPO / "data/fabfos/processed"


def run(label: str, *args: object) -> None:
    # One assembly command, with its wall clock and peak RSS.
    #
    # RSS is read from the child rusage rather than /usr/bin/time so the numbers land in the
    # same place as the rest of the output -- the resource declaration this replaces was
    # never measured, and a measurement nobody prints is the same thing again.
    before = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    t0 = time.monotonic()
    r = subprocess.run([sys.executable, "-m", f"ecspr.bake.direction.{label}",
                        *[str(a) for a in args]])
    if r.returncode:
        raise SystemExit(f"[reassemble] {label} exited {r.returncode}")
    peak = max(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss, before)
    print(f"[reassemble] {label}: {time.monotonic() - t0:.1f} s, peak RSS "
          f"{peak / 1024:.0f} MB (children high-water)", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--bake", default="metabolism_bake",
                    help="chunk supplying the member seams and the annotation to check")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--eq", type=Path, default=None)
    ap.add_argument("--dgbyg", type=Path, default=None)
    ap.add_argument("--curated", type=Path, default=None,
                    help="skip the MetaCyc parse and reuse this table")
    ap.add_argument("--calibration", type=Path, default=None,
                    help="skip the fit and reuse this table -- how the calibration arm is "
                         "held fixed while the chemistry arm moves")
    ap.add_argument("--sigma0", default=None,
                    help="passed to combine; omit to use canon.DIR_SIGMA_0")
    ap.add_argument("--metacyc", type=Path, default=None,
                    help="MetaCyc reactions.dat (default: newest under data/fabfos/originals)")
    ap.add_argument("--balance-gate", default=None,
                    help="r10 arm: where the raw reac_prop balance test runs relative to "
                         "the member ('before_member' is r9)")
    ap.add_argument("--clamp", default=None,
                    help="r10 arm: magnitude bound in kJ/mol ('100' is what r9 was baked "
                         "with; canon has since moved to three decades without a re-bake)")
    ap.add_argument("--prior-width", default=None,
                    help="r10 arm: which stored spread the curated prior uses "
                         "('tau' is r9, 'robust' is r10)")
    ap.add_argument("--supplementary-crosswalk", action="store_true",
                    help="build the curated table with the r9 supplementary crosswalk; "
                         "ignored when --curated supplies a table already built")
    ap.add_argument("--expect", type=Path, default=None,
                    help="annotation to compare against (default: the bake's own seam)")
    a = ap.parse_args()

    bake = Path(a.bake) if Path(a.bake).is_absolute() else PROCESSED / a.bake
    out = a.out
    out.mkdir(parents=True, exist_ok=True)
    eq = a.eq or bake / "seams/direction_member_eq.parquet"
    dgbyg = a.dgbyg or bake / "seams/direction_member_dgbyg.parquet"
    expect = a.expect or bake / "seams/direction_annotation.parquet"

    run("drive", "universe", "--reac-prop", MNX / "reac_prop.tsv",
        "--out", out / "_universe.json")

    curated = a.curated
    if curated is None:
        dat = a.metacyc or next(iter(sorted(
            (REPO / "data/fabfos/originals/metacyc").rglob("reactions.dat"))), None)
        if dat is None:
            raise SystemExit("[reassemble] no MetaCyc reactions.dat found; pass --curated "
                             "a table from a previous run, or --metacyc the drop-in")
        curated = out / "_curated_per_mnxr.parquet"
        run("curated", "--metacyc-reactions", dat,
            "--reac-xref", MNX / "reac_xref.tsv", "--reac-prop", MNX / "reac_prop.tsv",
            "--chem-xref", MNX / "chem_xref.tsv", "--out", curated,
            "--out-per-reaction", out / "_curated_per_reaction.parquet",
            *(["--supplementary-crosswalk"] if a.supplementary_crosswalk else []))

    calibration = a.calibration
    if calibration is None:
        calibration = out / "_calibration.parquet"
        run("calibrate", "--curated", curated, "--reac-prop", MNX / "reac_prop.tsv",
            "--eq-member", eq, "--out-calibration", calibration,
            "--out-points", out / "_calibration_points.parquet",
            *(["--balance-gate", a.balance_gate] if a.balance_gate else []))

    combine = ["--base-mnxrs", out / "_universe.json", "--eq", eq, "--dgbyg", dgbyg,
               "--curated", curated, "--calibration", calibration,
               "--out", out / "direction_annotation.parquet"]
    if a.sigma0 is not None:
        combine += ["--sigma0", a.sigma0]
    if a.prior_width is not None:
        combine += ["--prior-width", a.prior_width]
    if a.clamp is not None:
        combine += ["--clamp", a.clamp]
    run("combine", *combine)

    got = pd.read_parquet(out / "direction_annotation.parquet")
    want = pd.read_parquet(expect)
    print(f"\n[reassemble] against {expect}")
    g = got.sort_values("mnxr").reset_index(drop=True)
    w = want.sort_values("mnxr").reset_index(drop=True)
    # A re-bake may ADD a column -- r9 adds `sigma_sub` -- and that is a schema change to
    # report, not a comparison failure. Dropping one is different and stays fatal: a
    # consumer reading it would break.
    added, gone = [c for c in g.columns if c not in w.columns], \
                  [c for c in w.columns if c not in g.columns]
    if gone:
        raise SystemExit(f"[reassemble] COLUMNS DROPPED: {gone}")
    if added:
        print(f"[reassemble] new columns (not compared): {added}")
        g = g[list(w.columns)]
    if g.equals(w):
        print(f"[reassemble] IDENTICAL over {len(g):,} reactions")
        return
    print(f"[reassemble] DIFFERS over {len(g):,} reactions:")
    for c in g.columns:
        if g[c].equals(w[c]):
            continue
        d = (g[c] != w[c]) & ~(g[c].isna() & w[c].isna())
        print(f"  {c:<18} {int(d.sum()):>7,} rows")
    for t in sorted(set(w.dir_tier) | set(g.dir_tier)):
        print(f"  dir_tier {t}: {int((w.dir_tier == t).sum()):>7,} -> "
              f"{int((g.dir_tier == t).sum()):>7,}")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
