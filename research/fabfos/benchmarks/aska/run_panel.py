#!/usr/bin/env python3
"""Run the ASKA/FFA panel through the packaged ECSPr command line.

Four `ecspr` invocations and no solver code: `draw` mints the null pool as a
conditions table and a tests table, the two probes run over the observed pair and
over that pool with the SAME command, and `score` turns the two into deltas and
z-scores. The null arm being the identical probe call with a different pair of
files is the whole reason a z means anything here.

THE GATE IS RUN FIRST AND ON PURPOSE. `--draws 32` costs a couple of minutes and
answers the expensive question before the thousand is drawn: is the null's spread
large compared with the CONTROLS' spread? The controls are masks that reach no
atom-mapped reaction, so they must return the baseline exactly and their spread is
the solver's numerical floor. If the null does not clear that floor, every z below
it is noise over noise, and no number of draws fixes it.

    ./dev.sh -e --where                     # confirm which ecspr is imported
    python main/benchmarks/aska/run_panel.py --draws 32     # the gate
    python main/benchmarks/aska/run_panel.py --draws 1000   # the run

Solves shard per condition and resume from what is on disk, because this
workstation kills long local jobs and a thousand-draw null is exactly the shape of
run that gets killed at draw 900.

THE TWO ARMS SHARE A POOL BY SHARING A FILE, and the pool is seeded once. Re-drawing
with a different seed voids the comparison, so the seed is an argument with a
default rather than something regenerated per run.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
CACHE = HERE / "cache"
OUT = HERE / "out"

sys.path.insert(0, str(HERE.parent))
import bake_identity                                                          # noqa: E402

PAIRS = ROOT / "data/fabfos/benchmark/reference_tier4/atom_pairs_tier4.parquet"
DIRECTION = CACHE / "direction_ratios.parquet"
HOST = ROOT / "data/fabfos/runs/e_coli_k12/gpr/gpr_gem.parquet"
CLONES = CACHE / "gpr_clones.parquet"
TESA = CACHE / "gpr_tesa.parquet"
POOL = CACHE / "gpr_null_pool.parquet"
OBSERVED = CACHE / "conditions_observed.tsv"
OBSERVED_TESTS = CACHE / "tests_observed.tsv"
LIKE = CACHE / "tests_like.tsv"

PROBES = ("two-point", "ground")
BASELINE = "aska_ffa:BASELINE"


def ecspr(*args, log: Path):
    cmd = [sys.executable, "-m", "ecspr", *[str(a) for a in args]]
    print("+ " + " ".join(cmd), flush=True)
    with log.open("a") as fh:
        fh.write("\n+ " + " ".join(cmd) + "\n")
    r = subprocess.run(cmd, cwd=ROOT)
    if r.returncode:
        raise SystemExit(f"[panel] {args[0]} exited {r.returncode}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--draws", type=int, default=32,
                    help="draws per size stratum; 32 for the gate, 1000 for the run")
    ap.add_argument("--seed", type=int, default=20260812,
                    help="seeded once; re-drawing voids the comparison")
    ap.add_argument("--weighting", default="uniform", choices=("uniform", "belief"))
    ap.add_argument("--probes", nargs="+", default=list(PROBES), choices=PROBES)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    tag = f"n{args.draws}"
    out = args.out or (OUT / tag)
    out.mkdir(parents=True, exist_ok=True)
    log = out / "run.log"

    basis = ["--atom-pairs", PAIRS, "--direction", bake_identity.require_fresh(DIRECTION),
             "--element", "C", "--weighting", args.weighting]

    null_conditions = out / "conditions_null.tsv"
    null_tests = out / "tests_null.tsv"
    if not null_conditions.exists():
        # `--background` IS WHY THE DROP NO LONGER HAS TO BE CARRIED BY HAND. Every null
        # construction starts from the baseline's own steps -- host, plasmid, and the fadE
        # subtract -- so the constant background is in the null arm by construction rather
        # than by this script copying two columns onto every drawn row afterwards.
        ecspr("draw", "--gpr", POOL, "--like-tests", LIKE,
              "--like-conditions", OBSERVED, "--background", BASELINE,
              "--pool-table", POOL.name, "-n", args.draws,
              "--seed", args.seed, "--draw-column", "orf",
              "--out", null_conditions, "--out-tests", null_tests,
              "--log", log, log=log)
    else:
        print(f"[panel] reusing the drawn pool at {null_conditions}", flush=True)

    for probe in args.probes:
        arms = {
            "observed": (OBSERVED, OBSERVED_TESTS, [HOST, CLONES, TESA]),
            "null": (null_conditions, null_tests, [HOST, POOL, TESA]),
        }
        for arm, (conds, tests, gpr) in arms.items():
            res = out / f"{probe}_{arm}.parquet"
            ecspr(probe, *basis, "--gpr", *gpr, "--conditions", conds,
                  "--tests", tests,
                  "--out", res, "--shard-dir", out / f"shards_{probe}_{arm}",
                  "--log", log, log=log)

        ecspr("score",
              "--results", out / f"{probe}_observed.parquet",
              "--null", out / f"{probe}_null.parquet",
              "--tests", OBSERVED_TESTS, "--null-tests", null_tests,
              "--out", out / f"{probe}_scored.parquet",
              "--gate-out", out / f"{probe}_gate.tsv", "--log", log, log=log)

    print(f"\n[panel] products under {out}", flush=True)


if __name__ == "__main__":
    main()
