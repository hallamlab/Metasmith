"""Run the conditions tier's two drivers here, on this tree's bytes, without metasmith.

WHY THIS EXISTS. `examples/benchmark_conditions_dag.py` is plan-only and says why: the
cohort tables are human judgement, so a run on whatever happens to be on a disk is not
the benchmark. That argument is about PUBLISHING, not about measuring. Growing
entry-to-reaction coverage needs a before/after read on the same bytes, and the honest
way to get one is to execute the transform's own DRIVER string rather than a paraphrase
of it -- a copy of the logic measures the copy.

So this lifts `DRIVER` (and the literals it is formatted with) straight out of each
transform module by AST, formats it against paths in this worktree, and execs it. The
transform is never imported, so `metasmith.python_api` is not needed and the driver text
under test is byte-identical to the one a real run would write.

    mamba run -n msm-fabfos python build_references/run_benchmark_conditions_local.py \\
        --out data/scratch/bench_conditions_local

Outputs land under `--out` and are NOT a publishable artifact: the type declarations,
the BUILD.json and the DVC pin all come from a real run. Read them, compare them, throw
them away.
"""
from __future__ import annotations

import argparse
import ast
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BREF = REPO / "build_references"
TRANSFORMS = BREF / "transforms" / "benchmark"
BUILDLIB = BREF / "resources" / "buildlib"

# Every path a driver is formatted with, resolved in this worktree. Named here rather
# than threaded through argparse so a run of this script cannot quietly point at a
# different tree's extractions than the one it reports on.
GIVENS = {
    "extracts": REPO / "data" / "benchmarks" / "_extractions",
    "het":      REPO / "data" / "originals" / "benchmarks" / "het_screen",
    "bridge":   REPO / "data" / "processed" / "mnxr_lookup" / "mnxr_lookup.parquet",
    "metanetx": REPO / "data" / "originals" / "metanetx",
    # `raw::laser_records` -- the upstream checkout, for `inputs/Gene-Reaction
    # Pairings.txt`. NOT the extraction: this is the repository LASER publishes.
    "laser":    REPO / "data" / "originals" / "benchmarks" / "laser",
    "hosts_gem": REPO / "data" / "benchmarks",
}


def module_literals(path: Path) -> dict:
    """Module-level literal assignments, read WITHOUT importing.

    The transform modules open with `from metasmith.python_api import *` and build a
    `Transform()` at import time; neither is needed to read a string constant, and
    importing them would drag the engine in for no reason.
    """
    tree = ast.parse(path.read_text())
    out = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        try:
            out[target.id] = ast.literal_eval(node.value)
        except ValueError:
            continue
    return out


def run_driver(name: str, fmt: dict, work: Path) -> None:
    src = TRANSFORMS / f"{name}.py"
    lits = module_literals(src)
    if "DRIVER" not in lits:
        raise SystemExit(f"[local] {src} has no module-level DRIVER string")
    driver = lits["DRIVER"].format(**fmt)
    script = work / f"_{name}.py"
    script.write_text(driver)
    print(f"\n{'=' * 78}\n[local] {name}\n{'=' * 78}", flush=True)
    env = dict(os.environ, PYTHONPATH=str(BUILDLIB))
    r = subprocess.run([sys.executable, str(script)], cwd=work, env=env)
    if r.returncode:
        raise SystemExit(f"[local] {name} exited {r.returncode}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="data/scratch/bench_conditions_local",
                    help="where the two products land (relative to the repo root)")
    ap.add_argument("--only", choices=("condition_gpr", "conditions"), default=None)
    args = ap.parse_args()

    work = (REPO / args.out) if not Path(args.out).is_absolute() else Path(args.out)
    work.mkdir(parents=True, exist_ok=True)

    missing = [k for k, v in GIVENS.items() if not v.exists()]
    if missing:
        raise SystemExit(f"[local] givens absent from this tree: {missing}")

    cond_gpr_out = work / "condition_gpr.parquet"
    conditions_out = work / "conditions.tsv"

    # `lib` is the driver's sys.path anchor: every driver does
    # `sys.path.insert(0, os.path.dirname("{lib}"))`, so it must be a FILE inside buildlib.
    lib_anchor = BUILDLIB / "bench_cohorts.py"

    steps = [
        ("condition_gpr", dict(
            lib=lib_anchor, extracts=GIVENS["extracts"], het=GIVENS["het"],
            bridge=GIVENS["bridge"], metanetx=GIVENS["metanetx"],
            laser=GIVENS["laser"], hosts_gem=GIVENS["hosts_gem"],
            out=cond_gpr_out)),
        ("conditions", dict(
            lib=lib_anchor, cond_gpr=cond_gpr_out, extracts=GIVENS["extracts"],
            het=GIVENS["het"], out=conditions_out,
            columns=repr(module_literals(TRANSFORMS / "conditions.py")["COLUMNS"]),
            elements=repr(module_literals(TRANSFORMS / "conditions.py")["ELEMENTS"]))),
    ]
    for name, fmt in steps:
        if args.only and name != args.only:
            continue
        run_driver(name, fmt, work)

    print(f"\n[local] products under {work}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
