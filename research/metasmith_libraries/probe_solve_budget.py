#!/usr/bin/env python3
"""Time one template's solve under a hard wall-clock budget.

A plan a user is waiting on has a budget, and this is how you find out whether a
change spent it. The budget is enforced with a real alarm rather than measured
afterwards, so a search that has gone combinatorial is reported as such instead
of holding the terminal for twenty minutes.

Three knobs, because the two axes that actually move solve time are not the ones
you would guess:

    --targets N     solve for only the first N of the template's targets
    --exclude       mask transforms out by path substring
    --lib-root      resolve the template's libraries under a different tree

`--exclude` is the important one. The solver branches wherever a requirement has
more than one reachable producer, and every independent target downstream of
such a fork multiplies that branch -- so masking one of a competing pair is how
you attribute a blowup to the ambiguity rather than to the transform. See
`probe_ambiguity.py` for the map of where those forks are.

    python research/metasmith_libraries/probe_solve_budget.py \
        --template metagenomics_from_paired_reads --targets 22 --budget 5

Timings are per-process on purpose: the transform loader mutates `sys.path` and
caches per root, so two solves in one interpreter are not two measurements.
"""

from __future__ import annotations

import argparse
import importlib
import json
import signal
import sys
import time
from pathlib import Path

MLIB = Path(__file__).resolve().parents[2] / "src" / "metasmith_libraries"
sys.path.insert(0, str(MLIB))

from metasmith.models.solver_backend import Backend, UsePythonSolver  # noqa: E402
from metasmith.python_api import Spec, TransformInstanceLibrary  # noqa: E402


class OverBudget(Exception):
    pass


def _ring(signum, frame):
    raise OverBudget()


def load_libs(paths, exclude: list[str]):
    libs, masked = [], []
    for p in paths:
        lib = TransformInstanceLibrary.Load(Path(str(p)).resolve())
        if exclude:
            # invert=True excludes; the default keeps only what is named, which
            # silently plans against one transform and reports every target
            # dropped.
            hide = {k for k in lib.manifest if any(x in str(k) for x in exclude)}
            if hide:
                masked += sorted(str(h) for h in hide)
                lib = lib.AsView(hide, invert=True)
        libs.append(lib)
    return libs, masked


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--template", default="metagenomics_from_paired_reads")
    ap.add_argument("--targets", type=int, default=0, help="first N targets only (0 = all)")
    ap.add_argument("--libs", default="", help="comma-separated library names, overriding the template's")
    ap.add_argument("--lib-root", default="", help="resolve the template's libraries under this tree")
    ap.add_argument("--exclude", default="", help="comma-separated substrings; matching transforms are masked out")
    ap.add_argument("--budget", type=float, default=5.0, help="seconds; the solve is killed at this mark")
    ap.add_argument("--python-solver", action="store_true", help="revert to the python search")
    ap.add_argument("--engine-dir", default="", help="stage msm_solver from here instead of the package")
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    if args.engine_dir:
        from metasmith.models import solver_engine as SE
        SE.ENGINE_DIR = Path(args.engine_dir)
        SE.ResetEngineCache()

    out = {"label": args.label or args.template, "template": args.template,
           "budget": args.budget,
           "solver": "python" if args.python_solver else Backend("solve")}

    t0 = time.perf_counter()
    spec: Spec = importlib.import_module(args.template).build_spec()
    out["load_spec_s"] = round(time.perf_counter() - t0, 3)

    if args.targets:
        spec.target_types = spec.target_types[: args.targets]
    out["n_targets"] = len(spec.target_types)

    if args.lib_root:
        root = Path(args.lib_root)
        lib_paths = [root / Path(str(p)).name for p in spec.transform_libraries]
    elif args.libs:
        lib_paths = [MLIB / "transforms" / n for n in args.libs.split(",")]
    else:
        lib_paths = list(spec.transform_libraries)
    out["libs"] = [Path(str(p)).name for p in lib_paths]

    t0 = time.perf_counter()
    libs, masked = load_libs(lib_paths, [x for x in args.exclude.split(",") if x])
    out["load_libs_s"] = round(time.perf_counter() - t0, 3)
    out["masked"] = masked
    spec.transform_libraries = libs

    signal.signal(signal.SIGALRM, _ring)
    signal.setitimer(signal.ITIMER_REAL, args.budget)
    t0 = time.perf_counter()
    try:
        task = spec.Solve() if not args.python_solver else _python_solve(spec)
        signal.setitimer(signal.ITIMER_REAL, 0)
        dt = time.perf_counter() - t0
        out.update(solve_s=round(dt, 3), over_budget=dt > args.budget,
                   ok=bool(task.ok), steps=len(task.plan.steps),
                   dropped=sorted(str(d) for d in task.plan.dropped_targets))
        if not task.ok:
            out["hints"] = [str(h)[:400] for h in (task.plan.hints or [])][:4]
    except OverBudget:
        signal.setitimer(signal.ITIMER_REAL, 0)
        out.update(solve_s=None, over_budget=True, ok=None, steps=None,
                   note=f"killed at the {args.budget}s budget")
    except Exception as e:  # a broken library is a result, not a crash
        signal.setitimer(signal.ITIMER_REAL, 0)
        out.update(solve_s=round(time.perf_counter() - t0, 3), over_budget=False,
                   ok=False, error=f"{type(e).__name__}: {e}"[:400])

    print("SOLVE_BUDGET " + json.dumps(out))
    return 0 if out.get("ok") and not out["over_budget"] else 1


def _python_solve(spec: Spec):
    with UsePythonSolver():
        return spec.Solve()


if __name__ == "__main__":
    sys.exit(main())
