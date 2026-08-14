#!/usr/bin/env python3
"""Plan-only probe over the deep-learning embedding workflow.

Runs the solver against a chosen target set and reports which transforms it
picked. No agent, no staging, no remote work — the point is to see the plan.

It exists because `--only all` used to add a `downloadESMFoldWeights` step that
`--only esmc` did not, despite both having the weight tarball pre-staged; the
seed sweep below is how that was pinned to an MCTS local optimum rather than a
missing input.

    python main/probe_planner.py [case]        # see CASES
    python main/probe_planner.py all_seeds     # the sweep

Input paths are irrelevant to a plan, so they are deferred: what is being probed
is which transforms the solver reaches for, and nothing here opens a file.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

# _authoring/_dl_embeddings live with the template-authoring gate at
# src/metasmith_libraries/, a sibling of this research driver post-monorepo-move.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "metasmith_libraries"))

import _authoring as A
import _dl_embeddings as DL
from metasmith.python_api import DEFERRED, DataInstanceLibrary, TransformInstanceLibrary

CACHE_DIR = Path(__file__).resolve().parent / "cache"

ALL = list(DL.TARGETS)
CASES: dict[str, list[str]] = {
    "esmc_baseline":        ["esmc"],
    "esmfold_only":         ["esmfold"],
    "saprot_only":          ["saprot"],
    "esmc_plus_esmfold":    ["esmc", "esmfold"],
    "esmfold_plus_foldseek": ["esmfold", "foldseek_3di"],
    "esmfold_plus_saprot":  ["esmfold", "saprot"],
    "all_minus_saprot":     ["esmc", "ankh", "prott5", "esmfold"],
    "all_no_saprot_target": ["esmc", "ankh", "prott5", "esmfold", "foldseek_3di"],
    "all_no_foldseek_target": ["esmc", "ankh", "prott5", "esmfold", "saprot"],
    "all":                  ALL,
}


def probe(label: str, target_keys: list[str],
          max_iter: int = 1024, max_refine: int = 256, seed: int = 42):
    print(f"\n{'='*70}\n[{label}] targets={target_keys} "
          f"iter={max_iter} refine={max_refine} seed={seed}\n{'='*70}")

    location = CACHE_DIR / f"probe_{label}.xgdb"
    if location.exists(): shutil.rmtree(location)
    inputs = DataInstanceLibrary(location)
    inputs.Purge()
    DL.add_inputs(inputs, DEFERRED,
                  {w: DEFERRED for w in DL.needed_weights(target_keys)})
    inputs.Save()

    spec = DL.spec(inputs, target_keys)
    task = spec.Solve(max_iter=max_iter, max_refine=max_refine, seed=seed)
    if not task.ok:
        print(f"  PLAN FAILED steps={len(task.plan.steps)} "
              f"dropped={sorted(task.plan.dropped_targets)}")
        return

    names = {
        ti.model.key: (ti.name or str(path))
        for location in A.transforms("functionalAnnotation", "logistics")
        for path, ti in TransformInstanceLibrary.Load(location).IterateTransforms()
    }
    print(f"  OK steps={len(task.plan.steps)} key={task.GetKey()}")
    for i, step in enumerate(task.plan.steps, 1):
        ti = step.transform
        key = ti.model.key if hasattr(ti, "model") else getattr(ti, "_key", "?")
        print(f"    {i:2d}. {names.get(key, getattr(ti, 'name', None) or f'<unmapped {key}>')}")


if __name__ == "__main__":
    case = sys.argv[1] if len(sys.argv) > 1 else "esmc_baseline"
    if case == "all_seeds":
        for s in (1, 7, 13, 42, 99, 2024):
            probe(f"all_seed{s}", ALL, seed=s)
    elif case == "all_big_iter":
        probe("all_big_iter", ALL, max_iter=8192, max_refine=2048)
    elif case in CASES:
        probe(case, CASES[case])
    else:
        print(f"unknown case: {case}\nchoices: {', '.join([*CASES, 'all_seeds', 'all_big_iter'])}")
        sys.exit(1)
