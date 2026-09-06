"""A pytest plugin that adjudicates every plan a test session actually solved.

    MSM_SWEEP_OUT=out.json pytest tests/metasmith_libraries -m "not slow" \
        -p research.metasmith.witness_sweep.capture_plans

The real-library test corpus builds its workflows inside session fixtures, so
there is no module-level spec to import. Wrapping `Spec.SolveViews` -- the one
function every driver, template and test reaches the search through -- collects
them without re-authoring any of them, and without the collection being able to
disagree with what the tests actually plan.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

_CAPTURED: list = []


def pytest_configure(config):
    from metasmith.agents.spec import Spec

    original = Spec.SolveViews.__func__ if hasattr(Spec.SolveViews, "__func__") else Spec.SolveViews

    def recording(*args, **kwargs):
        task = original(*args, **kwargs)
        _CAPTURED.append(task)
        return task

    Spec.SolveViews = staticmethod(recording)


def pytest_sessionfinish(session, exitstatus):
    out = os.environ.get("MSM_SWEEP_OUT")
    if not out:
        return
    from metasmith.testing.solver_verification import plan_fingerprint, problem_of_plan
    from metasmith.testing.witness_sweep import CaseResult, summarise, sweep_problem

    results: list[CaseResult] = []
    seen: set[str] = set()
    for i, task in enumerate(_CAPTURED):
        result = getattr(task.plan, "_solver_result", None)
        problem = problem_of_plan(task.plan, name=f"libtest-{i}")
        if problem is None or result is None:
            continue
        fp = plan_fingerprint(result)
        # One case per distinct plan. The corpus re-solves the same workflow from
        # several tests, and a duplicate row would inflate every count in the
        # decoy matrix without adjudicating anything new.
        if fp in seen:
            continue
        seen.add(fp)
        name = f"libtest/{len(results):02d}-" + "+".join(
            sorted({s.transform.key[:8] for s in task.plan._solver_result.dependency_plan})
        )[:60]
        results.append(sweep_problem(name, problem, expect_fingerprint=fp))
    Path(out).write_text(json.dumps(summarise(results), indent=2, sort_keys=True))
    print(f"\n[capture_plans] {len(_CAPTURED)} solves seen, {len(results)} distinct plans swept -> {out}")
