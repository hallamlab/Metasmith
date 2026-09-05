from __future__ import annotations

import time

import pytest

from metasmith.models.solver_engine import EngineFor
from metasmith.testing.solver_bench import SWEEP_PROFILES
from metasmith.testing.solver_verification import (
    check_plan,
    generate_problem,
    plan_fingerprint,
)


@pytest.fixture(scope="module")
def engine():
    info = EngineFor("solve")
    if info is None:
        pytest.skip("no msm_solver advertising `solve` (./dev/metasmith.sh -bel)")
    return info


def _sink_178():
    return generate_problem(178, dict(SWEEP_PROFILES)["sink"], name="sink")


def test_the_search_alone_is_cheap_on_the_instance_the_refiner_cannot_finish(engine):
    problem = _sink_178()
    t0 = time.perf_counter()
    solution = problem.solve(seed=7, max_refine=0)
    elapsed = time.perf_counter() - t0
    verdict = check_plan(problem, solution)
    assert verdict.ok, verdict.violations
    assert elapsed < 5.0, (
        f"the mcts phase alone took {elapsed:.1f}s -- this case is meant to be"
        " cheap without the refiner and expensive with it"
    )


SINK_178_S7_R1_FINGERPRINT = "3b0380e86791f369941d1cc8"


def test_the_refiner_under_load_stays_within_its_measured_cost(engine):
    problem = _sink_178()
    t0 = time.perf_counter()
    solution = problem.solve(seed=7, max_refine=1)
    elapsed = time.perf_counter() - t0

    verdict = check_plan(problem, solution)
    assert verdict.ok, verdict.violations
    assert plan_fingerprint(solution) == SINK_178_S7_R1_FINGERPRINT
    assert elapsed < 8.0, (
        f"one refiner iteration on sink-178/s7 took {elapsed:.1f}s against a"
        " measured 3.5s -- the per-state scratch in `scratch.rs` is the thing"
        " that makes this cheap, and something has undone it"
    )
