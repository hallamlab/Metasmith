"""The refiner's cost is a function of the plan the search hands it.

This file used to pin one instance that took 53s and roughly 70 GB at a full
refiner budget, and 3.5s for a single iteration. It is 4 steps and unmeasurable
now, and so is every other instance that was expensive: the blow-up was never
intrinsic to the refiner, it was the weighted rule settling on a 57-step plan and
the refiner paying candidate-count times plan-length for it.

What is worth protecting is therefore the cause rather than the symptom. A
selection change that goes back to handing the refiner long plans will show up
here as a step count, long before anyone waits out the wall clock.
"""
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


#: The instances that were expensive under the weighted rule, with the plan
#: length each settles on now. `sink-6807` is the one the profiling report was
#: written about: 57 steps, 33,062 candidates an iteration, ~70 GB at budget 256.
FORMERLY_EXPENSIVE = [
    ("sink-6807", 6807, 4),
    ("sink-178", 178, 7),
    ("sink-9375", 9375, 9),
    ("sink-24", 24, 7),
]


@pytest.mark.parametrize(
    "name,seed,steps", FORMERLY_EXPENSIVE, ids=[c[0] for c in FORMERLY_EXPENSIVE]
)
def test_the_search_no_longer_hands_the_refiner_a_long_plan(engine, name, seed, steps):
    problem = generate_problem(seed, dict(SWEEP_PROFILES)["sink"], name="sink")
    solution = problem.solve(seed=7, max_refine=0)
    assert solution.complete, name
    assert len(solution.dependency_plan) == steps, (
        f"{name} settles on {len(solution.dependency_plan)} steps, not {steps}."
        " The refiner's cost is candidate count times plan length, so a longer"
        " plan here is what a refiner blow-up looks like before it is slow."
    )


SINK_178_S7_FINGERPRINT = "1d9079ba63569d3e1231a254"


def test_a_full_refiner_budget_on_the_worst_of_them_is_cheap(engine):
    problem = generate_problem(178, dict(SWEEP_PROFILES)["sink"], name="sink")
    t0 = time.perf_counter()
    solution = problem.solve(seed=7, max_refine=256)
    elapsed = time.perf_counter() - t0

    verdict = check_plan(problem, solution)
    assert verdict.ok, verdict.violations
    assert plan_fingerprint(solution) == SINK_178_S7_FINGERPRINT
    # Two orders of magnitude of headroom against a case that used to cost 3.5s
    # for ONE iteration. A failure here is a regression of a different kind than
    # a moved fingerprint, so keep both assertions.
    assert elapsed < 2.0, (
        f"a full refiner budget on sink-178/s7 took {elapsed:.1f}s -- either the"
        " search handed it a longer plan or the per-state scratch in `scratch.rs`"
        " has been undone"
    )
