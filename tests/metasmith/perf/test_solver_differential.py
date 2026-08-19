from __future__ import annotations

import pytest

pytestmark = pytest.mark.python_solver

from metasmith.models.solver_backend import RustSolver, _set_solver_class
from metasmith.models.solver_engine import EngineFor
from metasmith.testing.solver_differential import (
    SOLVE_SEEDS,
    SWEEP_PROFILES,
    run_sweep,
)


@pytest.fixture(scope="module")
def engine():
    info = EngineFor("solve")
    if info is None:
        pytest.skip("no msm_solver advertising `solve` (./dev.sh -bel)")
    previous = _set_solver_class(RustSolver)
    yield info
    _set_solver_class(previous)


def test_the_two_implementations_agree_at_corpus_scale(engine):
    report = run_sweep(engine, problems=len(SWEEP_PROFILES) * len(SOLVE_SEEDS) * 64)
    assert report.clean, "\n".join(
        f"{c.case}: {c.outcome} -- {c.detail}" for c in report.disagreements
    )
    assert len(report.unadjudicated) <= report.total // 100, (
        f"{len(report.unadjudicated)}/{report.total} cases hit the cap: "
        + ", ".join(f"{c.case} ({c.outcome})" for c in report.unadjudicated[:20])
    )
    assert report.agreed + len(report.unadjudicated) == report.total


def test_the_one_instance_neither_implementation_can_finish(engine):
    from metasmith.testing.solver_differential import compare
    from metasmith.testing.solver_verification import generate_problem

    dials = dict(SWEEP_PROFILES)["sink"]
    problem = generate_problem(178, dials, name="sink")
    outcome, detail, engine_s, _ = compare(engine, problem, seed=7, max_refine=0, timeout=120)
    assert outcome == "identical", f"sink-178/s7 at max_refine=0: {outcome} -- {detail}"
    assert engine_s < 5.0, (
        f"the mcts phase alone took {engine_s:.1f}s -- this case is meant to be"
        " cheap without the refiner and expensive with it"
    )


SINK_178_S7_R1_FINGERPRINT = "3b0380e86791f369941d1cc8"


def test_the_refiner_under_load_stays_within_its_measured_cost(engine):
    import time

    from metasmith.testing.solver_verification import (
        check_plan,
        generate_problem,
        plan_fingerprint,
    )

    dials = dict(SWEEP_PROFILES)["sink"]
    problem = generate_problem(178, dials, name="sink")
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
