from __future__ import annotations

import pytest

pytestmark = pytest.mark.python_solver

from metasmith.models.solver_backend import Backend, UsePythonSolver, _set_solver_class
from metasmith.models.solver_engine import EngineFor
from metasmith.testing.solver_differential import (
    CASE_TIMEOUT,
    SOLVE_SEEDS,
    SWEEP_PROFILES,
    cases,
    run_sweep,
)

#: Profiles whose PYTHON side sits at the `CASE_TIMEOUT` boundary, so which case
#: crosses it depends on machine load rather than on anything about the code.
#:
#: `sink` is the one profile that reaches the iteration cap, and naming its cases
#: one at a time did not converge -- a different stream crossed on each run while
#: the whole sweep, rerun with `timeout=0`, agreed 512 of 512 in the same 244s
#: before and after the change under suspicion. So the profile is settled off the
#: clock as a whole, with the uncapped run as the evidence.
#:
#: Only the REFERENCE side is exempted. An engine that blew the cap is a finding
#: about the shipped solver and still fails.
SETTLED_OFF_THE_CLOCK_PROFILES = {"sink"}

#: Individual cases settled the same way, for profiles that are otherwise fast.
SETTLED_OFF_THE_CLOCK: set[str] = set()


def _settled(c) -> bool:
    if c.case in SETTLED_OFF_THE_CLOCK:
        return True
    return c.profile in SETTLED_OFF_THE_CLOCK_PROFILES and c.outcome == "reference_timeout"


@pytest.fixture(scope="module")
def engine():
    info = EngineFor("solve")
    if info is None:
        pytest.skip("no msm_solver advertising `solve` (./dev.sh -bel)")
    return info


def test_the_reference_side_is_not_the_engine_wearing_a_hat(engine):
    previous = _set_solver_class(None)
    try:
        assert Backend("solve") == "rust", (
            "this test is vacuous without an engine -- the fixture should have skipped"
        )
        with UsePythonSolver():
            assert Backend("solve") == "python"
        assert Backend("solve") == "rust", "UsePythonSolver leaked past its block"
    finally:
        _set_solver_class(previous)


def test_the_sweep_visits_every_profile_and_every_stream():
    got = list(cases(problems=len(SWEEP_PROFILES) * len(SOLVE_SEEDS) * 3))
    assert {c[1] for c in got} == {name for name, _ in SWEEP_PROFILES}
    assert {c[3] for c in got} == set(SOLVE_SEEDS)
    assert len(got) == len(SWEEP_PROFILES) * len(SOLVE_SEEDS) * 3
    assert len(set(c[0] for c in got)) == len(got), "case names must be unique"


def test_the_two_implementations_agree_across_the_generated_corpus(engine):
    report = run_sweep(engine, problems=len(SWEEP_PROFILES) * len(SOLVE_SEEDS) * 16)
    assert report.total > 0
    assert report.clean, "\n".join(
        f"{c.case}: {c.outcome} -- {c.detail}" for c in report.disagreements
    )
    unsettled = [c for c in report.unadjudicated if not _settled(c)]
    assert not unsettled, (
        f"a side hit the {CASE_TIMEOUT:g}s cap, so these cases were never judged "
        "either way -- rerun each with --timeout 0 and add it to "
        "SETTLED_OFF_THE_CLOCK with its verdict, or find out why it got slower: "
        + ", ".join(f"{c.case} ({c.outcome})" for c in unsettled)
    )
    assert report.agreed == report.total - len(report.unadjudicated)


def test_a_disagreement_would_actually_be_reported(engine):
    from metasmith.testing.solver_differential import compare
    from metasmith.testing.solver_verification import GeneratorDials, generate_problem

    problem = generate_problem(1, GeneratorDials(n_types=6, n_extra_transforms=3))
    import metasmith.testing.solver_differential as sd

    original = sd.SOLVER_WIRE_VERSION
    sd.SOLVER_WIRE_VERSION = original + 99
    try:
        outcome, detail, _, _ = compare(engine, problem)
    finally:
        sd.SOLVER_WIRE_VERSION = original
    assert outcome == "engine_error", f"got {outcome}: {detail}"
    assert detail, "an engine error with no detail is unactionable"
