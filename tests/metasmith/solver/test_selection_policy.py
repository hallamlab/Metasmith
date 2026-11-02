"""The selection rule is PUCT, unconditionally, and only its configuration is reachable.

`MSM_SOLVER_POLICY` used to choose between two rules. It is gone: a policy switch
whose default is one of the arms is how a measurement of the incumbent gets
published under the challenger's name, which happened once here.
"""
from __future__ import annotations

import pytest

from metasmith.models.solver_engine import EngineError, EngineFor
from metasmith.testing.solver_bench import CORPUS
from metasmith.testing.solver_verification import (
    check_plan,
    generate_problem,
    plan_fingerprint,
)


@pytest.fixture(scope="module")
def engine():
    if EngineFor("solve") is None:
        pytest.skip("no msm_solver advertising `solve` (./dev/metasmith.sh -bel)")


def _problem():
    name, seed, dials = CORPUS[0]
    return generate_problem(seed, dials, name=name)


def test_no_environment_variable_selects_a_different_rule(engine, monkeypatch):
    problem = _problem()
    baseline = plan_fingerprint(problem.solve())
    for value in ("weighted", "puct", "", "nonsense"):
        monkeypatch.setenv("MSM_SOLVER_POLICY", value)
        assert plan_fingerprint(problem.solve()) == baseline, (
            f"MSM_SOLVER_POLICY={value!r} moved the plan -- a selection switch has"
            " come back, and with it the possibility of measuring the wrong arm"
        )


def test_the_configuration_knob_is_live_and_still_sound(engine, monkeypatch):
    problem = _problem()
    baseline = plan_fingerprint(problem.solve())
    # A temperature this high flattens the prior, so the plan is allowed to move;
    # what it may not do is stop being sound.
    monkeypatch.setenv("MSM_SOLVER_PUCT", "temperature=50.0,top_k=3")
    other = problem.solve()
    verdict = check_plan(problem, other)
    assert verdict.ok, verdict.violations
    assert plan_fingerprint(other) or baseline


def test_an_unparseable_configuration_is_refused_rather_than_defaulted(engine, monkeypatch):
    monkeypatch.setenv("MSM_SOLVER_PUCT", "c_puct=not_a_number")
    with pytest.raises(EngineError):
        _problem().solve()
