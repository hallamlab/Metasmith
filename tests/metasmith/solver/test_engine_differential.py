"""The differential gate, and the guard that keeps it from grading itself.

`test_engine_solve.py` asks whether the engine finds the same plan on a fixed
list of cases. This file asks the population question the port's acceptance
criterion is actually written in -- *the same plan on 100% of the generated
corpus* -- by driving `metasmith.testing.solver_differential`, and it does so
across several streams per problem, because one seed per problem exercises one
path through a search whose whole job is to choose between paths.

The size here is the size that stays in the fast gate. The full sweep lives in
`tests/perf/test_solver_differential.py` and the CLI::

    python -m metasmith.testing.solver_differential --problems 16000 --out sweep.json

The first test in this file is not about the solver at all. It is about whether
this file means anything.

**Opt-in.** Everything here runs the python solver as the engine's reference, so
it is off unless `--python-solver` is passed. The engine is what ships; a
routine run is asking whether *it* is correct, not whether a second
implementation agrees with it. This is what to reach for when there is reason to
suspect the engine.
"""

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

#: Cases whose *reference* side no longer fits the cap, settled off the clock
#: instead. T9's distance walk is flatter than the longest-path walk it replaced
#: and the search gives up on some cycle-dense `sink` instances; giving up costs
#: the full mcts budget, and the python reference pays it fifteen times over.
#: Each of these was rerun uncapped and came back `identical` -- 37.0s, 45.8s,
#: 26.6s and 29.9s of reference time against a 20s cap.
#:
#: Listed rather than absorbed by a larger cap, because the fast axis pays that
#: cap four times over and the point of the entry is that the sweep still fails
#: on an unadjudicated case nobody has looked at. Settle a new one with::
#:
#:     python -m metasmith.testing.solver_differential --problems 1 \
#:         --seeds <solve_seed> --timeout 0
SETTLED_OFF_THE_CLOCK = {
    "sink-3/s2147483647",
    "sink-7/s42",
    "sink-7/s1234",
    "sink-7/s2147483647",
}


@pytest.fixture(scope="module")
def engine():
    info = EngineFor("solve")
    if info is None:
        pytest.skip("no msm_solver advertising `solve` (./dev.sh -bel)")
    return info


def test_the_reference_side_is_not_the_engine_wearing_a_hat(engine):
    """The failure mode this whole file has is a green run.

    Both implementations are reached through one `SolverProblem.solve()`. Drop
    the `UsePythonSolver()` and every assertion in this file goes on passing,
    comparing the engine against itself, and nobody investigates a passing test.
    So the guard is asserted directly: outside the block the engine is what
    runs, inside it the Python solver is, and the sweep's reference helper
    carries that same assertion at every call site.

    Unpinned for the duration, deliberately. The claim under test is about the
    *default* -- with an engine staged and nothing said, the engine runs -- and
    a session-wide `--solver=python` would otherwise make this test report on
    the flag rather than on the code.
    """
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
    """A gate that quietly shrinks to one profile is a gate that stopped gating.

    The generated corpus earns its name from the dials -- cycles, lineage,
    duplicate transforms, product groups -- and an off-by-one in the case
    splitter could drop most of them while the totals still looked right.
    """
    got = list(cases(problems=len(SWEEP_PROFILES) * len(SOLVE_SEEDS) * 3))
    assert {c[1] for c in got} == {name for name, _ in SWEEP_PROFILES}
    assert {c[3] for c in got} == set(SOLVE_SEEDS)
    assert len(got) == len(SWEEP_PROFILES) * len(SOLVE_SEEDS) * 3
    assert len(set(c[0] for c in got)) == len(got), "case names must be unique"


def test_the_two_implementations_agree_across_the_generated_corpus(engine):
    """The gate itself, at the size the fast axis can carry.

    Every comparison checks four things in order of decreasing structure --
    completeness, topology, shape, step order -- and then asks the semantic
    checker whether the two are *wrong in the same way*, since some generated
    instances are unsound under both (the pinned refiner/`rectify` laundering)
    and parity of verdict is the claim, not soundness the Python solver has
    never had.
    """
    report = run_sweep(engine, problems=len(SWEEP_PROFILES) * len(SOLVE_SEEDS) * 16)
    assert report.total > 0
    assert report.clean, "\n".join(
        f"{c.case}: {c.outcome} -- {c.detail}" for c in report.disagreements
    )
    unsettled = [c for c in report.unadjudicated if c.case not in SETTLED_OFF_THE_CLOCK]
    assert not unsettled, (
        f"a side hit the {CASE_TIMEOUT:g}s cap, so these cases were never judged "
        "either way -- rerun each with --timeout 0 and add it to "
        "SETTLED_OFF_THE_CLOCK with its verdict, or find out why it got slower: "
        + ", ".join(f"{c.case} ({c.outcome})" for c in unsettled)
    )
    assert report.agreed == report.total - len(report.unadjudicated)


def test_a_disagreement_would_actually_be_reported(engine):
    """The sweep's own failure path, driven rather than assumed.

    A gate that reports `clean` because it never looks is the same shape of bug
    as a reference that is not a reference. Feeding it a problem whose two
    sides genuinely differ is not possible without breaking one of them, so the
    classifier is driven instead: two plans that differ get an outcome, and the
    outcome is not `identical`.
    """
    from metasmith.testing.solver_differential import compare
    from metasmith.testing.solver_verification import GeneratorDials, generate_problem

    problem = generate_problem(1, GeneratorDials(n_types=6, n_extra_transforms=3))
    # A wire version the engine refuses: the engine errors, and the sweep must
    # say so rather than counting it as agreement.
    import metasmith.testing.solver_differential as sd

    original = sd.SOLVER_WIRE_VERSION
    sd.SOLVER_WIRE_VERSION = original + 99
    try:
        outcome, detail, _, _ = compare(engine, problem)
    finally:
        sd.SOLVER_WIRE_VERSION = original
    assert outcome == "engine_error", f"got {outcome}: {detail}"
    assert detail, "an engine error with no detail is unactionable"
