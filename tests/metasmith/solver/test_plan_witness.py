"""The engine's own verdict on the plans it returns, and on plans built to be wrong.

On the default path, adjudicating the engine's answer rather than comparing it
to a second solver's. `check_plan` appears here only as an independent second
opinion whose *disagreements* are the interesting output -- it is never the
thing under test, and it must not be edited to agree.

The decoys carry most of the value. Five of the witness's conditions are
unfalsifiable on real solves: no shipped plan uses a product slot declaring
lineage, no decoded payload names the same slot twice, and the reflexive escape
in the lineage test fires on nothing. A clause with no decoy has never been
shown to reject anything.
"""

from __future__ import annotations

import pytest

from metasmith.models.solver_backend import Backend
from metasmith.models.solver_engine import EngineFor
from metasmith.testing.solver_bench import CORPUS, STRESS_CORPUS
from metasmith.testing.solver_verification import check_plan, generate_problem
from metasmith.testing.witness_check import (
    DECOYS,
    apply_decoy,
    solve_and_check,
    witness_check,
)

# Skipping rather than failing when nothing is staged: a witness that is not
# there cannot be wrong. Passing silently on the *python* solver is the failure
# mode worth guarding, and the backend assertion inside each test does that.
_staged = EngineFor("check") is not None
needs_engine = pytest.mark.skipif(
    not _staged, reason="no staged msm_solver advertising the 'check' capability"
)

ALL_CASES = [(f"{n}", s, d) for n, s, d in CORPUS + STRESS_CORPUS]


@needs_engine
def test_the_engine_is_the_thing_being_tested():
    assert Backend("solve") == "rust"


@needs_engine
@pytest.mark.parametrize("name,seed,dials", ALL_CASES, ids=[c[0] for c in ALL_CASES])
def test_the_witness_accepts_what_the_engine_returns(name, seed, dials):
    assert Backend("solve") == "rust"
    problem = generate_problem(seed, dials, name=name)
    _, _, verdict = solve_and_check(problem)
    # `complete` and `ok` are different questions. A search whose frontier ran
    # out returns a plan with no target step, and refusing that would be
    # refusing an honest "no answer".
    if verdict.complete:
        assert verdict.ok, f"{name} rejected on {verdict.clauses}"


@needs_engine
@pytest.mark.parametrize("name,seed,dials", ALL_CASES, ids=[c[0] for c in ALL_CASES])
def test_the_witness_agrees_with_the_independent_checker(name, seed, dials):
    assert Backend("solve") == "rust"
    problem = generate_problem(seed, dials, name=name)
    _, _, verdict = solve_and_check(problem)
    if not verdict.complete:
        pytest.skip("incomplete plan; soundness is not the question")
    assert verdict.ok == check_plan(problem, problem.solve()).ok


@needs_engine
@pytest.mark.parametrize("clause", sorted(DECOYS))
def test_each_clause_rejects_a_plan_built_to_break_it(clause):
    """A decoy caught by the wrong clause proves nothing about this one."""
    assert Backend("solve") == "rust"
    hosted = 0
    for name, seed, dials in ALL_CASES:
        problem = generate_problem(seed, dials, name=name)
        request, reply, verdict = solve_and_check(problem)
        if not (verdict.ok and verdict.complete):
            continue
        # Derived from a reply the witness has just accepted, so a rejection
        # cannot be blamed on the decoy merely being malformed.
        bad = apply_decoy(clause, request, reply)
        if bad is None:
            continue
        hosted += 1
        got = witness_check(request, bad)
        assert got.violated(clause), (
            f"{clause} decoy on {name} was not caught by {clause}; "
            f"got {got.clauses or 'nothing at all'}"
        )
    assert hosted, f"no corpus case can host a {clause} decoy, so it is untested"
