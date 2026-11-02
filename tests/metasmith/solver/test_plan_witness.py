"""The engine's own verdict on the plans it returns, and on plans built to be wrong.

`solver_witness` is the trusted computing base -- the crate the Lean proof is
about -- so this suite adjudicates the engine's answer rather than comparing it
to a second solver's.

`solver_spec.check_spec` appears here as an independent second opinion written in
another language from the same specification. Its *disagreements* with the engine
are the interesting output: they are the only thing that can catch a clause the
port states differently from the reference. It is never the thing under test, and
it must not be edited to agree.

The decoys carry most of the value, and they are shared with `test_plan_spec` so
both sides adjudicate the same mutated plans. A clause with no decoy has never
been shown to reject anything.
"""

from __future__ import annotations

import pytest

from metasmith.models.solver_backend import Backend
from metasmith.models.solver_engine import EngineFor
from metasmith.testing.solver_bench import CORPUS, STRESS_CORPUS
from metasmith.testing.solver_spec import CLAUSES, check_spec
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
    from metasmith.testing.solver_verification import generate_problem

    _, _, verdict = solve_and_check(generate_problem(seed, dials, name=name))
    # `complete` and `ok` are different questions. A search whose frontier ran
    # out returns a plan with no target step, and refusing that would be refusing
    # an honest "no answer".
    if verdict.complete:
        assert verdict.ok, f"{name} rejected on {verdict.clauses}"


@needs_engine
@pytest.mark.parametrize("name,seed,dials", ALL_CASES, ids=[c[0] for c in ALL_CASES])
def test_the_two_statements_of_the_specification_agree(name, seed, dials):
    """The rust witness and the python reference, on the plan and on every decoy.

    Both are written from `docs/metasmith/solver-spec.md`, in different languages,
    by different code paths. Agreement on an accepted plan is weak evidence;
    agreement on ten deliberately broken ones is what makes a divergence between
    the two visible at all.
    """
    assert Backend("solve") == "rust"
    from metasmith.testing.solver_verification import generate_problem

    request, reply, engine = solve_and_check(generate_problem(seed, dials, name=name))
    if not engine.complete:
        pytest.skip("incomplete plan; soundness is not the question")
    reference = check_spec(request, reply)
    assert engine.ok == reference.ok, (
        f"{name}: engine says ok={engine.ok} on {engine.clauses}, "
        f"reference says ok={reference.ok} on {reference.violations}"
    )
    if not engine.ok:
        return
    for clause in sorted(DECOYS):
        bad = apply_decoy(clause, request, reply)
        if bad is None:
            continue
        got_engine = witness_check(*bad)
        got_reference = check_spec(*bad)
        assert got_engine.ok == got_reference.ok, (
            f"{name}/{clause}: engine {got_engine.clauses} vs "
            f"reference {got_reference.violations}"
        )
        assert got_engine.violated(clause) == got_reference.violated(clause), (
            f"{name}/{clause}: the two disagree about which clause fired -- "
            f"engine {got_engine.clauses} vs reference {got_reference.violations}"
        )


@needs_engine
@pytest.mark.parametrize("clause", sorted(DECOYS))
def test_each_clause_of_the_engines_witness_rejects_a_plan_built_to_break_it(clause):
    """A decoy caught by the wrong clause proves nothing about this one."""
    assert Backend("solve") == "rust"
    from metasmith.testing.solver_verification import generate_problem

    hosted = 0
    for name, seed, dials in ALL_CASES:
        request, reply, verdict = solve_and_check(generate_problem(seed, dials, name=name))
        if not (verdict.ok and verdict.complete):
            continue
        # Derived from a reply the witness has just accepted, so a rejection
        # cannot be blamed on the decoy merely being malformed.
        bad = apply_decoy(clause, request, reply)
        if bad is None:
            continue
        hosted += 1
        got = witness_check(*bad)
        assert got.violated(clause), (
            f"{clause} decoy on {name} was not caught by {clause}; "
            f"got {got.clauses or 'nothing at all'}"
        )
    assert hosted, f"no corpus case can host a {clause} decoy, so it is untested"


@needs_engine
def test_the_clause_names_are_one_vocabulary():
    """The engine names its clauses; the reference names them; they must match.

    Nothing else forces this. The two are separate implementations and a rename
    on one side would silently turn every per-clause assertion above into a
    tautology about a clause that never fires.
    """
    from metasmith.testing.solver_verification import generate_problem

    name, seed, dials = ALL_CASES[0]
    request, reply, _ = solve_and_check(generate_problem(seed, dials, name=name))
    seen: set[str] = set()
    for clause in sorted(DECOYS):
        bad = apply_decoy(clause, request, reply)
        if bad is None:
            continue
        seen.update(witness_check(*bad).clauses)
    assert seen, "no decoy produced a verdict, so nothing was compared"
    assert seen <= set(CLAUSES), f"the engine names clauses the reference does not: {seen - set(CLAUSES)}"
