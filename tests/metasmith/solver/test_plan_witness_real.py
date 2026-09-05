"""The witness held against the workflows this project ships, not generated ones.

`test_plan_witness` adjudicates eight generated problems and three stress
variants. None of them is a workflow anybody runs, and the record for this scope
says twice over that a generated corpus will endorse a rule the real templates
refuse. This file is the same two questions asked of the eleven shipped
templates: does the engine still solve each one to a plan the proved witness
accepts, and does the witness reject a broken copy of that plan *by the clause
that was broken*.

The wider corpus -- the real-library test solves, PlanBench, the fabfos
pipelines, a driver carried in from another branch -- lives in
`research/metasmith/witness_sweep/`, because a hundred-case sweep in the fast
suite is how a suite stops being run. What is here is what must never regress.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.solver_backend import Backend
from metasmith.models.solver_engine import EngineFor
from metasmith.testing.solver_verification import check_plan
from metasmith.testing.witness_sweep import UNHOSTABLE, sweep_problem, templates_corpus

MLIB = Path(__file__).resolve().parents[3] / "src" / "metasmith_libraries"

# Skipping rather than failing on an uncompiled library: `_metadata/` is a build
# product a fresh clone does not have, and a template that cannot be loaded is
# not a witness result. The same reasoning as the staged-engine skip below.
_compiled = (MLIB / "transforms" / "logistics" / "_metadata" / "index.yml").exists()
needs_library = pytest.mark.skipif(
    not _compiled, reason="the standard library has no compiled metadata (dev/libraries.sh -bm)"
)
needs_engine = pytest.mark.skipif(
    EngineFor("check") is None,
    reason="no staged msm_solver advertising the 'check' capability",
)


@pytest.fixture(scope="module")
def swept() -> list:
    return [
        sweep_problem(name, problem, expect_fingerprint=fingerprint)
        for name, problem, fingerprint in templates_corpus(MLIB)
    ]


@needs_engine
@needs_library
def test_every_shipped_template_solves_to_a_plan_the_witness_accepts(swept):
    assert Backend("solve") == "rust"
    assert len(swept) >= 11, f"only {len(swept)} templates were reached"
    refused = [f"{r.name}: {r.verdict} {r.clauses or r.error}" for r in swept if r.verdict != "accepted"]
    assert not refused, "\n".join(refused)


@needs_engine
@needs_library
def test_the_reference_and_the_engine_agree_on_every_shipped_template(swept):
    """The python statement of the specification, on the same bytes.

    Agreement on an accepted plan is weak evidence on its own; agreement on the
    decoys below is what would make a divergence between the two visible.
    """
    disagree = [r.name for r in swept if r.reference_agrees is False]
    assert not disagree, disagree
    assert not [d for r in swept for d in r.decoy_disagreements]


@needs_engine
@needs_library
def test_each_clause_rejects_a_broken_copy_of_a_real_plan(swept):
    """A decoy caught by the wrong clause proves nothing about this one."""
    missed = [
        f"{r.name}/{clause}={outcome}"
        for r in swept for clause, outcome in r.decoys.items()
        if outcome not in (UNHOSTABLE, "caught")
    ]
    assert not missed, missed
    # A clause no real template can host has never been shown to reject
    # anything *here*; `test_spec_fixtures` is what covers those, and this
    # asserts the set does not silently grow.
    unhostable = {
        clause for r in swept for clause, outcome in r.decoys.items()
        if outcome == UNHOSTABLE and sum(
            1 for x in swept if x.decoys.get(clause) == "caught"
        ) == 0
    }
    assert not unhostable, f"no shipped template can host these decoys: {sorted(unhostable)}"


@needs_engine
@needs_library
def test_no_shipped_template_produces_two_endpoints_with_one_signature():
    """`check_plan(strict=True)`, which is the bar a refiner change must clear.

    `rectify` keys its endpoint map by signature, so two steps emitting
    signature-equal endpoints are merged onto one producer and every consumer is
    rewired to whichever came last in `get_order`. That is a plan the search
    never chose, and the wire the witness reads is already collapsed, so
    `uniqueProducer` cannot see it. `strict` is the only place it is visible.

    The eleven templates pass today. This exists so that a change which starts
    minting endpoints under a swap cannot land silently.
    """
    refused = []
    for name, problem, _fingerprint in templates_corpus(MLIB):
        verdict = check_plan(problem, problem.solve(), strict=True)
        if not verdict.ok:
            refused.append(f"{name}: {verdict.violations[0]}")
    assert not refused, "\n".join(refused)
