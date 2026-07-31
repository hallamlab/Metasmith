"""The differential gate at a size the fast axis cannot carry.

`tests/solver/test_engine_differential.py` runs the same sweep at 512
comparisons, which is enough to catch a port that broke and not enough to
support the claim the port was written to make. This runs it wide, and it also
runs the part of the corpus where the two implementations get *expensive* --
the problem seeds a small prefix never reaches.

Wider still, and off the clock entirely::

    python -m metasmith.testing.solver_differential --problems 16000 --out sweep.json

A case the sweep reports as unadjudicated is settled by rerunning it with
``--timeout 0``; the report prints the flags. That matters because the cap is
not a verdict, and a sweep that quietly counted capped cases as agreement would
report 100% on a corpus it never finished reading.
"""

from __future__ import annotations

import pytest

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
    return info


def test_the_two_implementations_agree_at_corpus_scale(engine):
    report = run_sweep(engine, problems=len(SWEEP_PROFILES) * len(SOLVE_SEEDS) * 64)
    assert report.clean, "\n".join(
        f"{c.case}: {c.outcome} -- {c.detail}" for c in report.disagreements
    )
    # Not an assertion that there are none -- at this width the corpus reaches
    # instances whose plan blows up under one stream and not another, and those
    # are expensive for *both* sides. The claim is that they are few enough to
    # name, since each one is a case the gate did not actually adjudicate.
    assert len(report.unadjudicated) <= report.total // 100, (
        f"{len(report.unadjudicated)}/{report.total} cases hit the cap: "
        + ", ".join(f"{c.case} ({c.outcome})" for c in report.unadjudicated[:20])
    )
    assert report.agreed + len(report.unadjudicated) == report.total


def test_the_one_instance_neither_implementation_can_finish(engine):
    """`sink-178` under seed 7, pinned at a refiner budget that terminates.

    The only comparison in 16,000 that the sweep could not settle at the default
    `max_refine=256`, because a single refiner iteration on this instance costs
    the engine 22 seconds and the reference around 380. It is not slow, it is
    unfinishable, and equally so on both sides.

    Keeping it here as a named case rather than a paragraph in the plan is the
    difference between a known limit and folklore: `max_refine=0` runs the mcts
    phase alone, which is the part that chooses the plan, and the two must still
    land on the same one.
    """
    from metasmith.testing.solver_differential import compare
    from metasmith.testing.solver_verification import generate_problem

    dials = dict(SWEEP_PROFILES)["sink"]
    problem = generate_problem(178, dials, name="sink")
    outcome, detail, engine_s, _ = compare(engine, problem, seed=7, max_refine=0, timeout=120)
    assert outcome == "identical", f"sink-178/s7 at max_refine=0: {outcome} -- {detail}"
    # If this ever stops being expensive, the case has lost its point and the
    # plan's claim about the refiner's cost curve needs re-measuring.
    assert engine_s < 5.0, (
        f"the mcts phase alone took {engine_s:.1f}s -- this case is meant to be"
        " cheap without the refiner and expensive with it"
    )
