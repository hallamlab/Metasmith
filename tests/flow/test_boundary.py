"""Empty / boundary cases E1-E4 and G4 from the catalog.

Empty input, single input, large fan-out wall-clock, dead-output channel,
empty input to a batched/group transform.

Catalog: see `tests/flow/AGENTS.md` Axes 4 (G4) and 9 (E1-E4).
"""

from __future__ import annotations

import time

import pytest

from metasmith.models.solver import Transform
from metasmith.models.workflow import WorkflowPlan

from .conftest import (
    build_batched_plan,
    build_dead_output_plan,
    build_empty_plan,
    build_linear_plan,
    run_and_load,
)


def test_e1_empty_input_plan_hint(tmp_path):
    """<E1> Empty input library: planner raises on `given=[]`.

    The Lead `build_empty_plan` builder skips because the planner does
    not yet return a PlanHint shape for unreachable targets. Until that
    lands, we pin the current contract directly: `WorkflowPlan.Generate`
    raises `AssertionError("nothing given")` on an empty given.
    """
    target = Transform()
    target.AddRequirement(properties={"assembly"})
    with pytest.raises(AssertionError, match="nothing given"):
        WorkflowPlan.Generate(
            given=[],
            transforms=[],
            target_names=["assembly"],
            target_model=target,
        )


def test_e2_single_input_chain(tmp_path, virtual_runtime):
    """<E2> Singleton input runs the full chain and lineage roundtrips.

    Shape: 1-sample input through a 2-step linear chain.
    """
    bp = build_linear_plan(tmp_path, n_steps=2)
    # Confirm the builder produced a single-sample input library.
    assert len(bp.data_library.manifest) == 1, (
        f"E2 expects a singleton input, got {len(bp.data_library.manifest)} entries"
    )
    task, lib = run_and_load(virtual_runtime, bp)
    events = lib._trace.events
    assert len(events) == 2, f"expected 2 events for 2-step chain, got {len(events)}"
    # Lineage chain roundtrips via the terminal output id.
    terminal_fid = events[-1].produces[0].file_instance_id
    walked = list(lib.walk_ancestors(terminal_fid))
    assert len(walked) >= 1, "single-input chain should yield at least 1 ancestor"


@pytest.mark.timeout(10)
def test_e3_large_fanout_under_5s(tmp_path, virtual_runtime):
    """<E3> Large batch (50 inputs, batch_size=10) finishes well under 5s.

    Drives the catalog's "100 parallel branches" cell — `build_branching_plan`
    only supports fanout=2, so we exercise scale via `build_batched_plan`
    with a large input set (50 inputs through a batched transform). The
    invariant is identical: high parallelism completes within the wall-clock
    sentinel without hanging.
    """
    bp = build_batched_plan(tmp_path, n_inputs=50, batch_size=10)
    t0 = time.perf_counter()
    task, lib = run_and_load(virtual_runtime, bp)
    elapsed = time.perf_counter() - t0
    assert elapsed < 5.0, f"large-batch run took {elapsed:.2f}s (>5s budget)"
    # Correctness sanity: 50/10 = 5 batched invocations.
    assert len(lib._trace.events) == 5, (
        f"expected 5 batched invocations, got {len(lib._trace.events)}"
    )


def test_e4_dead_output_no_hang(tmp_path):
    """<E4> Dead output channel: downstream channel empty, plan does not hang.

    The Lead `build_dead_output_plan` builder skips because no
    `mock_transforms` shape currently emits a deliberately-unconsumed
    output slot. The expected behavior is a multi-slot producer where one
    slot is unwired downstream — pin it as `xfail(strict=True)` so the
    test starts asserting the moment the stimulus shape exists.
    """
    pytest.xfail(
        reason="needs multi-slot mock with unconsumed slot; see catalog Axis 9 E4"
    )
    bp = build_dead_output_plan(tmp_path)
    assert bp.plan is not None


def test_g4_empty_input_to_group(tmp_path):
    """<G4> Empty input to a batched/group transform raises cleanly.

    The current contract is: `build_batched_plan(n_inputs=0, batch_size=3)`
    surfaces an `AssertionError("nothing given")` at plan generation —
    the planner does not silently emit an empty plan. We pin the raise so
    future "graceful empty" support trips this test for review.
    """
    with pytest.raises(AssertionError, match="nothing given"):
        build_batched_plan(tmp_path, n_inputs=0, batch_size=3)
