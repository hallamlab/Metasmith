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
    bp = build_linear_plan(tmp_path, n_steps=2)
    assert len(bp.data_library.manifest) == 1, (
        f"E2 expects a singleton input, got {len(bp.data_library.manifest)} entries"
    )
    task, lib = run_and_load(virtual_runtime, bp)
    events = lib._trace.events
    assert len(events) == 2, f"expected 2 events for 2-step chain, got {len(events)}"
    terminal_fid = events[-1].produces[0].file_instance_id
    walked = list(lib.walk_ancestors(terminal_fid))
    assert len(walked) >= 1, "single-input chain should yield at least 1 ancestor"


@pytest.mark.timeout(60)
def test_e3_large_fanout_batches_correctly(tmp_path, virtual_runtime):
    bp = build_batched_plan(tmp_path, n_inputs=50, batch_size=10)
    task, lib = run_and_load(virtual_runtime, bp)
    assert len(lib._trace.events) == 5, (
        f"expected 5 batched invocations, got {len(lib._trace.events)}"
    )


@pytest.mark.xfail(
    reason="the 5s budget is missed by 2-3x: 50 inputs / 5 batched invocations "
           "measure 9-14s under the virtual runtime. The batching itself is "
           "correct (pinned above); what is unbudgeted is the per-invocation "
           "cost of the virtual runtime. Re-tighten or retire this budget once "
           "that cost is measured -- and per tests/metasmith/AGENTS.md a claim "
           "about cost belongs in the perf axis, not here.",
    strict=False,
)
@pytest.mark.timeout(60)
def test_e3_large_fanout_under_5s(tmp_path, virtual_runtime):
    bp = build_batched_plan(tmp_path, n_inputs=50, batch_size=10)
    t0 = time.perf_counter()
    run_and_load(virtual_runtime, bp)
    elapsed = time.perf_counter() - t0
    assert elapsed < 5.0, f"large-batch run took {elapsed:.2f}s (>5s budget)"


def test_e4_dead_output_no_hang(tmp_path):
    pytest.xfail(
        reason="needs a multi-slot mock with an unconsumed slot"
    )
    bp = build_dead_output_plan(tmp_path)
    assert bp.plan is not None


def test_g4_empty_input_to_group(tmp_path):
    with pytest.raises(AssertionError, match="nothing given"):
        build_batched_plan(tmp_path, n_inputs=0, batch_size=3)
