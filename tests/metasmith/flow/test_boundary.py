from __future__ import annotations

from metasmith.models.workflow import Case

import pytest

from metasmith.models.solver import Transform
from metasmith.models.workflow import WorkflowPlan

from .conftest import (
    build_batched_plan,
    build_dead_output_plan,
    run_and_load,
)


def test_e1_empty_input_plan_hint(tmp_path):
    target = Transform()
    target.AddRequirement(properties={"assembly"})
    with pytest.raises(AssertionError, match="nothing given"):
        WorkflowPlan.Generate(
            cases=Case.ByShape([], target=target, target_names=["assembly"]),
            transforms=[],
        )


@pytest.mark.timeout(60)
def test_e3_large_fanout_batches_correctly(tmp_path, virtual_runtime):
    bp = build_batched_plan(tmp_path, n_inputs=50, batch_size=10)
    task, lib = run_and_load(virtual_runtime, bp)
    calls = [e for e in virtual_runtime.parse_trace() if e.get("type") == "bootstrap_call"]
    assert len(calls) == 5, f"expected 5 batched tasks, got {len(calls)}"
    assert len(lib._trace.events) == 50, (
        f"expected one event per member, 50, got {len(lib._trace.events)}"
    )


def test_e4_dead_output_no_hang(tmp_path):
    pytest.xfail(
        reason="needs a multi-slot mock with an unconsumed slot"
    )
    bp = build_dead_output_plan(tmp_path)
    assert bp.plan is not None


def test_g4_empty_input_to_group(tmp_path):
    with pytest.raises(AssertionError, match="nothing given"):
        build_batched_plan(tmp_path, n_inputs=0, batch_size=3)
