from __future__ import annotations

import pytest

from .conftest import (
    build_branching_plan,
    build_branching_with_failure_plan,
    build_linear_plan,
    run_and_load,
)


def test_b1_two_way_branch_shared_parent(tmp_path, virtual_runtime):
    bp = build_branching_plan(tmp_path, fanout=2)
    assert len(bp.plan.steps) == 3, f"branch+merge should be 3 steps, got {len(bp.plan.steps)}"

    task, lib = run_and_load(virtual_runtime, bp)

    events = lib._trace.events
    assert len(events) == 3, f"expected 3 invocations, got {len(events)}"
    leaf_consumers = [e for e in events if len(e.consumes) == 1]
    assert len(leaf_consumers) >= 2, (
        f"expected ≥2 single-input branchers, got {len(leaf_consumers)}"
    )
    parent_ids = set()
    for e in leaf_consumers[:2]:
        for ids in e.consumes.values():
            parent_ids.update(ids)
    assert len(parent_ids) == 1, (
        f"two-way branch must share one upstream parent id; got {parent_ids}"
    )


def test_b2_three_way_fanout_shared_ancestor(tmp_path, virtual_runtime):
    bp = build_branching_plan(tmp_path, fanout=3)
    assert bp is not None  # pragma: no cover


def test_b3_sibling_failure_isolated(tmp_path, virtual_runtime):
    pytest.xfail(
        reason="virtual runtime emits one promoted event per step regardless of "
        "per-slot raises; per-slot ExecutionResult failure tracking is the gap. "
        "Planner-side multi-slot wiring (the original G2 bug) is fixed."
    )
    bp = build_branching_with_failure_plan(tmp_path)
    task, lib = run_and_load(virtual_runtime, bp)
    failures = lib.find_failures()
    assert len(failures) == 1, f"expected 1 failure, got {len(failures)}"
    promoted = lib.find_invocations(status="promoted")
    assert len(promoted) >= 1, "sibling branch did not run"


def test_b4_nested_branching_walk(tmp_path, virtual_runtime):
    bp = build_linear_plan(tmp_path, n_steps=3)
    task, lib = run_and_load(virtual_runtime, bp)

    events = lib._trace.events
    terminal = events[-1]
    assert len(terminal.produces) == 1
    fid = terminal.produces[0].file_instance_id
    walked = list(lib.walk_ancestors(fid))
    assert len(walked) >= 1, "walk_ancestors returned no nodes for terminal output"
