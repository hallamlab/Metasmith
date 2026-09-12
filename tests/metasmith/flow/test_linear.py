from __future__ import annotations

from .conftest import (
    build_linear_plan,
    build_multi_input_plan,
    run_and_load,
)


def test_l1_single_step_round_trip(tmp_path, virtual_runtime):
    bp = build_linear_plan(tmp_path, n_steps=1)
    assert len(bp.plan.steps) == 1, f"expected 1 step, got {len(bp.plan.steps)}"

    task, lib = run_and_load(virtual_runtime, bp)

    events = lib._trace.events
    assert len(events) == 1, f"expected 1 invocation, got {len(events)}"
    ev = events[0]
    assert ev.status == "promoted", f"step should run (not hit), got {ev.status}"
    assert len(ev.consumes) == 1, f"single-step has one consume slot, got {ev.consumes!r}"
    assert len(ev.produces) == 1, f"identity transform produces 1 slot, got {len(ev.produces)}"

    fid = ev.produces[0].file_instance_id
    ancestors = list(lib.walk_ancestors(fid))
    assert len(ancestors) == 1, (
        f"expected 1 ancestor for single-step output, got {len(ancestors)}"
    )


def test_l2_two_step_preserves_id(tmp_path, virtual_runtime):
    bp = build_linear_plan(tmp_path, n_steps=2)
    assert len(bp.plan.steps) == 2

    task, lib = run_and_load(virtual_runtime, bp)

    events = lib._trace.events
    assert len(events) == 2, f"expected 2 invocations, got {len(events)}"
    orders = [e.step_order for e in events]
    assert orders == sorted(orders), f"events out of topological order: {orders}"
    tks = {e.transform_key for e in events}
    assert len(tks) == 2, f"expected 2 distinct transforms, got {tks}"
    for e in events:
        assert e.status == "promoted", f"step {e.step_order} status={e.status}"


def test_l3_three_step_lineage_chain(tmp_path, virtual_runtime):
    bp = build_linear_plan(tmp_path, n_steps=3)
    assert len(bp.plan.steps) == 3

    task, lib = run_and_load(virtual_runtime, bp)

    events = lib._trace.events
    assert len(events) == 3, f"expected 3 invocations, got {len(events)}"
    orders = [e.step_order for e in events]
    assert orders == sorted(orders), f"events out of topological order: {orders}"
    assert len({e.transform_key for e in events}) == 3, (
        "3-step chain must have 3 distinct transforms"
    )
    for e in events:
        assert len(e.produces) == 1
        assert len(e.consumes) == 1


def test_l4_multi_input_broadcast(tmp_path, virtual_runtime):
    bp = build_multi_input_plan(tmp_path, slots=2)
    assert len(bp.plan.steps) == 1

    task, lib = run_and_load(virtual_runtime, bp)

    events = lib._trace.events
    assert len(events) == 1, f"expected 1 invocation, got {len(events)}"
    ev = events[0]
    assert len(ev.consumes) == 2, (
        f"multi-input transform should consume 2 slots, got {ev.consumes!r}"
    )
    for slot_key, parent_ids in ev.consumes.items():
        assert len(parent_ids) >= 1, (
            f"slot {slot_key} has no parent ids in consumes"
        )
    assert len(ev.produces) == 1
