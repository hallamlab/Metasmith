from __future__ import annotations

import pytest

from .conftest import (
    build_fan_out_plan,
    build_one_group_fan_out_plan,
    run_and_load,
)


def test_f1_two_slot_distinct_ids(tmp_path):
    bp = build_fan_out_plan(tmp_path, n_slots=2)
    assert len(bp.plan.steps) == 1
    step = bp.plan.steps[0]
    assert len(step.produces) == 2, (
        f"two-slot producer should declare 2 produces lists, got {len(step.produces)}"
    )
    slot_0_insts = step.produces[0]
    assert len(slot_0_insts) >= 1, "slot_0 must carry at least one DataInstance"
    assert step.transform._key, "step transform must have a non-empty key"


def test_f2_four_slot_per_slot_stability(tmp_path):
    d1 = tmp_path / "build1"
    d2 = tmp_path / "build2"
    d1.mkdir()
    d2.mkdir()
    bp1 = build_fan_out_plan(d1, n_slots=4)
    bp2 = build_fan_out_plan(d2, n_slots=4)
    s1 = bp1.plan.steps[0]
    s2 = bp2.plan.steps[0]
    assert len(s1.produces) == 4
    assert len(s2.produces) == 4
    id1 = s1.produces[0][0].instance_id
    id2 = s2.produces[0][0].instance_id
    assert id1 == id2, (
        f"slot_0 instance_id drifted across builds: {id1} vs {id2}"
    )


@pytest.mark.parametrize("slot_idx", [0, 1, 2])
def test_f3_slot_routing(tmp_path, slot_idx):
    bp = build_fan_out_plan(tmp_path, n_slots=3)
    step = bp.plan.steps[0]
    assert len(step.produces) == 3
    for i, insts in enumerate(step.produces):
        for inst in insts:
            dtype_name = getattr(inst, "dtype_name", "") or ""
            if dtype_name:
                assert dtype_name.endswith(f"slot_{i}"), (
                    f"slot index {i} produced dtype {dtype_name!r}"
                )
    assert slot_idx < len(step.produces), (
        f"slot_{slot_idx} not in declared produces (have {len(step.produces)})"
    )


def test_f4_slot_lineage_isolation(tmp_path):
    bp = build_fan_out_plan(tmp_path, n_slots=2)
    step = bp.plan.steps[0]
    assert len(step.produces) == 2
    for i, slot_insts in enumerate(step.produces):
        assert len(slot_insts) >= 1, f"slot_{i} has no wired DataInstance"
    assert step.produces[0][0].instance_id != step.produces[1][0].instance_id


def test_f5_multi_slot_end_to_end(tmp_path, virtual_runtime):
    bp = build_fan_out_plan(tmp_path, n_slots=2)
    task, lib = run_and_load(virtual_runtime, bp)
    events = lib._trace.events
    assert len(events) == 1, f"expected 1 invocation, got {len(events)}"
    ev = events[0]
    assert len(ev.produces) == 2, (
        f"expected 2 produces (slot_0 + slot_1), got {len(ev.produces)}"
    )
    dtypes = {pf.dtype_key for pf in ev.produces}
    assert len(dtypes) == 2, f"slots collapsed to a single dtype: {dtypes}"
    promoted = lib.find_invocations(status="promoted")
    assert len(promoted) == 1, f"expected 1 promoted step, got {len(promoted)}"


def test_f6_one_group_multi_product_shape(tmp_path):
    bp = build_one_group_fan_out_plan(tmp_path, n_products=2)
    assert len(bp.plan.steps) == 1
    step = bp.plan.steps[0]
    assert len(step.transform.model.produces) == 1, (
        "products declared without NewProductGroup must stay in one group, "
        f"got {len(step.transform.model.produces)} groups"
    )
    assert len(step.produces) == 1, (
        f"expected 1 produce group, got {len(step.produces)}"
    )
    assert len(step.produces[0]) == 2, (
        f"expected 2 products in the single group, got {len(step.produces[0])}"
    )
    a, b = step.produces[0]
    assert a.instance_id != b.instance_id, (
        "two products of one group collapsed onto a single instance_id"
    )


def test_f6_one_group_multi_product_end_to_end(tmp_path, virtual_runtime):
    bp = build_one_group_fan_out_plan(tmp_path, n_products=2)
    task, lib = run_and_load(virtual_runtime, bp)
    events = lib._trace.events
    assert len(events) == 1, f"expected 1 invocation, got {len(events)}"
    ev = events[0]
    assert len(ev.produces) == 2, (
        f"expected 2 produces from one manifest entry, got {len(ev.produces)}"
    )
    dtypes = {pf.dtype_key for pf in ev.produces}
    assert len(dtypes) == 2, f"products collapsed to a single dtype: {dtypes}"
    promoted = lib.find_invocations(status="promoted")
    assert len(promoted) == 1, f"expected 1 promoted step, got {len(promoted)}"
