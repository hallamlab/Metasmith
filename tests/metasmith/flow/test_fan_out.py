from __future__ import annotations

import pytest

from .conftest import (
    build_fan_out_plan,
    run_and_load,
)


def test_f1_products_share_one_group_with_distinct_ids(tmp_path):
    bp = build_fan_out_plan(tmp_path, n_products=2)
    assert len(bp.plan.steps) == 1
    step = bp.plan.steps[0]
    assert len(step.transform.model.produces) == 1, (
        f"co-produced outputs must share one group, "
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


def test_f2_product_ids_stable_across_builds(tmp_path):
    d1 = tmp_path / "build1"
    d2 = tmp_path / "build2"
    d1.mkdir()
    d2.mkdir()
    s1 = build_fan_out_plan(d1, n_products=4).plan.steps[0]
    s2 = build_fan_out_plan(d2, n_products=4).plan.steps[0]
    assert len(s1.produces[0]) == 4
    assert len(s2.produces[0]) == 4
    ids1 = [inst.instance_id for inst in s1.produces[0]]
    ids2 = [inst.instance_id for inst in s2.produces[0]]
    assert ids1 == ids2, f"product instance_ids drifted across builds: {ids1} vs {ids2}"


@pytest.mark.parametrize("slot_idx", [0, 1, 2])
def test_f3_product_routing(tmp_path, slot_idx):
    step = build_fan_out_plan(tmp_path, n_products=3).plan.steps[0]
    group = step.produces[0]
    assert len(group) == 3
    for i, inst in enumerate(group):
        dtype_name = getattr(inst, "dtype_name", "") or ""
        if dtype_name:
            assert dtype_name.endswith(f"slot_{i}"), (
                f"product index {i} produced dtype {dtype_name!r}"
            )
    assert slot_idx < len(group)


def test_f5_one_group_end_to_end(tmp_path, virtual_runtime):
    bp = build_fan_out_plan(tmp_path, n_products=2)
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
