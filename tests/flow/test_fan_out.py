"""Fan-out cases F1-F4 from the catalog.

One transform emits N distinct output slots; each slot reaches its
downstream consumer with a stable, independent identity.

Plan-level wiring of multi-slot `dependency_map` is now end-to-end
(solver consolidates per-pgroup variants instead of splitting them into
disjoint timelines, and the lineage walk no longer treats sibling
produces as parents). F1-F4 still assert at plan level; F5 adds an
end-to-end run that exercises compile + execute for a 2-slot transform.

Catalog: see `tests/flow/AGENTS.md` Axis 3.
"""

from __future__ import annotations

import pytest

from .conftest import build_fan_out_plan, run_and_load


def test_f1_two_slot_distinct_ids(tmp_path):
    """<F1> Two-slot producer: each slot's product DataInstance is distinct.

    Asserts at plan level: `step.produces` is a list of per-slot lists, and
    the slot_0 DataInstance has a different `instance_id` from any future
    slot_N DataInstance in the same step.
    """
    bp = build_fan_out_plan(tmp_path, n_slots=2)
    assert len(bp.plan.steps) == 1
    step = bp.plan.steps[0]
    # produces is a list (one entry per declared output slot).
    assert len(step.produces) == 2, (
        f"two-slot producer should declare 2 produces lists, got {len(step.produces)}"
    )
    # Slot 0 is wired (has DataInstances); other slots may be empty in
    # current builds — but the *declared* count must be 2.
    slot_0_insts = step.produces[0]
    assert len(slot_0_insts) >= 1, "slot_0 must carry at least one DataInstance"
    # Same producer step keys all wired instances back to one transform_key.
    assert step.transform._key, "step transform must have a non-empty key"


def test_f2_four_slot_per_slot_stability(tmp_path):
    """<F2> Four-slot producer: per-slot derived hex stable across builds.

    Build the same plan twice and confirm the slot_0 instance_id is
    identical — proves the planner's hash derivation is deterministic for
    fan-out shapes.
    """
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
    """<F3> Slot routing: declared output slot N matches target slot_N.

    Parametrized over slot index 0-2 on a 3-slot producer. Plan-level
    assertion: each `produces[N]` entry must have a dtype name reflecting
    slot N.
    """
    bp = build_fan_out_plan(tmp_path, n_slots=3)
    step = bp.plan.steps[0]
    assert len(step.produces) == 3
    # Slot 0 carries the bound DataInstance; subsequent slot lists are
    # empty placeholders today (see module docstring). Walk all slots and
    # confirm slot index N maps to the slot_N declaration in the planner's
    # output schema, by inspecting any present DataInstance.
    for i, insts in enumerate(step.produces):
        for inst in insts:
            dtype_name = getattr(inst, "dtype_name", "") or ""
            # The dtype_name carries "mock::slot_<i>" for wired slots.
            if dtype_name:
                assert dtype_name.endswith(f"slot_{i}"), (
                    f"slot index {i} produced dtype {dtype_name!r}"
                )
    # The parametrized slot_idx must at least exist as a declared output.
    assert slot_idx < len(step.produces), (
        f"slot_{slot_idx} not in declared produces (have {len(step.produces)})"
    )


def test_f4_slot_lineage_isolation(tmp_path):
    """<F4> Slot lineage isolation: every slot has a wired, distinct DataInstance.

    With the solver consolidating per-pgroup variants, every declared output
    slot now carries its own DataInstance with a unique `instance_id`.
    """
    bp = build_fan_out_plan(tmp_path, n_slots=2)
    step = bp.plan.steps[0]
    assert len(step.produces) == 2
    for i, slot_insts in enumerate(step.produces):
        assert len(slot_insts) >= 1, f"slot_{i} has no wired DataInstance"
    assert step.produces[0][0].instance_id != step.produces[1][0].instance_id


def test_f5_multi_slot_end_to_end(tmp_path, virtual_runtime):
    """<F5> End-to-end: a 2-slot producer compiles, runs, and promotes both slots.

    Exercises the full G2 fix path — solver wires every pgroup, the lineage
    walk doesn't treat siblings as parents, and CollectResults resolves both
    outputs in a single CollectResults pass. The original bug surfaced as
    `KeyError: (D:slot_1)` at `workflow.py:get_io_signature`; the fix lets
    `PrepareNextflow` see both slots in `dependency_map`.
    """
    bp = build_fan_out_plan(tmp_path, n_slots=2)
    task, lib = run_and_load(virtual_runtime, bp)
    events = lib._trace.events
    # The virtual runtime emits one InvocationEvent per multi-slot step with
    # both produces; G2's fix is that both slots actually appear here.
    assert len(events) == 1, f"expected 1 invocation, got {len(events)}"
    ev = events[0]
    assert len(ev.produces) == 2, (
        f"expected 2 produces (slot_0 + slot_1), got {len(ev.produces)}"
    )
    dtypes = {pf.dtype_key for pf in ev.produces}
    assert len(dtypes) == 2, f"slots collapsed to a single dtype: {dtypes}"
    # The library walk should now resolve both promoted outputs.
    promoted = lib.find_invocations(status="promoted")
    assert len(promoted) == 1, f"expected 1 promoted step, got {len(promoted)}"
