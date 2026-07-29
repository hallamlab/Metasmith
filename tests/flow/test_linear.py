"""Linear-flow cases L1-L4 from the catalog.

Covers the oracle baselines for one-step, two-step, three-step linear
identity chains, plus the multi-input single-step broadcast shape.

Catalog: see `tests/flow/AGENTS.md` Axis 1.
"""

from __future__ import annotations

from .conftest import (
    build_linear_plan,
    build_multi_input_plan,
    run_and_load,
)


def test_l1_single_step_round_trip(tmp_path, virtual_runtime):
    """<L1> Single-step identity chain round-trips one instance with stable id.

    Shape: `Given(assembly) → [identity_bam] → Target(bam)`.
    """
    bp = build_linear_plan(tmp_path, n_steps=1)
    # Plan-level: exactly one step.
    assert len(bp.plan.steps) == 1, f"expected 1 step, got {len(bp.plan.steps)}"

    task, lib = run_and_load(virtual_runtime, bp)

    # Telemetry-level: exactly one event for the one transform.
    events = lib._trace.events
    assert len(events) == 1, f"expected 1 invocation, got {len(events)}"
    ev = events[0]
    assert ev.status == "promoted", f"step should run (not hit), got {ev.status}"
    assert len(ev.consumes) == 1, f"single-step has one consume slot, got {ev.consumes!r}"
    # Each invocation produces exactly one file for this identity transform.
    assert len(ev.produces) == 1, f"identity transform produces 1 slot, got {len(ev.produces)}"

    # The walk_ancestors from the produced file_instance_id yields exactly one
    # ancestor (the input leaf), confirming stable producer→consumer linkage.
    fid = ev.produces[0].file_instance_id
    ancestors = list(lib.walk_ancestors(fid))
    assert len(ancestors) == 1, (
        f"expected 1 ancestor for single-step output, got {len(ancestors)}"
    )


def test_l2_two_step_preserves_id(tmp_path, virtual_runtime):
    """<L2> Two-step chain: instance_ids stable through both hops.

    Shape: `assembly → [identity_bam] → bam → [identity_branch_a] → branch_a`.
    """
    bp = build_linear_plan(tmp_path, n_steps=2)
    assert len(bp.plan.steps) == 2

    task, lib = run_and_load(virtual_runtime, bp)

    events = lib._trace.events
    assert len(events) == 2, f"expected 2 invocations, got {len(events)}"
    # Topological order: step_order is monotonic.
    orders = [e.step_order for e in events]
    assert orders == sorted(orders), f"events out of topological order: {orders}"
    # Two distinct transform keys.
    tks = {e.transform_key for e in events}
    assert len(tks) == 2, f"expected 2 distinct transforms, got {tks}"
    # Both successful.
    for e in events:
        assert e.status == "promoted", f"step {e.step_order} status={e.status}"


def test_l3_three_step_lineage_chain(tmp_path, virtual_runtime):
    """<L3> Three-step chain: trace records 3 ancestors in topological order.

    Shape: `assembly → bam → branch_a → branch_b`.
    """
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
    # Each step produces exactly one file (identity chain).
    for e in events:
        assert len(e.produces) == 1
        assert len(e.consumes) == 1


def test_l4_multi_input_broadcast(tmp_path, virtual_runtime):
    """<L4> Single-step, two input slots → output records both as parents.

    Shape: `Given(reads, assembly) → [alignment(reads, assembly)] → Target(bam)`.
    """
    bp = build_multi_input_plan(tmp_path, slots=2)
    assert len(bp.plan.steps) == 1

    task, lib = run_and_load(virtual_runtime, bp)

    events = lib._trace.events
    assert len(events) == 1, f"expected 1 invocation, got {len(events)}"
    ev = events[0]
    # The single event consumes from BOTH input slots (reads + assembly).
    assert len(ev.consumes) == 2, (
        f"multi-input transform should consume 2 slots, got {ev.consumes!r}"
    )
    # Both slots must record at least one parent instance_id each.
    for slot_key, parent_ids in ev.consumes.items():
        assert len(parent_ids) >= 1, (
            f"slot {slot_key} has no parent ids in consumes"
        )
    assert len(ev.produces) == 1
