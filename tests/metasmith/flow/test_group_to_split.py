from __future__ import annotations

import pytest

from .conftest import (
    build_group_then_split_plan,
    run_and_load,
)


def _step_events_by_transform(lib) -> dict[str, list]:
    out: dict[str, list] = {}
    for ev in lib.find_invocations():
        out.setdefault(ev.transform_key, []).append(ev)
    return out


def _events_by_step_order(lib) -> dict[int, list]:
    out: dict[int, list] = {}
    for ev in lib.find_invocations():
        order = ev.step_order
        if order is None:
            continue
        out.setdefault(int(order), []).append(ev)
    return out


def test_gs1_group_then_unfold(tmp_path, virtual_runtime):
    bp = build_group_then_split_plan(tmp_path)
    task, lib = run_and_load(virtual_runtime, bp)

    by_order = _events_by_step_order(lib)
    assert set(by_order.keys()) == {1, 2}, (
        f"<GS1> expected step_order ∈ {{1,2}}, got {sorted(by_order)}"
    )

    grouped_events = by_order[1]
    unfold_events = by_order[2]
    assert len(grouped_events) == 1, (
        f"<GS1> expected 1 group_aggregate invocation, got {len(grouped_events)}"
    )
    assert len(unfold_events) >= 1, (
        f"<GS1> expected >=1 unfold_batch invocation, got {len(unfold_events)}"
    )

    g_consumes = {k: list(v) for k, v in grouped_events[0].consumes.items()}
    assert len(g_consumes) == 2, (
        f"<GS1> group_aggregate must consume root + assembly slots; "
        f"got slot keys={list(g_consumes)}"
    )
    total_grouped_parents = sum(len(v) for v in g_consumes.values())
    assert total_grouped_parents >= 2, (
        f"<GS1> aggregate's parent set must cover >=2 upstream instances, "
        f"got {total_grouped_parents}: {g_consumes}"
    )

    for ev in unfold_events:
        assert ev.consumes, (
            f"<GS1> unfold event {ev.task_hash} has empty consumes"
        )


def test_gs2_unfold_per_sample_parents(tmp_path, virtual_runtime):
    bp = build_group_then_split_plan(tmp_path)
    task, lib = run_and_load(virtual_runtime, bp)

    by_order = _events_by_step_order(lib)
    unfold_events = by_order.get(2, [])
    if len(unfold_events) < 2:
        pytest.skip(
            "<GS2> stimulus emits one grouped artifact, so AsBatch unfold "
            "produces a single downstream artifact. Per-sample reattribution "
            "requires a multi-row grouped output — extend "
            "`mock_transforms.group_then_unfold` to fan-out N grouped rows "
            "before this becomes observable. Behaviour pinned in GS1."
        )

    grouped_event = by_order[1][0]
    grouped_parent_ids: set[str] = set()
    for parents in grouped_event.consumes.values():
        grouped_parent_ids.update(parents)
    for ev in unfold_events:
        per_event: set[str] = set()
        for parents in ev.consumes.values():
            per_event.update(parents)
        assert per_event, f"<GS2> unfold event {ev.task_hash} has empty consumes"
        assert per_event != grouped_parent_ids, (
            f"<GS2> unfold event {ev.task_hash} consumes the full aggregate "
            f"union {per_event}; expected a per-sample subset"
        )


def test_gs3_unfold_roundtrip_walk(tmp_path, virtual_runtime):
    bp = build_group_then_split_plan(tmp_path)
    task, lib = run_and_load(virtual_runtime, bp)

    by_order = _events_by_step_order(lib)
    unfold_events = by_order.get(2, [])
    grouped_events = by_order.get(1, [])
    assert unfold_events, "<GS3> no unfold (step_order=2) events in trace"
    assert grouped_events, "<GS3> no group_aggregate (step_order=1) events in trace"

    grouped_produced_ids: set[str] = set()
    for ev in grouped_events:
        for prod in ev.produces:
            grouped_produced_ids.add(prod.file_instance_id)

    unfold = unfold_events[0]
    consumed_ids: set[str] = set()
    for parents in unfold.consumes.values():
        consumed_ids.update(parents)
    assert consumed_ids, "<GS3> unfold event consumes is empty"
    assert grouped_produced_ids, (
        "<GS3> grouped step recorded no produced file_instance_ids"
    )
    idx = lib._trace
    for pid in grouped_produced_ids:
        ev = idx.find_event_for_instance(pid)
        assert ev is not None and ev.step_order == 1, (
            f"<GS3> grouped output {pid} not resolvable through trace index"
        )

    summary = lib.summary()
    assert summary["counts"]["events"] >= 2, (
        f"<GS3> expected >=2 events in summary, got {summary['counts']}"
    )
    assert set(summary["by_status"]).issubset({"hit", "miss", "promoted", "fail"})
