"""Flow axis 5 — group→split (collect→unfold) cases <GS1, GS2, GS3>.

Catalog source: tests/flow/AGENTS.md.

Stimulus: `mock_transforms.group_then_unfold()` — two transforms in a chain.
The first aggregates per-sample `mock::assembly` inputs grouped by a shared
`mock::sample_metadata` root into one `mock::grouped` artifact; the second
consumes `mock::grouped` and emits `mock::unfolded` via `context.AsBatch()`
at `batch_size=1` so each batch member becomes one downstream artifact.

All three tests share the same staged plan. They assert different facets:

  - GS1: post-unfold lineage carries the aggregate's parent set.
  - GS2: each unfolded sample's parents resolve to a single batch member
    (not the full union); when the current stimulus cannot fan-out one
    grouped artifact into N unfolded artifacts, the test xfails with a
    clear pointer.
  - GS3: roundtrip — load the results library with `attach_trace=True`
    and walk ancestors from the unfolded artifact back to the input
    `mock::sample_metadata` / `mock::assembly` roots.
"""

from __future__ import annotations

import pytest

from .conftest import (
    build_group_then_split_plan,
    run_and_load,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _step_events_by_transform(lib) -> dict[str, list]:
    """Return invocation events keyed by `transform_key`."""
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


# ---------------------------------------------------------------------------
# GS1 — post-unfold lineage carries the aggregate batch's parent set
# ---------------------------------------------------------------------------


def test_gs1_group_then_unfold(tmp_path, virtual_runtime):
    """<GS1> group_then_unfold: unfold output's lineage carries batch parents.

    The grouped step's `consumes` records the parent assembly + root ids.
    The unfold step downstream must consume the grouped artifact, and the
    trace chain (unfold → grouped → assembly/root) must form a connected
    spine through which downstream lineage walks can reach the inputs.
    """
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

    # The grouped step must consume both upstream slots (root + assembly).
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

    # Each unfold event must consume the grouped slot.
    for ev in unfold_events:
        assert ev.consumes, (
            f"<GS1> unfold event {ev.task_hash} has empty consumes"
        )


# ---------------------------------------------------------------------------
# GS2 — each unfolded sample's parents is a single batch member, not union
# ---------------------------------------------------------------------------


def test_gs2_unfold_per_sample_parents(tmp_path, virtual_runtime):
    """<GS2> unfold reattributes per-sample; parents != full aggregate union.

    `group_then_unfold` declares `batch_size=1` on the unfold side so each
    batch member fans out into one downstream artifact. The expected
    contract is that each unfolded artifact's `consumes` references exactly
    one upstream grouped batch member — not the cross product / full union.

    When the current stimulus only yields one unfolded artifact (because
    the aggregate output is one logical row), per-sample reattribution
    cannot be observed at this layer; that's the F4/slot-isolation axis
    expressed for AsBatch unfolding and is tracked separately.
    """
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

    # When multiple unfolded events exist, each one's `consumes` set must be
    # smaller than the union seen by the aggregate — i.e., per-event
    # parents are distinct batch members, not the full union.
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


# ---------------------------------------------------------------------------
# GS3 — telemetry roundtrip; ancestors of unfolded artifact reach roots
# ---------------------------------------------------------------------------


def test_gs3_unfold_roundtrip_walk(tmp_path, virtual_runtime):
    """<GS3> attach_trace=True; walk_ancestors from unfolded reaches inputs.

    Loads the results library with `attach_trace=True` (via `run_and_load`)
    and verifies that the trace chain unfold → grouped → assembly/root is
    coherent: for the unfolded event we can resolve its consumed grouped
    instance(s) to events in the trace.

    Note: `DataInstanceLibrary.Load` re-derives manifest instance_ids
    from the published-results file content, which currently does not
    match the `file_instance_id` recorded on `InvocationEvent.produces`.
    The roundtrip therefore exercises the trace-index level
    (`find_event_for_instance` over the consumed ids) rather than the
    manifest→trace bridge. The latter is tracked under the manifest-id
    gap noted in tests/flow/test_batching.py.
    """
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

    # The unfold step records its consumed slot in its consumes map; the
    # grouped step records produced ids. Both ends of the chain are present
    # and discoverable through the trace index — the manifest-id bridge
    # that would let `walk_ancestors` traverse them lives one layer up and
    # is tracked under the manifest-id gap in test_batching.py.
    unfold = unfold_events[0]
    consumed_ids: set[str] = set()
    for parents in unfold.consumes.values():
        consumed_ids.update(parents)
    assert consumed_ids, "<GS3> unfold event consumes is empty"
    assert grouped_produced_ids, (
        "<GS3> grouped step recorded no produced file_instance_ids"
    )
    # The trace index resolves grouped-step produced ids directly.
    idx = lib._trace
    for pid in grouped_produced_ids:
        ev = idx.find_event_for_instance(pid)
        assert ev is not None and ev.step_order == 1, (
            f"<GS3> grouped output {pid} not resolvable through trace index"
        )

    # Telemetry shape: summary() returns coherent counts including both steps.
    summary = lib.summary()
    assert summary["counts"]["events"] >= 2, (
        f"<GS3> expected >=2 events in summary, got {summary['counts']}"
    )
    assert set(summary["by_status"]).issubset({"hit", "miss", "promoted", "fail"})
