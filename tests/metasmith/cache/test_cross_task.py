from __future__ import annotations

from pathlib import Path

import pytest

from tests.metasmith.cache._cache_harness import (
    build_samples_library,
    build_transform_library,
    build_types_library,
    build_workflow_task,
    capture_run,
    identity_transform_code,
)


TYPE_NAMES = ("seed", "shared", "left", "right")


def _build_fan_out_task(tmp_path: Path):
    types_path = build_types_library(tmp_path, TYPE_NAMES)
    samples = build_samples_library(
        tmp_path, types_path, count=1, input_type="seed"
    )
    transforms = {
        "trA": identity_transform_code("trA", "seed", "shared"),
        "trL": identity_transform_code("trL", "shared", "left"),
        "trR": identity_transform_code("trR", "shared", "right"),
    }
    tr_lib = build_transform_library(tmp_path / "tr", types_path, transforms)
    return build_workflow_task(
        samples,
        tr_lib,
        sample_type="seed",
        target_specs=[
            ("left_target", {"left"}),
            ("right_target", {"right"}),
        ],
    )


def _trace_path(virtual_runtime, task) -> Path:
    return (
        virtual_runtime.home
        / "runs"
        / task.GetKey()
        / "_metasmith"
        / "trace.jsonl"
    )


def test_shared_upstream_executes_once(tmp_path, virtual_runtime):
    task = _build_fan_out_task(tmp_path)
    capture_run(virtual_runtime, task)

    events = virtual_runtime.parse_trace()
    by_step: dict[str, int] = {}
    for e in events:
        if e.get("type") == "bootstrap_call":
            by_step[e.get("step_name", "")] = by_step.get(
                e.get("step_name", ""), 0
            ) + 1

    assert by_step.get("trA", 0) == 1, (
        f"shared upstream trA executed {by_step.get('trA', 0)} times; "
        f"expected 1. full counts: {by_step}"
    )
    assert by_step.get("trL", 0) == 1
    assert by_step.get("trR", 0) == 1


def test_shared_upstream_one_cache_key(tmp_path, virtual_runtime):
    import sqlite3

    task = _build_fan_out_task(tmp_path)
    capture_run(virtual_runtime, task)

    cache_db = virtual_runtime.home / "task_cache" / "cache.sqlite"
    assert cache_db.exists(), "promote did not write cache.sqlite"
    conn = sqlite3.connect(cache_db)
    try:
        rows = conn.execute(
            "SELECT transform_key, COUNT(*) FROM entries "
            "WHERE tombstoned_at IS NULL GROUP BY transform_key"
        ).fetchall()
    finally:
        conn.close()
    by_tr = dict(rows)
    assert sum(by_tr.values()) == 3, (
        f"expected 3 cache rows (one per transform), got {by_tr}"
    )
    duplicates = [tk for tk, n in by_tr.items() if n > 1]
    assert not duplicates, (
        f"shared upstream cache entry duplicated: {duplicates}"
    )


def test_shared_upstream_lineage_walks_back_through_one_parent(
    tmp_path, virtual_runtime
):
    from metasmith.telemetry import TraceIndex

    task = _build_fan_out_task(tmp_path)
    capture_run(virtual_runtime, task)

    trace_path = _trace_path(virtual_runtime, task)
    assert trace_path.exists(), f"no trace.jsonl at {trace_path}"
    idx = TraceIndex.read(trace_path)

    by_step: dict[str, list] = {}
    for ev in idx.events:
        if ev.step_name:
            by_step.setdefault(ev.step_name, []).append(ev)

    assert "trL" in by_step and "trR" in by_step, (
        f"missing consumer events in trace: {list(by_step)}"
    )

    def _consumed_ids(ev) -> set[str]:
        out: set[str] = set()
        for vs in ev.consumes.values():
            out.update(vs)
        return out

    left_consumed = _consumed_ids(by_step["trL"][0])
    right_consumed = _consumed_ids(by_step["trR"][0])

    shared = left_consumed & right_consumed
    assert shared, (
        f"left ({left_consumed}) and right ({right_consumed}) consumer "
        f"events share no upstream input id; planner emitted distinct "
        f"trA instances per branch"
    )
