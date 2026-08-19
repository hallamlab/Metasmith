from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tests.metasmith.cache._cache_harness import capture_run, clear_trace
from tests.metasmith.cache.fixtures.cache_fixtures import linear_3step, mixed_cacheability


def _trace_path(virtual_runtime, task) -> Path:
    return (
        virtual_runtime.home
        / "runs"
        / task.GetKey()
        / "_metasmith"
        / "trace.jsonl"
    )


def _events_by_status(trace_path: Path) -> dict[str, list]:
    from metasmith.telemetry import TraceIndex

    if not trace_path.exists():
        return {}
    idx = TraceIndex.read(trace_path)
    out: dict[str, list] = {}
    for e in idx.events:
        out.setdefault(e.status, []).append(e)
    return out


def test_cacheable_false_never_emits_hit_even_with_hot_cache(
    tmp_path, virtual_runtime
):
    task = mixed_cacheability.build_task(tmp_path / "run")

    capture_run(virtual_runtime, task)
    capture_run(virtual_runtime, task)

    cache_db = virtual_runtime.home / "task_cache" / "cache.sqlite"
    assert cache_db.exists()
    conn = sqlite3.connect(cache_db)
    try:
        tkeys = [
            r[0]
            for r in conn.execute(
                "SELECT transform_key FROM entries WHERE tombstoned_at IS NULL"
            ).fetchall()
        ]
    finally:
        conn.close()
    trB_keys = [k for k in tkeys if "trB" in k]
    assert not trB_keys, (
        f"trB (cacheable=False) leaked into cache as: {trB_keys}"
    )

    trace = _trace_path(virtual_runtime, task)
    by_status = _events_by_status(trace)
    hit_trB = [
        e for e in by_status.get("hit", []) if (e.step_name or "") == "trB"
    ]
    assert not hit_trB, (
        f"trB (cacheable=False) served a hit on rerun: {hit_trB}"
    )


def test_env_kill_switch_short_circuits_probe(
    tmp_path, virtual_runtime, monkeypatch
):
    task = linear_3step.build_task(tmp_path)
    snap1 = capture_run(virtual_runtime, task)
    assert snap1.cache_state != (), (
        "first run without kill-switch should populate the cache"
    )
    clear_trace(virtual_runtime)

    monkeypatch.setenv("METASMITH_CACHE", "0")
    snap2 = capture_run(virtual_runtime, task)

    assert snap2.cache_state == snap1.cache_state, (
        "kill-switch on; cache must not be written to"
    )
    assert snap2.executed_steps == snap1.executed_steps, (
        "kill-switch on; run 2 should re-execute the full plan"
    )

    by_status = _events_by_status(_trace_path(virtual_runtime, task))
    hits = by_status.get("hit", [])
    assert not hits, (
        f"kill-switch on; got {len(hits)} hit rows in trace.jsonl: "
        f"{[e.step_name for e in hits]}"
    )
