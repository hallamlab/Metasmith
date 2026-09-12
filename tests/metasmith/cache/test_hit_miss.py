from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tests.metasmith.cache._cache_harness import capture_run, clear_trace
from tests.metasmith.cache.fixtures.cache_fixtures import (
    linear_3step,
    mixed_cacheability,
    parallel_then_group,
)


def _trace_path(virtual_runtime, task) -> Path:
    return (
        virtual_runtime.home
        / "runs"
        / task.GetKey()
        / "_metasmith"
        / "trace.jsonl"
    )


def _find_invocations(trace_path: Path, *, status):
    from metasmith.telemetry import TraceIndex

    if not trace_path.exists():
        return []
    idx = TraceIndex.read(trace_path)
    statuses = {status} if isinstance(status, str) else set(status)
    return [e for e in idx.events if e.status in statuses]


def test_default_cacheable_e2e_hits_on_rerun(tmp_path, virtual_runtime):
    task = linear_3step.build_task(tmp_path)
    snap1 = capture_run(virtual_runtime, task)
    assert snap1.executed_steps, "first run executed zero steps"

    clear_trace(virtual_runtime)
    snap2 = capture_run(virtual_runtime, task)
    assert snap2.executed_steps == ()


def test_miss_then_hit_explicit_trace_assertion(tmp_path, virtual_runtime):
    task = linear_3step.build_task(tmp_path)

    capture_run(virtual_runtime, task)
    run1_trace = _trace_path(virtual_runtime, task)
    assert run1_trace.exists(), "first run produced no trace.jsonl"

    promoted = _find_invocations(run1_trace, status="promoted")
    assert promoted, (
        "first run wrote no promoted rows; promote_run never appended"
    )
    misses = _find_invocations(run1_trace, status="miss")
    assert _find_invocations(run1_trace, status="hit") == [], (
        "first cold-cache run emitted a hit row"
    )
    assert misses or promoted, "first run wrote neither miss nor promoted rows"

    capture_run(virtual_runtime, task)
    run2_trace = _trace_path(virtual_runtime, task)
    hits = _find_invocations(run2_trace, status="hit")
    assert hits, (
        f"second run wrote no hit rows in {run2_trace}; cache probe missed"
    )
    statuses = {e.status for e in _find_invocations(
        run2_trace, status=("hit", "miss", "promoted", "fail")
    )}
    assert statuses <= {"hit"}, (
        f"second run mixed in non-hit rows: {statuses}"
    )


def test_cacheable_false_skips_publishDir(tmp_path, virtual_runtime):
    task = mixed_cacheability.build_task(tmp_path / "run1")
    capture_run(virtual_runtime, task)

    cache_db = virtual_runtime.home / "task_cache" / "cache.sqlite"
    assert cache_db.exists(), "promote did not write cache.sqlite"
    conn = sqlite3.connect(cache_db)
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM entries WHERE tombstoned_at IS NULL"
        ).fetchone()[0]
    finally:
        conn.close()
    assert n == 2, (
        f"expected 2 cache entries (trA + trC; trB cacheable=False), got {n}"
    )


def test_cache_hit_skips_executor(tmp_path, virtual_runtime):
    task = linear_3step.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    first_bootstrap_count = len(
        [e for e in virtual_runtime.parse_trace() if e.get("type") == "bootstrap_call"]
    )
    assert first_bootstrap_count > 0, "fixture executed zero steps on first run"

    clear_trace(virtual_runtime)
    capture_run(virtual_runtime, task)
    second_bootstrap_count = len(
        [e for e in virtual_runtime.parse_trace() if e.get("type") == "bootstrap_call"]
    )
    assert second_bootstrap_count == 0, (
        f"executor fired {second_bootstrap_count} times on a fully cached run"
    )


def test_cache_miss_writes_then_hits(tmp_path, virtual_runtime):
    task = linear_3step.build_task(tmp_path)
    snap1 = capture_run(virtual_runtime, task)
    assert snap1.cache_state != ()

    clear_trace(virtual_runtime)
    snap2 = capture_run(virtual_runtime, task)
    assert snap2.executed_steps == ()
    assert snap2.cache_state == snap1.cache_state


def test_synthetic_channel_survives_group_reduction(tmp_path, virtual_runtime):
    task = parallel_then_group.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    clear_trace(virtual_runtime)
    snap = capture_run(virtual_runtime, task)
    assert "trA" not in snap.executed_steps
    assert "trB" not in snap.executed_steps
    assert "trC" not in snap.executed_steps
