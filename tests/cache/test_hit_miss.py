"""Hit/miss decision axis.

The compile-time probe + post-exec promote loop is the load-bearing
contract. Two runs of the same task on the same agent home: first
populates the cache (`status="miss"` + `status="promoted"`), second
reads it back (`status="hit"`). The executor must not fire on a
fully-cached rerun, and `cacheable=False` opt-outs must be honored end
to end including under the kill switch.

Coverage: G1 (default-on caching), G3 (executor skip on hit), G6
(cacheable=False opt-out), G4/G12 (synthetic channel survives o.group
reduction), and the explicit miss-then-hit telemetry assertion against
trace.jsonl via the telemetry API.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tests.cache._cache_harness import capture_run, clear_trace
from tests.cache.fixtures.cache_fixtures import (
    linear_3step,
    mixed_cacheability,
    parallel_then_group,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _trace_path(virtual_runtime, task) -> Path:
    """Resolve the per-run trace.jsonl path for a given task."""
    return (
        virtual_runtime.home
        / "runs"
        / task.GetKey()
        / "_metasmith"
        / "trace.jsonl"
    )


def _find_invocations(trace_path: Path, *, status):
    """Pull InvocationEvent rows from a trace.jsonl, filtered by status.

    Goes through the telemetry API rather than parsing JSON by hand —
    the same path real users hit via `DataInstanceLibrary.Load(
    attach_trace=True).find_invocations(...)`.
    """
    from metasmith.telemetry import TraceIndex

    if not trace_path.exists():
        return []
    idx = TraceIndex.read(trace_path)
    statuses = {status} if isinstance(status, str) else set(status)
    return [e for e in idx.events if e.status in statuses]


# ---------------------------------------------------------------------------
# Miss → hit (the contract)
# ---------------------------------------------------------------------------


def test_default_cacheable_e2e_hits_on_rerun(tmp_path, virtual_runtime):
    """G1, G6: default cacheable=True; second run is a full cache hit.

    Two consecutive runs of the SAME task with every transform default
    `cacheable=True`. The second run's executed_steps must be empty —
    every step served from synthetic channels, no real process fires.
    """
    task = linear_3step.build_task(tmp_path)
    snap1 = capture_run(virtual_runtime, task)
    assert snap1.executed_steps, "first run executed zero steps"

    clear_trace(virtual_runtime)
    snap2 = capture_run(virtual_runtime, task)
    assert snap2.executed_steps == ()


def test_miss_then_hit_explicit_trace_assertion(tmp_path, virtual_runtime):
    """First run records miss+promoted rows; second run records hit rows.

    This is the explicit miss-then-hit assertion the cache axis is
    contractually required to pin (per `tests/cache/AGENTS.md`). It
    asserts via the telemetry surface (`TraceIndex.events` filtered by
    `status`), so a regression in either the compile-time hit-row
    emission or the post-exec promote append surfaces here, not in a
    raw file-content diff.
    """
    task = linear_3step.build_task(tmp_path)

    # Run 1: cold cache → executor runs every step → promote_run appends
    # one promoted row per cacheable step.
    capture_run(virtual_runtime, task)
    run1_trace = _trace_path(virtual_runtime, task)
    assert run1_trace.exists(), "first run produced no trace.jsonl"

    promoted = _find_invocations(run1_trace, status="promoted")
    assert promoted, (
        "first run wrote no promoted rows; promote_run never appended"
    )
    misses = _find_invocations(run1_trace, status="miss")
    # Either miss or promoted rows are acceptable for run 1 depending on
    # whether the runtime emits both — pin: at least one of the two
    # exists per executed step, and no hits.
    assert _find_invocations(run1_trace, status="hit") == [], (
        "first cold-cache run emitted a hit row"
    )
    assert misses or promoted, "first run wrote neither miss nor promoted rows"

    # Run 2: same task, same agent home → compile-time probe hits every
    # cacheable step → trace.jsonl leads with N hit rows.
    capture_run(virtual_runtime, task)
    run2_trace = _trace_path(virtual_runtime, task)
    hits = _find_invocations(run2_trace, status="hit")
    assert hits, (
        f"second run wrote no hit rows in {run2_trace}; cache probe missed"
    )
    # Every event the second run emits must be a hit — no executor fire.
    statuses = {e.status for e in _find_invocations(
        run2_trace, status=("hit", "miss", "promoted", "fail")
    )}
    assert statuses <= {"hit"}, (
        f"second run mixed in non-hit rows: {statuses}"
    )


def test_cacheable_false_skips_publishDir(tmp_path, virtual_runtime):
    """G6: cacheable=False writes no cache entry.

    The mixed_cacheability fixture flags trB `cacheable=False`; the
    cache sqlite should contain rows for trA + trC only.
    """
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
    """G3: on a fully cached run, no bootstrap fires for any plan step."""
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
    """G1, G3: a fresh workspace misses; the same task then hits."""
    task = linear_3step.build_task(tmp_path)
    snap1 = capture_run(virtual_runtime, task)
    assert snap1.cache_state != ()

    clear_trace(virtual_runtime)
    snap2 = capture_run(virtual_runtime, task)
    assert snap2.executed_steps == ()
    assert snap2.cache_state == snap1.cache_state


def test_synthetic_channel_survives_group_reduction(tmp_path, virtual_runtime):
    """G4, G12: cached step emits synthetic channel; o.group reduction completes.

    Build a parallel_then_group task, prime the cache via a first run,
    then on a second run every step is cached. The synthetic
    `Channel.of(...)` must re-enter `o.post()` so index_history
    populates and the downstream `o.group()` reduction in trB
    completes correctly — we assert by checking that trA / trB / trC
    are absent from the executed-steps tuple on rerun (full hit).
    """
    task = parallel_then_group.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    clear_trace(virtual_runtime)
    snap = capture_run(virtual_runtime, task)
    assert "trA" not in snap.executed_steps
    assert "trB" not in snap.executed_steps
    assert "trC" not in snap.executed_steps
