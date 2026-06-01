"""Cache opt-out / kill-switch axis.

Two ways to disable caching, asserted independently:

  * per-transform: `TransformInstance(..., cacheable=False)` — the
    affected transform never produces a cache entry and never serves
    one, even when a hot cache exists for prior runs.

  * global kill-switch: `METASMITH_CACHE=0` env — the compile-time
    probe is short-circuited even with a hot cache, so every step
    re-executes and trace.jsonl is dominated by promoted (never hit)
    rows.

Both assertions go through the trace.jsonl telemetry surface so
regressions in either probe or promote surfaces here, not in a
file-content diff.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tests.cache._cache_harness import capture_run, clear_trace
from tests.cache.fixtures.cache_fixtures import linear_3step, mixed_cacheability


def _trace_path(virtual_runtime, task) -> Path:
    return (
        virtual_runtime.home
        / "runs"
        / task.GetKey()
        / "_metasmith"
        / "trace.jsonl"
    )


def _events_by_status(trace_path: Path) -> dict[str, list]:
    """Group InvocationEvents in trace.jsonl by status."""
    from metasmith.telemetry import TraceIndex

    if not trace_path.exists():
        return {}
    idx = TraceIndex.read(trace_path)
    out: dict[str, list] = {}
    for e in idx.events:
        out.setdefault(e.status, []).append(e)
    return out


# ---------------------------------------------------------------------------
# Per-transform opt-out
# ---------------------------------------------------------------------------


def test_cacheable_false_never_emits_hit_even_with_hot_cache(
    tmp_path, virtual_runtime
):
    """A transform flagged `cacheable=False` must never appear as a hit.

    Build the mixed_cacheability fixture twice on the SAME agent home
    so any cache that *could* be reused is present on the second run.
    trB (cacheable=False) must:
      - never produce a cache.sqlite row, AND
      - never appear in a `status="hit"` invocation on the second run.

    trB's invocation rows on both runs should be `status="promoted"`
    (or "miss" with non-zero exit) — execution-only, no cache traffic.
    """
    task = mixed_cacheability.build_task(tmp_path / "run")

    # Two consecutive runs on the same agent home.
    capture_run(virtual_runtime, task)
    capture_run(virtual_runtime, task)

    # No cache row was written for trB on the first run.
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
    # No transform_key for trB should appear in the cache. We use a
    # substring match because the stored key is the full transform
    # signature (file path / signature), not the bare name.
    trB_keys = [k for k in tkeys if "trB" in k]
    assert not trB_keys, (
        f"trB (cacheable=False) leaked into cache as: {trB_keys}"
    )

    # And on the second run, no trB invocation row carries status="hit".
    trace = _trace_path(virtual_runtime, task)
    by_status = _events_by_status(trace)
    hit_trB = [
        e for e in by_status.get("hit", []) if (e.step_name or "") == "trB"
    ]
    assert not hit_trB, (
        f"trB (cacheable=False) served a hit on rerun: {hit_trB}"
    )


# ---------------------------------------------------------------------------
# Global kill-switch
# ---------------------------------------------------------------------------


def test_env_kill_switch_short_circuits_probe(
    tmp_path, virtual_runtime, monkeypatch
):
    """METASMITH_CACHE=0 disables the probe even with a hot cache.

    Run 1 (env unset) populates the cache. Run 2 (env=0) must:
      - re-execute every step (cache_state from run 1 is unchanged), and
      - emit no `status="hit"` rows in trace.jsonl on run 2.
    """
    # Run 1 primes the cache.
    task = linear_3step.build_task(tmp_path)
    snap1 = capture_run(virtual_runtime, task)
    assert snap1.cache_state != (), (
        "first run without kill-switch should populate the cache"
    )
    clear_trace(virtual_runtime)

    # Run 2 with kill-switch ON.
    monkeypatch.setenv("METASMITH_CACHE", "0")
    snap2 = capture_run(virtual_runtime, task)

    # Cache must not be written to nor served from.
    assert snap2.cache_state == snap1.cache_state, (
        "kill-switch on; cache must not be written to"
    )
    assert snap2.executed_steps == snap1.executed_steps, (
        "kill-switch on; run 2 should re-execute the full plan"
    )

    # And no hit rows on run 2.
    by_status = _events_by_status(_trace_path(virtual_runtime, task))
    hits = by_status.get("hit", [])
    assert not hits, (
        f"kill-switch on; got {len(hits)} hit rows in trace.jsonl: "
        f"{[e.step_name for e in hits]}"
    )
