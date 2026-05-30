"""Forward-looking xfail tests for cache hit/miss execution semantics.

Coverage: G1 / G3 / G6 (default-on caching, opt-out, executor skip on hit,
emergency kill-switch).
All xfail-strict — flipped green by S3 + S4 + S5.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tests.integration._cache_harness import capture_run, clear_trace
from tests.integration.fixtures.cache_fixtures import (
    linear_3step,
    mixed_cacheability,
)

from tests.e2e_virtual.conftest import virtual_runtime  # noqa: F401


@pytest.mark.xfail(strict=True, reason="S3 synthetic-channel hit path not landed")
def test_default_cacheable_e2e_hits_on_rerun(tmp_path, virtual_runtime):
    """G1, G6: default cacheable=True; second run is a full cache hit.

    Pinned to S4 + S3. Two consecutive runs of the same fixture, where
    every transform has the default `cacheable=True`. The second run's
    executed_steps must be empty (no real process executions; everything
    served from synthetic channels).
    """
    task = linear_3step.build_task(tmp_path / "run1")
    snap1 = capture_run(virtual_runtime, task)
    assert snap1.executed_steps  # first run did execute

    clear_trace(virtual_runtime)
    task2 = linear_3step.build_task(tmp_path / "run2")
    snap2 = capture_run(virtual_runtime, task2)
    assert snap2.executed_steps == ()


def test_cacheable_false_skips_publishDir(tmp_path, virtual_runtime):
    """G6: cacheable=False emits no publishDir and writes no cache entry.

    Pinned to S4. The mixed_cacheability fixture has 3 transforms; trB is
    flagged `cacheable=False` so only 2 entries should land in the cache.
    Cache shard paths are multihash hex (no transform-name leakage) so we
    assert on the SQLite row count rather than path strings. If S4's
    opt-out path did not honor `cacheable=False`, all 3 steps would
    promote and the count would be 3.
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


@pytest.mark.xfail(strict=True, reason="S3 synthetic-channel hit path not landed")
def test_cache_hit_skips_executor(tmp_path, virtual_runtime):
    """G3: on a fully cached run, no bootstrap fires for any plan step.

    Pinned to S3 + S5. The virtual runtime emits one `bootstrap_call`
    trace event per real process invocation — those are the executor
    surrogate here. After priming the cache, the second run's
    bootstrap_call count must drop from N (first run) to 0.
    """
    task = linear_3step.build_task(tmp_path / "run1")
    capture_run(virtual_runtime, task)
    first_bootstrap_count = len(
        [e for e in virtual_runtime.parse_trace() if e.get("type") == "bootstrap_call"]
    )
    assert first_bootstrap_count > 0, "fixture executed zero steps on first run"

    clear_trace(virtual_runtime)
    task2 = linear_3step.build_task(tmp_path / "run2")
    capture_run(virtual_runtime, task2)
    second_bootstrap_count = len(
        [e for e in virtual_runtime.parse_trace() if e.get("type") == "bootstrap_call"]
    )
    assert second_bootstrap_count == 0, (
        f"executor fired {second_bootstrap_count} times on a fully cached run"
    )


@pytest.mark.xfail(strict=True, reason="S3 synthetic-channel hit path not landed")
def test_cache_miss_writes_then_hits(tmp_path, virtual_runtime):
    """G1, G3: a fresh workspace misses; the same task then hits.

    Pinned to S5. First run populates the cache; second run on the
    *same* agent home reads it back. Concrete: cache_state after run 1
    is non-empty, after run 2 is unchanged, and run 2's
    executed_steps == ().
    """
    task = linear_3step.build_task(tmp_path / "run1")
    snap1 = capture_run(virtual_runtime, task)
    assert snap1.cache_state != ()

    clear_trace(virtual_runtime)
    task2 = linear_3step.build_task(tmp_path / "run2")
    snap2 = capture_run(virtual_runtime, task2)
    assert snap2.executed_steps == ()
    assert snap2.cache_state == snap1.cache_state


def test_emergency_off_switch(tmp_path, virtual_runtime, monkeypatch):
    """S4: METASMITH_CACHE=0 disables the cache, env unset uses it.

    Asserts the symmetric difference: with the env unset, run 2 hits
    (executed_steps empty, cache populated). With METASMITH_CACHE=0
    primed *before* the second run, the cache is bypassed (executed_steps
    non-empty even though run 1 populated the cache). On main neither
    branch reflects caching, so the second branch's `executed_steps
    differs from the cached-baseline` assertion fails because there is
    no baseline to compare to.
    """
    # First run primes the cache (env unset).
    task1 = linear_3step.build_task(tmp_path / "run1")
    snap1 = capture_run(virtual_runtime, task1)
    clear_trace(virtual_runtime)

    # Second run with kill-switch ON should ignore the primed cache.
    monkeypatch.setenv("METASMITH_CACHE", "0")
    task2 = linear_3step.build_task(tmp_path / "run2")
    snap2 = capture_run(virtual_runtime, task2)

    # If caching is implemented and honored, the kill switch makes run 2
    # behave like an uncached miss → cache_state must not grow.
    assert snap2.cache_state == snap1.cache_state, (
        "kill-switch on; cache must not be written to"
    )
    # And run 2 should fully execute — proving the env was respected
    # rather than the run silently caching anyway.
    assert snap2.executed_steps == snap1.executed_steps, (
        "kill-switch on; run 2 should re-execute the full plan"
    )
    # The discriminator vs main: snap1 itself must be a populated cache
    # (proving caching was actually active in the first run).
    assert snap1.cache_state != (), (
        "first run without kill-switch should populate the cache"
    )
