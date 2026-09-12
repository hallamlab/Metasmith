from __future__ import annotations

from pathlib import Path

import pytest

from tests.metasmith.cache._cache_harness import capture_run, clear_trace
from tests.metasmith.cache.fixtures.cache_fixtures import linear_3step


def test_full_caching_flow_end_to_end(tmp_path, virtual_runtime):
    task = linear_3step.build_task(tmp_path)
    snap1 = capture_run(virtual_runtime, task)

    assert snap1.executed_steps != (), "run 1 should execute every step"
    assert snap1.cache_state != (), "run 1 should populate the cache"
    assert snap1.result_fingerprints, "run 1 should produce results"

    clear_trace(virtual_runtime)

    snap2 = capture_run(virtual_runtime, task)

    assert snap2.executed_steps == (), (
        f"run 2 should execute zero steps; got {snap2.executed_steps}"
    )
    assert snap2.cache_state == snap1.cache_state, (
        "run 2 should not write new cache entries"
    )

    assert snap2.result_fingerprints == snap1.result_fingerprints, (
        "run 2 fingerprints diverged from run 1 — cache served stale bytes"
    )
