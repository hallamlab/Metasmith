"""End-to-end contrast test: the canonical before/after pin.

Runs ``linear_3step`` twice and asserts the difference between the two
RunSnapshots is exactly the difference caching introduces:

  before  (run 1):  cache miss path  — every step executes, cache populated
  after   (run 2):  cache hit path   — no step executes, cache unchanged
  fingerprints are identical between the two runs

This test reads as a behavioral spec — anyone reviewing the caching
branch can open this file and see what "before" looked like vs "after."
Xfail-strict until all of S1-S5 land, then the marker is removed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.metasmith.cache._cache_harness import capture_run, clear_trace
from tests.metasmith.cache.fixtures.cache_fixtures import linear_3step

# `virtual_runtime` fixture is provided project-wide by tests/conftest.py.


def test_full_caching_flow_end_to_end(tmp_path, virtual_runtime):
    # First run: cache miss everywhere.
    task = linear_3step.build_task(tmp_path)
    snap1 = capture_run(virtual_runtime, task)

    assert snap1.executed_steps != (), "run 1 should execute every step"
    assert snap1.cache_state != (), "run 1 should populate the cache"
    assert snap1.result_fingerprints, "run 1 should produce results"

    clear_trace(virtual_runtime)

    # Second run: cache hit everywhere.
    snap2 = capture_run(virtual_runtime, task)

    # The diff that caching introduces:
    assert snap2.executed_steps == (), (
        f"run 2 should execute zero steps; got {snap2.executed_steps}"
    )
    assert snap2.cache_state == snap1.cache_state, (
        "run 2 should not write new cache entries"
    )

    # The diff that caching does NOT introduce: results bytes are
    # identical to the original cache-populating run.
    assert snap2.result_fingerprints == snap1.result_fingerprints, (
        "run 2 fingerprints diverged from run 1 — cache served stale bytes"
    )
