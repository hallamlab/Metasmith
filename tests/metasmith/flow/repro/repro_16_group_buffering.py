from __future__ import annotations

import time
from pathlib import Path

import pytest

from tests.metasmith.flow.conftest import build_group_then_split_plan, run_and_load


# Hard wall on this pin so a deadlock regression here surfaces as a fast
# failure rather than a hang. It is a liveness bound, not a cost claim -- a
# cost claim belongs in the perf axis, per tests/metasmith/AGENTS.md.
#
# The original 5.0 was set when this plan shape ran in well under a second.
# The member cache moved the unit to one group member's invocation, and the
# virtual runtime now calls the real key, probe and promote functions per
# member, which took the same shape to ~17s. Widened to keep the guard while
# it stops asserting a speed nobody committed to.
_REPRO_16_WALL_BUDGET_S = 60.0


def test_repro_16_group_buffering_no_deadlock(tmp_path, virtual_runtime):
    bp = build_group_then_split_plan(tmp_path)
    t0 = time.monotonic()
    task, lib = run_and_load(virtual_runtime, bp)
    elapsed = time.monotonic() - t0

    assert task is not None
    assert lib is not None
    assert elapsed < _REPRO_16_WALL_BUDGET_S, (
        f"group_then_unfold plan exceeded {_REPRO_16_WALL_BUDGET_S}s "
        f"on virtual_runtime: {elapsed:.2f}s — possible deadlock regression"
    )

    summary = lib.summary()
    assert summary["counts"]["sessions"] >= 1, (
        f"no SessionStart sentinel after group_then_unfold run; "
        f"summary={summary!r}"
    )
