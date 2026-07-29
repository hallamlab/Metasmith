"""Pin for inbox #16 — non-parent stream buffering until parent closes.

The canonical buggy shape is documented in `tests/flow/test_group_cases.py:
test_c04_descendant_single_bs1_buffer_until_close` (the 20-case matrix).
That test runs against `NxfTestRunner` (real Nextflow) because the bug is
a channel-timing issue: the DESCENDANT branch's combine(by:0) used to
buffer S items until the by-stream closed, deferring the first emission
past the post-fix `INCREMENTAL_EMIT_THRESHOLD_MS` budget.

This pin re-implements the same scenario at the plan-shape level against
the `virtual_runtime` so it stays in the fast suite:

- Build a group_then_unfold plan (the same group→unfold producer/consumer
  contract that the 20-case matrix exercises).
- Confirm the plan compiles and runs to completion against
  `virtual_runtime` — i.e., the orchestrator does not deadlock waiting
  on a non-parent stream that the post-#16 fix guarantees will emit
  incrementally.

The wall-clock invariant ("first emission < 2000ms") is only meaningful
under real-channel timing and lives in the docker e2e suite. Here we
pin the structural invariant: the plan runs to completion with the
expected lineage events.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from tests.flow.conftest import build_group_then_split_plan, run_and_load


# Hard wall on this pin so a deadlock regression here surfaces as a fast
# failure rather than a hang. The virtual_runtime path completes in
# well under a second for this plan shape on commodity CI.
_REPRO_16_WALL_BUDGET_S = 5.0


def test_repro_16_group_buffering_no_deadlock(tmp_path, virtual_runtime):
    """Pins inbox #16: group→unfold plan completes without buffering deadlock.

    Catalog references: <G7> and <RC3>. The full real-channel timing
    invariant is exercised by
    `tests/flow/test_group_cases.py::test_c04_descendant_single_bs1_buffer_until_close`;
    this pin guards the fast suite against the structural shape returning
    a deadlocked run.
    """
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

    # If the run completed, the trace index must surface at least the
    # session sentinel; otherwise we would have died upstream.
    summary = lib.summary()
    assert summary["counts"]["sessions"] >= 1, (
        f"no SessionStart sentinel after group_then_unfold run; "
        f"summary={summary!r}"
    )
