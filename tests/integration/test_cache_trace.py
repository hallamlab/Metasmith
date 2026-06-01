"""Forward-looking xfail tests for the resume-trace UX (inbox #14).

Coverage: G11 — `_metasmith/trace.jsonl` records hit/run per task;
`msm status <key>` renders it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.integration._cache_harness import capture_run
from tests.integration.fixtures.cache_fixtures import linear_3step

from tests.e2e_virtual.conftest import virtual_runtime  # noqa: F401


def test_trace_jsonl_records_v2_invocation_events(tmp_path, virtual_runtime):
    """C7: trace.jsonl carries v2 InvocationEvent rows + SessionStart sentinel.

    Run once (all promoted), run again (all hit). The second run's
    trace.jsonl must lead with a SessionStart sentinel and then carry
    exactly N InvocationEvent rows for an N-step plan, each with
    `status: hit` and `schema_version: 2`. Compile-time emits the hit
    rows; post-exec promote.py appends miss/promoted/fail rows carrying
    the same `session_id` recovered from the sentinel.
    """
    task = linear_3step.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    capture_run(virtual_runtime, task)

    runs = sorted((virtual_runtime.home / "runs").glob("*/_metasmith/trace.jsonl"))
    assert runs, "no trace.jsonl emitted under any run_dir"
    raw = [json.loads(l) for l in runs[-1].read_text().splitlines() if l.strip()]
    assert raw, "trace.jsonl is empty"

    head = raw[0]
    assert head.get("event") == "session_start", (
        f"first line must be SessionStart sentinel, got {head}"
    )
    assert "session_id" in head
    session_id = head["session_id"]

    events = raw[1:]
    assert events, "no InvocationEvent rows after SessionStart"
    for row in events:
        assert row.get("schema_version") == 2, (
            f"non-v2 row in trace.jsonl: {row}"
        )
        assert row.get("session_id") == session_id, (
            f"session_id drift between sentinel ({session_id}) and event {row}"
        )
        assert row.get("task_hash"), f"row missing task_hash: {row}"
    statuses = [r.get("status") for r in events]
    assert all(s == "hit" for s in statuses), (
        f"second run expected all hit statuses, got {statuses}"
    )


def test_msm_status_joins_meta(tmp_path, virtual_runtime, capsys):
    """G11: `msm status <run_dir>` joins trace + workflow.step_N.meta.

    Pinned to S8. After a run, status_run reads any trace.jsonl rows
    that already exist AND parses every `workflow.step_N.meta` file
    deposited by the compile pass — the join gives one entry per task
    keyed by step order with the stable cache_key + cacheable fields.
    """
    from metasmith.ops.cache import status_run

    task = linear_3step.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    key = task.GetKey()
    run_dir = virtual_runtime.home / "runs" / key
    assert run_dir.exists(), f"run_dir not found at {run_dir}"

    result = status_run(str(run_dir))
    assert result["run_dir"] == str(run_dir)
    # Every cacheable step deposits a `workflow.step_N.meta` file with
    # cache_key + cacheable lines; status_run must surface them keyed
    # by step order.
    assert result["meta"], "no workflow.step_*.meta files were joined"
    for order, body in result["meta"].items():
        assert "cache_key" in body, f"step {order} meta missing cache_key"
        assert "cacheable" in body, f"step {order} meta missing cacheable"
