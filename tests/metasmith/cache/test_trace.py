from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.metasmith.cache._cache_harness import capture_run
from tests.metasmith.cache.fixtures.cache_fixtures import linear_3step


def test_trace_jsonl_records_v2_invocation_events(tmp_path, virtual_runtime):
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
    from metasmith.ops.cache import status_run

    task = linear_3step.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    key = task.GetKey()
    run_dir = virtual_runtime.home / "runs" / key
    assert run_dir.exists(), f"run_dir not found at {run_dir}"

    result = status_run(str(run_dir))
    assert result["run_dir"] == str(run_dir)
    assert result["meta"], "no workflow.step_*.meta files were joined"
    for order, body in result["meta"].items():
        assert "cache_key" in body, f"step {order} meta missing cache_key"
        assert "cacheable" in body, f"step {order} meta missing cacheable"
