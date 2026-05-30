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


@pytest.mark.xfail(strict=True, reason="S3 hit-path trace emission not landed")
def test_trace_jsonl_records_hit_and_run(tmp_path, virtual_runtime):
    """G11: per-task trace.jsonl records `source: hit|run` and step name.

    Run once (all runs), run again (all hits). The second run's
    trace.jsonl must have exactly N rows for an N-step plan, each with
    `source: hit`. Critical that this is two-pass: compile-time emits
    `source: hit` rows; post-exec emits `source: run` rows. Both reach
    the file.
    """
    task = linear_3step.build_task(tmp_path / "run1")
    capture_run(virtual_runtime, task)
    task2 = linear_3step.build_task(tmp_path / "run2")
    capture_run(virtual_runtime, task2)

    # Find the latest run_dir's trace.jsonl
    runs = sorted((virtual_runtime.home / "runs").glob("*/_metasmith/trace.jsonl"))
    assert runs, "no trace.jsonl emitted under any run_dir"
    lines = [json.loads(l) for l in runs[-1].read_text().splitlines() if l.strip()]
    sources = [r.get("source") for r in lines]
    assert sources, "trace.jsonl is empty"
    assert all(s == "hit" for s in sources), (
        f"second run expected all hits, got {sources}"
    )


def test_msm_status_joins_meta(tmp_path, virtual_runtime, capsys):
    """G11: `msm status <run_dir>` joins trace + workflow.step_N.meta.

    Pinned to S8. After a run, status_run reads any trace.jsonl rows
    that already exist AND parses every `workflow.step_N.meta` file
    deposited by the compile pass — the join gives one entry per task
    keyed by step order with the stable cache_key + cacheable fields.
    """
    from metasmith.ops.cache import status_run

    task = linear_3step.build_task(tmp_path / "run1")
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
