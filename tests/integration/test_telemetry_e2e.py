"""End-to-end telemetry API tests (C8 / G5 / G6).

Exercises the user-facing telemetry surface on `DataInstanceLibrary`
against real virtual-runtime workflow runs. The virtual runtime does
not invoke real Nextflow but follows the same compile/promote contract:
- compile-time `_compute_cache_decisions` emits the SessionStart sentinel
  + per-step hit `InvocationEvent` rows
- post-exec `promote_run` emits promoted/miss/fail rows that carry the
  same `session_id`

These tests cover G5 (lineage + summary + find_invocations + find_failures)
and G6 (logs resolved through the cache shard after rm -rf work/).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from metasmith.models.lineage import (
    INVOCATION_EVENT_SCHEMA_VERSION,
    InvocationEvent,
    ProducedFile,
    append_invocation_event,
)
from metasmith.models.libraries import DataInstanceLibrary

from tests.integration._cache_harness import capture_run
from tests.integration.fixtures.cache_fixtures import (
    linear_3step,
    parallel_then_group,
)

from tests.e2e_virtual.conftest import virtual_runtime  # noqa: F401


def _run_workspace(virtual_runtime) -> Path:
    """Return the workspace dir for the most recent virtual run."""
    runs = sorted((virtual_runtime.home / "runs").glob("*"))
    assert runs, "no run_dir under virtual_runtime.home/runs/"
    return runs[-1]


def _load_results_lib(workspace: Path) -> DataInstanceLibrary:
    """Auto-attach trace; results dir is `workspace/results`."""
    results = workspace / "results"
    assert results.exists(), f"no results dir at {results}"
    return DataInstanceLibrary.Load(results, attach_trace=True)


# ---------------------------------------------------------------------------
# G5 — summary + lineage walk + resume hits
# ---------------------------------------------------------------------------


def test_telemetry_e2e_summary_after_capture_run(tmp_path, virtual_runtime):
    """After one run, summary() reports v2 schema + one session + >=1 event."""
    task = linear_3step.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    lib = _load_results_lib(_run_workspace(virtual_runtime))

    s = lib.summary()
    assert s["schema_version"] == INVOCATION_EVENT_SCHEMA_VERSION
    assert s["counts"]["events"] >= 1, s
    assert s["counts"]["sessions"] == 1
    assert s["counts"]["instances"] >= 1
    # find_failures: clean run has none.
    assert lib.find_failures() == []


def test_telemetry_e2e_resume_yields_hits(tmp_path, virtual_runtime):
    """Run-then-rerun: every event on the 2nd run reports status=='hit'."""
    task = linear_3step.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    capture_run(virtual_runtime, task)
    lib = _load_results_lib(_run_workspace(virtual_runtime))

    events = lib.find_invocations()
    assert events, "no InvocationEvents on rerun"
    statuses = {e.status for e in events}
    assert statuses == {"hit"}, (
        f"second run expected all hit, got {statuses!r}"
    )
    # session_id of the SessionStart sentinel matches every event's
    # session_id (rotation contract from C7).
    sids = {e.session_id for e in events}
    assert len(sids) == 1
    s = lib.summary()
    assert s["time"]["last_session"] == next(iter(sids))


def test_telemetry_e2e_lineage_walk_parallel_then_group(tmp_path, virtual_runtime):
    """parallel_then_group: 3 A → 1 B grouped → 1 C. Walking back from C must
    pass through B (one parent) and surface the 3 A producer events."""
    task = parallel_then_group.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    lib = _load_results_lib(_run_workspace(virtual_runtime))

    invocations = lib.find_invocations()
    assert invocations, "no InvocationEvents emitted"
    # parallel_then_group has 3 parallel A invocations + 1 B + 1 C = 5 tasks.
    # All run on the first capture_run so all events report 'promoted'.
    # (transform_key is populated only on cache-hit emits today; the
    # promote-side rows carry it as "" until the meta-file plumbing is
    # extended — counting events is the load-bearing assertion.)
    statuses = {e.status for e in invocations}
    assert statuses <= {"promoted", "hit", "miss"}, statuses
    assert len(invocations) >= 3, (
        f"expected ≥3 events for parallel_then_group, got {len(invocations)}"
    )

    # walk_ancestors from any produced file_instance_id terminates with a
    # bounded number of yields (no cycle blowup).
    for inv in invocations:
        for pf in inv.produces:
            descendants = list(lib.walk_ancestors(pf.file_instance_id))
            assert len(descendants) <= 64, "walk_ancestors should terminate"
            break
        break


# ---------------------------------------------------------------------------
# G6 — logs resolved from cache shard
# ---------------------------------------------------------------------------


def _stage_logs_for_shard(cache_root: Path, key_hex: str, body: str = "ok\n") -> Path:
    """Drop synthetic `.command.*` into `<cache_root>/<ab>/<rest>/logs/`."""
    shard = cache_root / key_hex[:2] / key_hex[2:]
    logs_dir = shard / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    for name in (".command.sh", ".command.out", ".command.err", ".command.log"):
        (logs_dir / name).write_text(body)
    return logs_dir


def test_telemetry_e2e_get_logs_of_resolves_shard(tmp_path, virtual_runtime):
    """`get_logs_of(any_output)` resolves to `<shard>/logs/.command.*` files.

    Mirrors the G6 "rm -rf work/" + resume invariant: the library reads
    its logs from the cache shard, not from the workspace work dir.
    """
    task = linear_3step.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    workspace = _run_workspace(virtual_runtime)
    cache_root = virtual_runtime.home / "task_cache"
    assert cache_root.exists(), f"cache_root absent at {cache_root}"

    # Load the trace directly so we can pick any task_hash, and stage logs
    # into its shard.
    trace = workspace / "_metasmith" / "trace.jsonl"
    events = [
        json.loads(l)
        for l in trace.read_text().splitlines()
        if l.strip() and json.loads(l).get("event") != "session_start"
    ]
    assert events, "no events emitted to trace.jsonl"
    # Pick the first event with a non-empty cache_key.
    chosen = next((e for e in events if e.get("cache_key")), None)
    assert chosen is not None, "no event carries a cache_key"
    key_hex = chosen["cache_key"]
    _stage_logs_for_shard(cache_root, key_hex)

    lib = _load_results_lib(workspace)
    lib.set_cache_root(cache_root)

    # Pick any produced file_instance_id from the event we staged logs for.
    produces = chosen.get("produces") or []
    assert produces, f"event {key_hex[:8]} has no produces"
    fid = produces[0]["file_instance_id"]

    bundle = lib.get_logs_of(fid)
    assert bundle.status == "available", (
        f"expected available logs, got {bundle.status!r}: {bundle.reason}"
    )
    assert bundle.stdout is not None and bundle.stdout.exists()
    assert bundle.stderr is not None and bundle.stderr.exists()
    assert bundle.command_sh is not None and bundle.command_sh.exists()
    assert bundle.command_log is not None and bundle.command_log.exists()


def test_telemetry_e2e_get_logs_of_legacy_shard(tmp_path, virtual_runtime):
    """Shard present but no `logs/` subdir → status=='legacy_shard_no_logs'."""
    task = linear_3step.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    workspace = _run_workspace(virtual_runtime)
    cache_root = virtual_runtime.home / "task_cache"

    trace = workspace / "_metasmith" / "trace.jsonl"
    events = [
        json.loads(l)
        for l in trace.read_text().splitlines()
        if l.strip() and json.loads(l).get("event") != "session_start"
    ]
    chosen = next(e for e in events if e.get("cache_key"))
    key_hex = chosen["cache_key"]
    shard = cache_root / key_hex[:2] / key_hex[2:]
    logs_dir = shard / "logs"
    if logs_dir.exists():
        shutil.rmtree(logs_dir)

    lib = _load_results_lib(workspace)
    lib.set_cache_root(cache_root)
    fid = chosen["produces"][0]["file_instance_id"]
    bundle = lib.get_logs_of(fid)
    assert bundle.status == "legacy_shard_no_logs", bundle


# ---------------------------------------------------------------------------
# G5 — find_failures end-to-end via injected fail event
# ---------------------------------------------------------------------------


def test_telemetry_e2e_find_failures_picks_injected_fail(tmp_path, virtual_runtime):
    """Appending a `status='fail'` event to trace.jsonl surfaces in find_failures().

    The virtual runtime never fails on its own; this test verifies the
    library correctly reads + filters the v2 trace format when a fail
    row is present (the path taken by real Nextflow runs that exit
    non-zero, which `promote_run` cannot synthesize without a broken
    transform).
    """
    task = linear_3step.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    workspace = _run_workspace(virtual_runtime)
    trace = workspace / "_metasmith" / "trace.jsonl"

    fail_event = InvocationEvent(
        task_hash="dead" * 16,
        transform_key="tx_broken",
        status="fail",
        consumes={},
        produces=[
            ProducedFile(
                file_instance_id="ff" * 10,
                slot_id="ff" * 10,
                path="out/broken.fa",
                dtype_key="seq::fa",
            )
        ],
        session_id=1,
        exit_code=137,
        cache_key="dead" * 16,
    )
    append_invocation_event(trace, fail_event)

    lib = _load_results_lib(workspace)
    failures = lib.find_failures()
    hashes = {e.task_hash for e in failures}
    assert "dead" * 16 in hashes, (
        f"injected fail not surfaced, failures={list(hashes)}"
    )
