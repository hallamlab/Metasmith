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

from tests.metasmith.cache._cache_harness import capture_run
from tests.metasmith.cache.fixtures.cache_fixtures import (
    linear_3step,
    parallel_then_group,
)


def _run_workspace(virtual_runtime) -> Path:
    runs = sorted((virtual_runtime.home / "runs").glob("*"))
    assert runs, "no run_dir under virtual_runtime.home/runs/"
    return runs[-1]


def _load_results_lib(workspace: Path) -> DataInstanceLibrary:
    results = workspace / "results"
    assert results.exists(), f"no results dir at {results}"
    return DataInstanceLibrary.Load(results, attach_trace=True)


def test_telemetry_e2e_summary_after_capture_run(tmp_path, virtual_runtime):
    task = linear_3step.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    lib = _load_results_lib(_run_workspace(virtual_runtime))

    s = lib.summary()
    assert s["schema_version"] == INVOCATION_EVENT_SCHEMA_VERSION
    assert s["counts"]["events"] >= 1, s
    assert s["counts"]["sessions"] == 1
    assert s["counts"]["instances"] >= 1
    assert lib.find_failures() == []


def test_telemetry_e2e_resume_yields_hits(tmp_path, virtual_runtime):
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
    sids = {e.session_id for e in events}
    assert len(sids) == 1
    s = lib.summary()
    assert s["time"]["last_session"] == next(iter(sids))


def test_telemetry_e2e_lineage_walk_parallel_then_group(tmp_path, virtual_runtime):
    task = parallel_then_group.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    lib = _load_results_lib(_run_workspace(virtual_runtime))

    invocations = lib.find_invocations()
    assert invocations, "no InvocationEvents emitted"
    statuses = {e.status for e in invocations}
    assert statuses <= {"promoted", "hit", "miss"}, statuses
    assert len(invocations) >= 3, (
        f"expected ≥3 events for parallel_then_group, got {len(invocations)}"
    )
    promoted_or_miss = [e for e in invocations if e.status in ("promoted", "miss")]
    tx_keys = {e.transform_key for e in promoted_or_miss}
    assert tx_keys and "" not in tx_keys, (
        f"every promote-side event must carry transform_key, got {tx_keys!r}"
    )
    empty_consumes = [
        (e.task_hash[:8], e.transform_key)
        for e in promoted_or_miss if not e.consumes
    ]
    assert not empty_consumes, (
        f"promote-side events with empty consumes: {empty_consumes!r}"
    )

    for inv in invocations:
        for pf in inv.produces:
            descendants = list(lib.walk_ancestors(pf.file_instance_id))
            assert len(descendants) <= 64, "walk_ancestors should terminate"
            break
        break

    non_leaf = [e for e in invocations if e.consumes]
    assert non_leaf, "expected ≥1 non-leaf event in parallel_then_group"
    target = max(non_leaf, key=lambda e: sum(len(v) for v in e.consumes.values()))
    assert target.produces, f"event {target.task_hash[:8]} has no produces"
    descendants = list(lib.walk_ancestors(target.produces[0].file_instance_id))
    assert len(descendants) >= 1, (
        f"walk_ancestors from {target.task_hash[:8]} yielded 0 ancestors; "
        f"consumes encoding likely regressed"
    )

    promoted = [e for e in invocations if e.status == "promoted"]
    if promoted:
        path_bearing = [
            (pf.path, pf.slot_id, pf.file_instance_id)
            for e in promoted for pf in e.produces
            if pf.path and pf.file_instance_id != pf.slot_id
        ]
        assert path_bearing, (
            "no promoted event carries a populated ProducedFile.path "
            "with per-file file_instance_id; C0.5 emission regressed"
        )


def _stage_logs_for_shard(cache_root: Path, key_hex: str, body: str = "ok\n") -> Path:
    shard = cache_root / key_hex[:2] / key_hex[2:]
    logs_dir = shard / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    for name in (".command.sh", ".command.out", ".command.err", ".command.log"):
        (logs_dir / name).write_text(body)
    return logs_dir


def test_telemetry_e2e_get_logs_of_resolves_shard(tmp_path, virtual_runtime):
    task = linear_3step.build_task(tmp_path)
    capture_run(virtual_runtime, task)
    workspace = _run_workspace(virtual_runtime)
    cache_root = virtual_runtime.home / "task_cache"
    assert cache_root.exists(), f"cache_root absent at {cache_root}"

    trace = workspace / "_metasmith" / "trace.jsonl"
    events = [
        json.loads(l)
        for l in trace.read_text().splitlines()
        if l.strip() and json.loads(l).get("event") != "session_start"
    ]
    assert events, "no events emitted to trace.jsonl"
    chosen = next((e for e in events if e.get("cache_key")), None)
    assert chosen is not None, "no event carries a cache_key"
    key_hex = chosen["cache_key"]
    _stage_logs_for_shard(cache_root, key_hex)

    lib = _load_results_lib(workspace)
    lib.set_cache_root(cache_root)

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


def test_telemetry_e2e_find_failures_picks_injected_fail(tmp_path, virtual_runtime):
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


def _non_sentinel_events(workspace: Path) -> list[InvocationEvent]:
    lib = _load_results_lib(workspace)
    return lib.find_invocations()


def test_consumes_values_parse_as_hex_lists(tmp_path, virtual_runtime):
    task = linear_3step.build_task(tmp_path, n_samples=3)
    capture_run(virtual_runtime, task)
    events = _non_sentinel_events(_run_workspace(virtual_runtime))
    assert events, "no events emitted"
    bad: list[tuple[str, str, str]] = []
    for ev in events:
        for slot_key, ids in ev.consumes.items():
            for iid in ids:
                try:
                    raw = bytes.fromhex(iid)
                except ValueError:
                    continue
                try:
                    inner = raw.decode("ascii")
                except UnicodeDecodeError:
                    continue
                if (
                    len(inner) > 16
                    and all(c in "0123456789abcdef" for c in inner)
                ):
                    bad.append((
                        ev.task_hash[:8], slot_key,
                        f"hex-of-hex inner='{inner[:24]}...' (len={len(inner)})",
                    ))
    assert not bad, f"consumes hex-of-hex violations: {bad!r}"


def test_produced_files_have_nonempty_path(tmp_path, virtual_runtime):
    task = linear_3step.build_task(tmp_path, n_samples=1)
    capture_run(virtual_runtime, task)
    capture_run(virtual_runtime, task)
    events = _non_sentinel_events(_run_workspace(virtual_runtime))
    empty = [
        (ev.task_hash[:8], ev.status, pf.dtype_key)
        for ev in events for pf in ev.produces if not pf.path
    ]
    assert not empty, f"ProducedFile.path empty on {empty!r}"


def test_cache_hit_file_id_minted_from_path(tmp_path, virtual_runtime):
    task = linear_3step.build_task(tmp_path, n_samples=1)
    capture_run(virtual_runtime, task)
    capture_run(virtual_runtime, task)
    events = _non_sentinel_events(_run_workspace(virtual_runtime))
    hit_events = [e for e in events if e.status == "hit"]
    assert hit_events, "no cache-hit events emitted"
    degenerate = [
        (ev.task_hash[:8], pf.slot_id[:12])
        for ev in hit_events for pf in ev.produces
        if pf.slot_id == pf.file_instance_id
    ]
    assert not degenerate, (
        f"cache-hit produces with slot_id==file_instance_id: {degenerate!r}"
    )


def test_dtype_key_matches_producer_not_consumer(tmp_path, virtual_runtime):
    task = linear_3step.build_task(tmp_path, n_samples=1)
    capture_run(virtual_runtime, task)
    workspace_cold = _run_workspace(virtual_runtime)
    promote_dtype_keys = {
        ev.task_hash: {pf.dtype_key for pf in ev.produces}
        for ev in _non_sentinel_events(workspace_cold)
        if ev.status == "promoted"
    }
    capture_run(virtual_runtime, task)
    workspace_warm = _run_workspace(virtual_runtime)
    hit_dtype_keys = {
        ev.task_hash: {pf.dtype_key for pf in ev.produces}
        for ev in _non_sentinel_events(workspace_warm)
        if ev.status == "hit"
    }
    mismatches: list[tuple[str, set, set]] = []
    for th, hk in hit_dtype_keys.items():
        pk = promote_dtype_keys.get(th)
        if pk is not None and pk != hk:
            mismatches.append((th[:8], pk, hk))
    assert not mismatches, (
        f"cache-hit dtype_key differs from promote for same task: {mismatches!r}"
    )


def test_step_name_populated_on_all_routes(tmp_path, virtual_runtime):
    task = linear_3step.build_task(tmp_path, n_samples=1)
    capture_run(virtual_runtime, task)
    capture_run(virtual_runtime, task)
    workspace = _run_workspace(virtual_runtime)
    trace_dir = workspace / "_metasmith"
    all_events: list[dict] = []
    for trace_path in sorted(trace_dir.glob("trace*.jsonl")):
        for line in trace_path.read_text().splitlines():
            if not line.strip():
                continue
            ev = json.loads(line)
            if ev.get("event") == "session_start":
                continue
            all_events.append(ev)
    assert all_events, "no non-sentinel events across rotated traces"
    statuses = {ev.get("status") for ev in all_events}
    assert {"promoted", "hit"} <= statuses, (
        f"need both promoted and hit events to detect route divergence; "
        f"got statuses={statuses!r}"
    )
    missing = [
        (ev.get("task_hash", "")[:8], ev.get("status"))
        for ev in all_events if not ev.get("step_name")
    ]
    assert not missing, f"events without step_name: {missing!r}"


def test_invocation_event_is_one_per_batch_not_per_step(tmp_path, virtual_runtime):
    N = 3
    task = linear_3step.build_task(tmp_path, n_samples=N)
    capture_run(virtual_runtime, task)
    events = _non_sentinel_events(_run_workspace(virtual_runtime))
    step1_events = [
        e for e in events
        if e.step_order == 1 and e.status in ("promoted", "miss")
    ]
    assert len(step1_events) == N, (
        f"expected {N} per-batch events for step 1 with {N} samples, "
        f"got {len(step1_events)} — emission still step-aggregated"
    )
    step1_consume_sets = [
        tuple(sorted((k, tuple(sorted(v))) for k, v in e.consumes.items()))
        for e in step1_events
    ]
    assert len(set(step1_consume_sets)) == N, (
        f"expected {N} distinct per-batch consumes; got {len(set(step1_consume_sets))} "
        f"duplicates — batches share consumes (aggregation)"
    )


def test_linear_3step_n_samples_yields_n_direct_parent_pairs(tmp_path, virtual_runtime):
    N = 3
    task = linear_3step.build_task(tmp_path, n_samples=N)
    capture_run(virtual_runtime, task)
    lib = _load_results_lib(_run_workspace(virtual_runtime))

    pairs = list(lib.Trace("cf::step_a", "cf::seed"))
    assert len(pairs) == N, (
        f"expected exactly {N} (step_a, seed) pairs for {N} samples, "
        f"got {len(pairs)} — likely cross-sample contamination "
        f"(N² aggregate or missing per-batch routing)"
    )
    seeds = {to_inst.path for _, to_inst in pairs}
    assert len(seeds) == N, (
        f"expected {N} distinct seeds, got {len(seeds)}: {seeds!r}"
    )
