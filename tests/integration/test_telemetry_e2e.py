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
    # C0: promote-side events must carry direct-parent `consumes`,
    # mirroring the cache-hit route (workflow.py:1419-1422). Every
    # transform in parallel_then_group has ≥1 required dep, so every
    # promote-side event should report a non-empty consumes dict.
    empty_consumes = [
        (e.task_hash[:8], e.transform_key)
        for e in promoted_or_miss if not e.consumes
    ]
    assert not empty_consumes, (
        f"promote-side events with empty consumes: {empty_consumes!r}"
    )

    # walk_ancestors from any produced file_instance_id terminates with a
    # bounded number of yields (no cycle blowup).
    for inv in invocations:
        for pf in inv.produces:
            descendants = list(lib.walk_ancestors(pf.file_instance_id))
            assert len(descendants) <= 64, "walk_ancestors should terminate"
            break
        break

    # C0-amend: walk_ancestors must yield ≥1 result from a non-leaf
    # event. Pre-amend, consumes carried `hex(utf8("+".join(hexes)))`
    # garbage that no TraceIndex lookup could resolve, so the walk
    # silently produced zero ancestors. The B and C events in
    # parallel_then_group both have ≥1 input dep — pick the event with
    # the largest consumes set and walk from one of its produces.
    non_leaf = [e for e in invocations if e.consumes]
    assert non_leaf, "expected ≥1 non-leaf event in parallel_then_group"
    target = max(non_leaf, key=lambda e: sum(len(v) for v in e.consumes.values()))
    assert target.produces, f"event {target.task_hash[:8]} has no produces"
    descendants = list(lib.walk_ancestors(target.produces[0].file_instance_id))
    assert len(descendants) >= 1, (
        f"walk_ancestors from {target.task_hash[:8]} yielded 0 ancestors; "
        f"consumes encoding likely regressed"
    )

    # C0.5: promote-side ProducedFile.path must be populated, and
    # file_instance_id must diverge from slot_id (per-file mint via
    # LinPayload.mint_file_id). At least one promoted event needs
    # ≥1 produces with a non-empty path AND a file_instance_id !=
    # slot_id. Cache-hit events on the first run match promote on
    # the same task; legacy fallback events emit path="" with
    # file_instance_id == slot_id, which would fail this check.
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


# ---------------------------------------------------------------------------
# S2 — Red invariant tests gating C2 (plans/lineage-quadrant-audit.md, G3/G7)
#
# Each test pins one shape invariant the C2 endgame depends on. All are
# currently red on HEAD (88b4d9c) and xfail-marked with the plan step that
# turns them green. Remove the marker in that step's commit.
# ---------------------------------------------------------------------------


def _non_sentinel_events(workspace: Path) -> list[InvocationEvent]:
    """Load InvocationEvents (no SessionStart) via the library."""
    lib = _load_results_lib(workspace)
    return lib.find_invocations()


@pytest.mark.xfail(
    reason="I1 — Bug A: consumes values are hex-of-hex (iid_string.encode().hex()) "
    "instead of plain 32-byte hex. Fixed in S4.",
    strict=False,
)
def test_consumes_values_parse_as_hex_lists(tmp_path, virtual_runtime):
    """I1: every consumes[k][i] decodes as plain hex of len 32 (a slot_id).

    Bug A: workflow.py:~1279-1290 packs sorted_inputs as a `+`-joined
    bytes object; the value persisted to step_N.meta and re-emitted in
    consumes is the hex of that ASCII-encoded hex string — one extra
    encoding layer. Decode once: you get a hex string. Decode twice:
    you get the bytes. The BFS in agents.py walks consumes looking
    for file_instance_id (plain hex) and finds nothing.
    """
    task = linear_3step.build_task(tmp_path, n_samples=3)
    capture_run(virtual_runtime, task)
    events = _non_sentinel_events(_run_workspace(virtual_runtime))
    assert events, "no events emitted"
    bad: list[tuple[str, str, str]] = []
    for ev in events:
        for slot_key, ids in ev.consumes.items():
            for iid_hex in ids:
                try:
                    raw = bytes.fromhex(iid_hex)
                except ValueError:
                    bad.append((ev.task_hash[:8], slot_key, "not-hex"))
                    continue
                # A 32-byte file_instance_id => 64 hex chars. iid format
                # used in the codebase is 32 bytes + 1-byte prefix => 66
                # hex chars. Anything longer is the hex-of-hex bug.
                if len(iid_hex) > 80:
                    bad.append((ev.task_hash[:8], slot_key, f"len={len(iid_hex)}"))
                # If decoded bytes are ASCII-printable hex, that's the bug.
                try:
                    inner = raw.decode("ascii")
                    if all(c in "0123456789abcdef" for c in inner) and len(inner) > 16:
                        bad.append((
                            ev.task_hash[:8], slot_key,
                            f"hex-of-hex inner_len={len(inner)}",
                        ))
                except UnicodeDecodeError:
                    pass
    assert not bad, f"consumes hex-of-hex violations: {bad!r}"


@pytest.mark.xfail(
    reason="I2 — Bug B: cache-hit emits produces[].path=''; C0.5 only fixed "
    "the promote route. Fixed in S4.",
    strict=False,
)
def test_produced_files_have_nonempty_path(tmp_path, virtual_runtime):
    """I2: every produces[].path on every event is a non-empty string.

    Bug B: workflow.py:1412-1498 (cache-hit emission) constructs
    ProducedFile with path=''. C0.5 (72e1715) plumbed paths through
    the promote route only.
    """
    task = linear_3step.build_task(tmp_path, n_samples=1)
    # First run: promote events (path populated).
    capture_run(virtual_runtime, task)
    # Second run: cache-hit events (path empty today).
    capture_run(virtual_runtime, task)
    events = _non_sentinel_events(_run_workspace(virtual_runtime))
    empty = [
        (ev.task_hash[:8], ev.status, pf.dtype_key)
        for ev in events for pf in ev.produces if not pf.path
    ]
    assert not empty, f"ProducedFile.path empty on {empty!r}"


@pytest.mark.xfail(
    reason="I3 — Bug C: cache-hit emits slot_id == file_instance_id (degenerate); "
    "should mint per-file via LinPayload.mint_file_id. Fixed in S4.",
    strict=False,
)
def test_cache_hit_file_id_minted_from_path(tmp_path, virtual_runtime):
    """I3: on cache-hit events, file_instance_id != slot_id (per-file mint).

    Bug C: workflow.py:1412-1498 cache-hit emission passes slot_id as
    file_instance_id. Promote (C0.5) correctly mints
    LinPayload.mint_file_id(slot_id, relpath). The values should
    diverge whenever there is a non-empty path.
    """
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


@pytest.mark.xfail(
    reason="I4 — Bug D: cache-hit produces[].dtype_key is the downstream consumer's "
    "dep_key, not the producer's dtype.key. Fixed in S4.",
    strict=False,
)
def test_dtype_key_matches_producer_not_consumer(tmp_path, virtual_runtime):
    """I4: produces[].dtype_key on cache-hit matches the promote-side dtype_key for
    the same task.

    Bug D: warm-run audit (linear_3step) showed cache-hit step 1
    emits dtype_key='MbSRYjOi' (step 2's consume key) where promote
    step 1 correctly emits dtype_key='IA33yeXE'. The producer's
    dtype.key is the correct value.
    """
    task = linear_3step.build_task(tmp_path, n_samples=1)
    # Run 1: cold (promote)
    capture_run(virtual_runtime, task)
    workspace_cold = _run_workspace(virtual_runtime)
    promote_dtype_keys = {
        ev.task_hash: {pf.dtype_key for pf in ev.produces}
        for ev in _non_sentinel_events(workspace_cold)
        if ev.status == "promoted"
    }
    # Run 2: warm (hits) — same workspace, additional events appended.
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


@pytest.mark.xfail(
    reason="I5 — Bug E: step_name only emitted on cache-hit; missing on promote. "
    "Fixed in S4.",
    strict=False,
)
def test_step_name_populated_on_all_routes(tmp_path, virtual_runtime):
    """I5: every event (promote AND hit) carries a non-empty step_name.

    Cosmetic but symptomatic of route divergence — emission schema
    should be byte-identical between promote and cache-hit routes.
    Reads the rotated trace.1.jsonl too (cold-run promote events end up
    there after the second `capture_run` rotates the trace).
    """
    task = linear_3step.build_task(tmp_path, n_samples=1)
    capture_run(virtual_runtime, task)  # cold: promote events
    capture_run(virtual_runtime, task)  # warm: hit events
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


@pytest.mark.xfail(
    reason="I6 — C2 structural defect: promote/cache-hit emit one event per step, "
    "aggregating batches. Fixed in S4 (per-batch emission loop).",
    strict=False,
)
def test_invocation_event_is_one_per_batch_not_per_step(tmp_path, virtual_runtime):
    """I6: for linear_3step(n_samples=N), non-sentinel events == n_steps * N.

    Today promote_run (promote.py:386-561) and _compute_cache_decisions
    (workflow.py:1412-1498) emit ONE event per step regardless of
    batch count. N=3 samples → 3 events (one per step) where the trace
    semantics demand 9 (one per task = batch). This aggregation hides
    cross-sample lineage contamination because every batch's parents
    are lumped into the same consumes dict.
    """
    N = 3
    task = linear_3step.build_task(tmp_path, n_samples=N)
    capture_run(virtual_runtime, task)
    events = _non_sentinel_events(_run_workspace(virtual_runtime))
    n_steps = 3  # linear_3step has 3 transforms
    expected = n_steps * N
    assert len(events) == expected, (
        f"expected {expected} per-batch events ({n_steps} steps × {N} samples), "
        f"got {len(events)} (likely step-aggregated)"
    )


@pytest.mark.xfail(
    reason="I7 — C2 structural: cross-sample lineage purity. With step-aggregated "
    "consumes + _manifests fallback present today, this may pass; once "
    "_manifests is deleted (S6) it breaks N²-style without S4. Fixed in S4.",
    strict=False,
)
def test_linear_3step_n_samples_yields_exactly_n_parent_pairs(tmp_path, virtual_runtime):
    """I7: lib.Trace('cf::step_c', 'cf::seed') yields exactly N pairs for N samples.

    This is the C2 structural gate: with step-aggregated consumes,
    walking back from any step_c output reaches every seed (cartesian
    N×N). With per-batch consumes, each step_c output reaches exactly
    one seed (the one it descends from).

    Today, the _manifests/*.json fallback (agents.py:1291-1318) means
    Trace can sometimes find the right pairs even when consumes is
    aggregated — manifests carry per-file ancestry. Once _manifests is
    deleted (S6), this test catches the contamination.
    """
    N = 3
    task = linear_3step.build_task(tmp_path, n_samples=N)
    capture_run(virtual_runtime, task)
    lib = _load_results_lib(_run_workspace(virtual_runtime))

    pairs = list(lib.Trace("cf::step_c", "cf::seed"))
    assert len(pairs) == N, (
        f"expected exactly {N} (step_c, seed) pairs for {N} samples, "
        f"got {len(pairs)} — likely cross-sample contamination (N² or N+aggregate)"
    )
    # Stronger check: each step_c output should pair with a *distinct* seed.
    seeds = {to_inst.path for _, to_inst in pairs}
    assert len(seeds) == N, f"expected {N} distinct seeds, got {len(seeds)}: {seeds!r}"
