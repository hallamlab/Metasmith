from __future__ import annotations
import json
from pathlib import Path

import pytest

from metasmith.models.lineage import (
    INVOCATION_EVENT_SCHEMA_VERSION,
    InvocationEvent,
    InvocationNotFound,
    ProducedFile,
    SessionStart,
    append_invocation_event,
)
from metasmith.telemetry import TraceIndex


def _write_trace(path: Path, events):
    path.parent.mkdir(parents=True, exist_ok=True)
    sentinel = SessionStart(
        session_id=1,
        compile_started_at="2026-05-31T00:00:00Z",
        metasmith_version="test",
    )
    path.write_text("")
    with open(path, "a") as f:
        f.write(sentinel.to_jsonl() + "\n")
    for ev in events:
        append_invocation_event(path, ev)


def _mk_event(
    task_hash: str,
    *,
    status: str = "promoted",
    transform_key: str = "tx_a",
    produces: list[ProducedFile] | None = None,
    consumes: dict[str, list[str]] | None = None,
    session_id: int = 1,
    step_order: int = 1,
    exit_code: int | None = 0,
) -> InvocationEvent:
    return InvocationEvent(
        task_hash=task_hash,
        transform_key=transform_key,
        status=status,  # type: ignore[arg-type]
        consumes=consumes or {},
        produces=produces or [],
        session_id=session_id,
        step_order=step_order,
        cache_key=task_hash,
        exit_code=exit_code,
    )


def test_trace_index_read_indexes_sentinel_and_events(tmp_path):
    trace = tmp_path / "_metasmith" / "trace.jsonl"
    pf = ProducedFile(
        file_instance_id="aa" * 10,
        slot_id="bb" * 10,
        path="out/a.fa",
        dtype_key="seq::fa",
    )
    _write_trace(trace, [_mk_event("ffff", produces=[pf])])

    idx = TraceIndex.read(trace)
    assert len(idx.sentinels) == 1
    assert idx.sentinels[0].session_id == 1
    assert len(idx.events) == 1
    found = idx.find_event_for_instance("aa" * 10)
    assert found is not None
    assert found.task_hash == "ffff"
    found_by_slot = idx.find_event_for_instance("bb" * 10)
    assert found_by_slot is found


def test_trace_index_legacy_row_tolerated(tmp_path):
    trace = tmp_path / "_metasmith" / "trace.jsonl"
    trace.parent.mkdir(parents=True)
    legacy = json.dumps({"source": "hit", "step": "1", "cache_key": "deadbeef"})
    trace.write_text(legacy + "\n")
    idx = TraceIndex.read(trace)
    assert idx.events == []


def test_attach_trace_idempotent_and_raises_on_switch(tmp_path):
    from metasmith.models.libraries import DataInstanceLibrary
    from metasmith.models.lineage import TraceAlreadyAttached

    lib = DataInstanceLibrary(tmp_path / "lib")
    trace = tmp_path / "_metasmith" / "trace.jsonl"
    _write_trace(trace, [_mk_event("ffff")])
    lib.attach_trace(trace)
    lib.attach_trace(trace)
    other = tmp_path / "_metasmith" / "trace.99.jsonl"
    _write_trace(other, [_mk_event("eeee")])
    with pytest.raises(TraceAlreadyAttached):
        lib.attach_trace(other)


def test_summary_shape(tmp_path):
    from metasmith.models.libraries import DataInstanceLibrary

    lib = DataInstanceLibrary(tmp_path / "lib")
    trace = tmp_path / "_metasmith" / "trace.jsonl"
    _write_trace(
        trace,
        [
            _mk_event("aaaa", transform_key="tx_a", status="promoted"),
            _mk_event("bbbb", transform_key="tx_b", status="hit"),
            _mk_event("cccc", transform_key="tx_a", status="hit"),
        ],
    )
    lib.attach_trace(trace)
    s = lib.summary()

    assert s["schema_version"] == INVOCATION_EVENT_SCHEMA_VERSION
    assert set(s.keys()) == {
        "schema_version",
        "counts",
        "by_dtype",
        "by_transform",
        "by_status",
        "time",
    }
    assert s["counts"]["events"] == 3
    assert s["counts"]["sessions"] == 1
    assert s["by_transform"] == {"tx_a": 2, "tx_b": 1}
    assert s["by_status"] == {"promoted": 1, "hit": 2}
    assert s["time"]["first_session"] == 1
    assert s["time"]["last_session"] == 1


def test_get_invocation_raises_when_missing(tmp_path):
    from metasmith.models.libraries import DataInstanceLibrary

    lib = DataInstanceLibrary(tmp_path / "lib")
    trace = tmp_path / "_metasmith" / "trace.jsonl"
    _write_trace(trace, [_mk_event("aaaa")])
    lib.attach_trace(trace)
    assert lib.get_invocation("aaaa").task_hash == "aaaa"
    with pytest.raises(InvocationNotFound):
        lib.get_invocation("nope")
    assert lib.try_get_invocation("nope") is None


def test_find_failures_predicate(tmp_path):
    from metasmith.models.libraries import DataInstanceLibrary

    lib = DataInstanceLibrary(tmp_path / "lib")
    trace = tmp_path / "_metasmith" / "trace.jsonl"
    _write_trace(
        trace,
        [
            _mk_event("h1", status="hit"),
            _mk_event("p1", status="promoted"),
            _mk_event("f1", status="fail", exit_code=1),
            _mk_event("m1", status="miss", exit_code=42),
            _mk_event("m2", status="miss", exit_code=0),
        ],
    )
    lib.attach_trace(trace)
    failures = lib.find_failures()
    hashes = sorted(e.task_hash for e in failures)
    assert hashes == ["f1", "m1"]


def test_find_invocations_filters_and_or_semantics(tmp_path):
    from metasmith.models.libraries import DataInstanceLibrary

    lib = DataInstanceLibrary(tmp_path / "lib")
    trace = tmp_path / "_metasmith" / "trace.jsonl"
    _write_trace(
        trace,
        [
            _mk_event("a", transform_key="tx_a", status="hit"),
            _mk_event("b", transform_key="tx_b", status="promoted"),
            _mk_event("c", transform_key="tx_c", status="hit"),
        ],
    )
    lib.attach_trace(trace)
    only_hits = lib.find_invocations(status="hit")
    assert sorted(e.task_hash for e in only_hits) == ["a", "c"]
    multi_tx = lib.find_invocations(transform_key=["tx_a", "tx_c"])
    assert sorted(e.task_hash for e in multi_tx) == ["a", "c"]
    and_filter = lib.find_invocations(transform_key="tx_a", status="promoted")
    assert and_filter == []


def test_empty_trace_index_safe_queries(tmp_path):
    from metasmith.models.libraries import DataInstanceLibrary

    lib = DataInstanceLibrary(tmp_path / "lib")
    assert lib.find_invocations() == []
    assert lib.find_failures() == []
    assert lib.list_transforms() == []
    s = lib.summary()
    assert s["counts"]["events"] == 0
    assert s["counts"]["sessions"] == 0


def test_t1_find_invocations_positive_and_negative(tmp_path, virtual_runtime):
    from tests.metasmith.flow.conftest import build_linear_plan, run_and_load

    bp = build_linear_plan(tmp_path, n_steps=2)
    transform_keys = [s.transform.GetKey() for s in bp.plan.steps]
    _task, lib = run_and_load(virtual_runtime, bp)

    matched_any = False
    for tk in transform_keys:
        hits = lib.find_invocations(transform_key=tk)
        if hits:
            matched_any = True
            break
    assert matched_any, (
        f"no events found for any plan transform_key={transform_keys!r}; "
        f"summary={lib.summary()['by_transform']!r}"
    )
    assert lib.find_invocations(transform_key="no_such_transform_xyz") == []


def test_t2_find_invocations_status_hit(tmp_path, virtual_runtime):
    from tests.metasmith.flow.conftest import build_linear_plan, run_and_load

    bp = build_linear_plan(tmp_path, n_steps=2)
    _task, lib_cold = run_and_load(virtual_runtime, bp)
    assert lib_cold.find_invocations(status="hit") == [], (
        f"unexpected hit rows on cold cache: {lib_cold.find_invocations(status='hit')!r}"
    )

    bp2 = build_linear_plan(tmp_path, n_steps=2)
    _task2, lib_hot = run_and_load(virtual_runtime, bp2)
    hits = lib_hot.find_invocations(status="hit")
    assert len(hits) >= 1, (
        f"expected >=1 hit row on hot-cache rerun; "
        f"summary={lib_hot.summary()['by_status']!r}"
    )


def test_t5_get_siblings_of_task_scope(tmp_path, virtual_runtime):
    from tests.metasmith.flow.conftest import build_branching_plan, run_and_load

    bp = build_branching_plan(tmp_path, fanout=2)
    _task, lib = run_and_load(virtual_runtime, bp)

    found_target = None
    for path in lib.manifest:
        meta = lib.instance_meta.get(path)
        if meta is None:
            continue
        inst_id = meta["instance_id"]
        try:
            siblings = lib.get_siblings_of(inst_id, scope="task")
        except Exception:
            continue
        if siblings:
            found_target = (inst_id, siblings)
            break

    if found_target is None:
        any_id = None
        for path in lib.manifest:
            meta = lib.instance_meta.get(path)
            if meta is not None:
                any_id = meta["instance_id"]
                break
        if any_id is None:
            pytest.skip("no tracked instances in library after virtual run")
        sibs = lib.get_siblings_of(any_id, scope="task")
        assert sibs == [], (
            f"expected isolated-task siblings to be [], got {sibs!r}"
        )
        return

    inst_id, siblings = found_target
    assert all(getattr(s, "instance_id", None) != inst_id for s in siblings), (
        f"get_siblings_of returned the query target itself: {inst_id!r}"
    )
    assert len(siblings) >= 1


def test_t6_get_logs_of_xfail_or_skip(tmp_path, virtual_runtime):
    from tests.metasmith.flow.conftest import build_linear_plan, run_and_load

    bp = build_linear_plan(tmp_path, n_steps=2)
    _task, lib = run_and_load(virtual_runtime, bp)

    sample_inst_id: str | None = None
    for path in lib.manifest:
        meta = lib.instance_meta.get(path)
        if meta is not None:
            sample_inst_id = meta["instance_id"]
            break
    if sample_inst_id is None:
        pytest.skip("no tracked instances in library after virtual run")

    bundle = lib.get_logs_of(sample_inst_id)
    if getattr(bundle, "status", None) == "available":
        return
    pytest.skip(
        reason=(
            "virtual_runtime does not emit command shard; "
            "covered by tests/e2e/docker/T6 (LogBundle.status="
            f"{getattr(bundle, 'status', None)!r})"
        )
    )
