"""Telemetry API unit tests (C8 / G5).

Covers the attach_trace + summary() shape on `DataInstanceLibrary` plus
the fundamental query dispatch (instance_id lookup, get_invocation,
find_failures predicate). Heavier scenarios (groupTuple lineage walk,
log resume) live in `tests/integration/test_telemetry_e2e.py`.
"""

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
    """TraceIndex parses the sentinel + events; lookup by file_instance_id works."""
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
    """A row missing schema_version is skipped, not raised."""
    trace = tmp_path / "_metasmith" / "trace.jsonl"
    trace.parent.mkdir(parents=True)
    legacy = json.dumps({"source": "hit", "step": "1", "cache_key": "deadbeef"})
    trace.write_text(legacy + "\n")
    idx = TraceIndex.read(trace)
    assert idx.events == []


def test_attach_trace_idempotent_and_raises_on_switch(tmp_path):
    """Same (path, across_sessions) → no-op. Different path → TraceAlreadyAttached."""
    from metasmith.models.libraries import DataInstanceLibrary
    from metasmith.models.lineage import TraceAlreadyAttached

    lib = DataInstanceLibrary(tmp_path / "lib")
    trace = tmp_path / "_metasmith" / "trace.jsonl"
    _write_trace(trace, [_mk_event("ffff")])
    lib.attach_trace(trace)
    lib.attach_trace(trace)  # idempotent
    other = tmp_path / "_metasmith" / "trace.99.jsonl"
    _write_trace(other, [_mk_event("eeee")])
    with pytest.raises(TraceAlreadyAttached):
        lib.attach_trace(other)


def test_summary_shape(tmp_path):
    """summary() returns the documented dict shape with schema_version=2."""
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
    """get_invocation(unknown) raises InvocationNotFound; try_get returns None."""
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
    """find_failures picks status=='fail' OR (status=='miss' AND exit_code!=0)."""
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
            _mk_event("m2", status="miss", exit_code=0),  # successful miss → not a failure
        ],
    )
    lib.attach_trace(trace)
    failures = lib.find_failures()
    hashes = sorted(e.task_hash for e in failures)
    assert hashes == ["f1", "m1"]


def test_find_invocations_filters_and_or_semantics(tmp_path):
    """AND across kwargs; OR within iterables."""
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
    """A library with no trace attached returns empty results, not errors."""
    from metasmith.models.libraries import DataInstanceLibrary

    lib = DataInstanceLibrary(tmp_path / "lib")
    assert lib.find_invocations() == []
    assert lib.find_failures() == []
    assert lib.list_transforms() == []
    s = lib.summary()
    assert s["counts"]["events"] == 0
    assert s["counts"]["sessions"] == 0
