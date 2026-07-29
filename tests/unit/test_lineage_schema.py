import json
from pathlib import Path

import pytest

from metasmith.models.lineage import (
    INVOCATION_EVENT_SCHEMA_VERSION,
    LIN_PAYLOAD_VERSION,
    GroupingFrame,
    InvocationEvent,
    LeafRecord,
    LinPayload,
    LineageNode,
    LogBundle,
    ProducedFile,
    SessionStart,
    append_invocation_event,
)


def _example_payload() -> LinPayload:
    return LinPayload(
        v=LIN_PAYLOAD_VERSION,
        entries={
            "slot_a": [123456789, 987654321],
            "slot_b": [42],
        },
    )


def _example_event(status="miss") -> InvocationEvent:
    return InvocationEvent(
        task_hash="task_abcdef",
        transform_key="lib::tool.py",
        status=status,
        consumes={"reads": ["fid_1", "fid_2"]},
        produces=[
            ProducedFile(
                file_instance_id="fid_out",
                slot_id="slot_X",
                path="out/x.gbk",
                dtype_key="sequences::gbk",
            ),
        ],
        session_id=7,
        group=GroupingFrame(
            strategy="groupTuple",
            group_key="sample_42",
            sibling_instance_ids=["fid_1", "fid_2"],
        ),
        work_dir="/work/abc",
        nxf_task_id="42",
        exit_code=0,
        started_at="2026-05-30T12:00:00Z",
        ended_at="2026-05-30T12:00:05Z",
        time_source="orchestrator",
        container="quay.io/biocontainers/tool:1.0",
        host="agent-1",
        step_order=3,
        step_name="binner",
        cache_key="0123456789abcdef",
    )


def test_lin_payload_roundtrip():
    payload = _example_payload()
    encoded = payload.to_json()
    decoded = LinPayload.from_json(encoded)
    assert decoded == payload
    assert decoded.v == LIN_PAYLOAD_VERSION
    assert decoded.entries["slot_a"] == [123456789, 987654321]
    assert decoded.entries["slot_b"] == [42]
    # Wire shape matches what Orchestrator.JsonforEcho(index) emits today,
    # wrapped in {"v": 2, "entries": ...}.
    assert json.loads(encoded) == {
        "v": LIN_PAYLOAD_VERSION,
        "entries": {"slot_a": [123456789, 987654321], "slot_b": [42]},
    }
    # mint_file_id is deterministic.
    fid1 = LinPayload.mint_file_id("slot_A", "out/x.gbk")
    fid2 = LinPayload.mint_file_id("slot_A", "out/x.gbk")
    fid3 = LinPayload.mint_file_id("slot_A", Path("out/x.gbk"))
    fid_diff = LinPayload.mint_file_id("slot_A", "out/y.gbk")
    assert fid1 == fid2 == fid3
    assert fid_diff != fid1


@pytest.mark.xfail(
    strict=True,
    reason=(
        "dc8dd6c collapsed the wire to a single entry map (`lineages = "
        "[lin_payload.entries]`) and cfd0236 made the emitter conform "
        "(`entries:index[0]`), so every batch member after the first is "
        "discarded and AsBatch() yields once"
    ),
)
def test_lin_payload_carries_one_entry_map_per_batch_member():
    """The wire is a LIST of per-member maps, not one map.

    `_collateBatch` builds one index per batch member and the process
    receives them as a list, so a `batch_size=3` task has three lineage maps
    — three FILES groups, three sets of slot hashes. Bootstrap turns each
    into one `context.AsBatch()` member.

    Observed on real Nextflow (3 seeds, batch_size=3): the task's `index`
    has size 3 while the emitted envelope carries member 0 only — one FILES
    group holding one file. The other two members' files are staged in the
    task directory and never reach the protocol, silently.

    `release` is the reference: it emits `JsonforEcho(index)` (the whole
    list) and parses it with `json.loads` + a list check.
    """
    members = [
        {"slot_a": ["h1"], LinPayload.FILES_KEY: [["/w/a1.fq"]]},
        {"slot_a": ["h2"], LinPayload.FILES_KEY: [["/w/a2.fq"]]},
        {"slot_a": ["h3"], LinPayload.FILES_KEY: [["/w/a3.fq"]]},
    ]
    payload = LinPayload(v=LIN_PAYLOAD_VERSION, entries=members)
    decoded = LinPayload.from_json(payload.to_json())
    assert decoded.entries == members
    assert [m[LinPayload.FILES_KEY] for m in decoded.entries] == [
        [["/w/a1.fq"]],
        [["/w/a2.fq"]],
        [["/w/a3.fq"]],
    ]


def test_lin_payload_rejects_unknown_version():
    with pytest.raises(ValueError):
        LinPayload.from_json(json.dumps({"v": 99, "entries": {}}))


def test_lin_payload_rejects_non_dict_entries():
    with pytest.raises(ValueError):
        LinPayload.from_json(json.dumps({"v": LIN_PAYLOAD_VERSION, "entries": []}))


def test_lin_payload_file_groups_and_lineage_index():
    """Wire-shape: Orchestrator injects FILES alongside lineage_index hashes.

    `file_groups()` extracts the special FILES key; `lineage_index()`
    returns everything else. C5's bootstrap consumes both.
    """
    raw = {
        "v": LIN_PAYLOAD_VERSION,
        "entries": {
            "slot_reads": [12345, 67890],
            "slot_db": [42],
            LinPayload.FILES_KEY: [["/work/r1.fq", "/work/r2.fq"], ["/work/db.fa"]],
        },
    }
    payload = LinPayload.from_json(json.dumps(raw))
    assert payload.file_groups() == [
        ["/work/r1.fq", "/work/r2.fq"],
        ["/work/db.fa"],
    ]
    assert payload.lineage_index() == {
        "slot_reads": [12345, 67890],
        "slot_db": [42],
    }
    # Missing FILES key returns empty list (e.g., direct-run path).
    bare = LinPayload(v=LIN_PAYLOAD_VERSION, entries={"slot_x": [1]})
    assert bare.file_groups() == []
    assert bare.lineage_index() == {"slot_x": [1]}


def test_invocation_event_roundtrip():
    ev = _example_event()
    line = ev.to_jsonl()
    parsed = InvocationEvent.from_jsonl(line)
    assert parsed is not None
    assert parsed == ev
    assert parsed.status == "miss"
    assert parsed.produces[0].file_instance_id == "fid_out"
    assert parsed.group is not None
    assert parsed.group.strategy == "groupTuple"


def test_legacy_row_tolerance():
    legacy_row = json.dumps(
        {"source": "hit", "step": 2, "cache_key": "deadbeef", "transform_key": "lib::x"}
    )
    parsed = InvocationEvent.from_jsonl(legacy_row)
    assert parsed is None  # legacy v1 rows skip, do not raise

    session_row = SessionStart(
        session_id=12,
        compile_started_at="2026-05-30T11:59:00Z",
        metasmith_version="0.20.0",
    ).to_jsonl()
    parsed = InvocationEvent.from_jsonl(session_row)
    assert parsed is None  # SessionStart sentinel skipped by event parser


def test_lineage_node_to_json():
    leaf = LineageNode(
        instance_id="leaf_1",
        dtype_key="sequences::gbk",
        path="reads/in.gbk",
        produced_by=LeafRecord(source="user_added", added_at="2026-05-30T10:00:00Z"),
    )
    root = LineageNode(
        instance_id="out_1",
        dtype_key="sequences::gbk",
        path="out/o.gbk",
        produced_by=_example_event(),
        inputs={"reads": [leaf]},
        group=GroupingFrame(strategy="flat", group_key="g"),
    )
    encoded = root.to_json()
    decoded = json.loads(encoded)
    assert decoded["instance_id"] == "out_1"
    assert decoded["produced_by"]["kind"] == "invocation"
    assert decoded["produced_by"]["task_hash"] == "task_abcdef"
    assert decoded["inputs"]["reads"][0]["instance_id"] == "leaf_1"
    assert decoded["inputs"]["reads"][0]["produced_by"]["kind"] == "leaf"
    assert decoded["group"]["strategy"] == "flat"


def test_lineage_node_to_mermaid():
    leaf = LineageNode(
        instance_id="leaf12345abcdef",
        dtype_key="reads::fastq",
        path="r.fastq",
        produced_by=LeafRecord(source="user_added"),
    )
    root = LineageNode(
        instance_id="root99887766aa",
        dtype_key="sequences::gbk",
        path="o.gbk",
        produced_by=_example_event(),
        inputs={"reads": [leaf]},
        group=GroupingFrame(strategy="groupTuple", group_key="g"),
    )
    diagram = root.to_mermaid(depth=2, include_groups=True)
    assert diagram.startswith("graph TD")
    assert "n_root998877" in diagram
    assert "n_leaf12345a" in diagram
    assert "-->|reads [groupTuple]|" in diagram

    # depth=0 emits just the root node, no edges
    shallow = root.to_mermaid(depth=0)
    assert "-->" not in shallow
    assert "n_root998877" in shallow


def test_log_bundle_status_closure():
    closed_statuses = {
        "available",
        "missing",
        "pruned",
        "legacy_shard_no_logs",
        "remote_cache_no_logs",
        "not_applicable",
    }
    for s in closed_statuses:
        b = LogBundle(status=s)  # type: ignore[arg-type]
        assert b.status == s
        assert b.is_available() == (s == "available")
    available = LogBundle(
        status="available",
        stdout=Path("/cache/00/aa/logs/.command.out"),
        stderr=Path("/cache/00/aa/logs/.command.err"),
    )
    assert available.is_available()
    missing = LogBundle(status="missing", reason="shard pruned")
    assert not missing.is_available()
    assert missing.reason == "shard pruned"


def test_append_invocation_event_writer(tmp_path):
    trace = tmp_path / "trace.jsonl"
    # SessionStart sentinel
    trace.write_text(
        SessionStart(session_id=1, compile_started_at="2026-05-30T12:00:00Z").to_jsonl() + "\n",
        encoding="utf-8",
    )
    ev_a = _example_event(status="hit")
    ev_b = _example_event(status="promoted")
    append_invocation_event(trace, ev_a)
    append_invocation_event(trace, ev_b)

    lines = trace.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    head = json.loads(lines[0])
    assert head["event"] == SessionStart.EVENT_NAME
    assert head["schema_version"] == INVOCATION_EVENT_SCHEMA_VERSION

    parsed_a = InvocationEvent.from_jsonl(lines[1])
    parsed_b = InvocationEvent.from_jsonl(lines[2])
    assert parsed_a is not None and parsed_a.status == "hit"
    assert parsed_b is not None and parsed_b.status == "promoted"
