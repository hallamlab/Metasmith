"""The lineage report's rows, and what it reads off a run still in flight."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from types import SimpleNamespace

import cbor2

from metasmith.agents.lineage_report import (
    Node, explode, read_nodes, write_csv_atomic,
)
from metasmith.caching.admission import shard_for
from metasmith.caching.invocation import MANIFEST_NAME
from metasmith.caching.promote import CACHE_HITS_LOG, CACHE_RECORD_FILE
from metasmith.models.lineage import LinPayload


def _nodes(*specs) -> dict[str, Node]:
    return {
        nid: Node(id=nid, dtype_name=t, path=f"/{nid}", parents=tuple(parents))
        for nid, t, parents in specs
    }


def _rows(nodes, columns, **kw):
    rows, truncated = explode(nodes, columns, **kw)
    assert not truncated
    return [dict(zip(columns, r)) for r in rows]


def test_a_chain_is_one_row():
    nodes = _nodes(("s", "seed", []), ("a", "a", ["s"]), ("b", "b", ["a"]))
    assert _rows(nodes, ["seed", "a", "b"]) == [{"seed": "/s", "a": "/a", "b": "/b"}]


def test_a_merge_of_n_is_n_rows():
    nodes = _nodes(
        ("s1", "seed", []), ("s2", "seed", []), ("s3", "seed", []),
        ("a1", "a", ["s1"]), ("a2", "a", ["s2"]), ("a3", "a", ["s3"]),
        ("m", "b", ["a1", "a2", "a3"]),
    )
    rows = _rows(nodes, ["seed", "a", "b"])
    assert sorted((r["seed"], r["a"]) for r in rows) == [
        ("/s1", "/a1"), ("/s2", "/a2"), ("/s3", "/a3"),
    ]
    assert {r["b"] for r in rows} == {"/m"}


def test_a_fan_out_is_one_row_per_leaf():
    nodes = _nodes(("s", "seed", []), ("a1", "a", ["s"]), ("a2", "a", ["s"]))
    assert _rows(nodes, ["seed", "a"]) == [
        {"seed": "/s", "a": "/a1"}, {"seed": "/s", "a": "/a2"},
    ]


def test_a_diamond_does_not_fork():
    nodes = _nodes(
        ("r", "root", []), ("a", "a", ["r"]), ("b", "b", ["r"]), ("m", "m", ["a", "b"]),
    )
    assert len(_rows(nodes, ["root", "a", "b", "m"])) == 1


def test_a_combination_that_disagrees_with_itself_is_dropped():
    nodes = _nodes(
        ("x1", "x", []), ("x2", "x", []), ("y", "y", ["x1"]), ("m", "m", ["x1", "x2", "y"]),
    )
    assert _rows(nodes, ["x", "y", "m"]) == [{"x": "/x1", "y": "/y", "m": "/m"}]


def test_excluded_and_unrecorded_parents_are_skipped():
    nodes = _nodes(
        ("e", "env::python", []), ("s", "seed", []), ("a", "a", ["s", "e", "missing"]),
    )
    assert _rows(nodes, ["seed", "a"]) == [{"seed": "/s", "a": "/a"}]


def test_the_row_cap_truncates_and_says_so():
    nodes = _nodes(*[(f"s{i}", "seed", []) for i in range(5)])
    rows, truncated = explode(nodes, ["seed"], cap=2)
    assert truncated and len(rows) == 2


def test_the_csv_is_replaced_whole(tmp_path):
    target = tmp_path / "out.csv"
    write_csv_atomic(target, ["a", "b"], [("1", "2")])
    write_csv_atomic(target, ["a", "b"], [("3", "4")])
    assert list(csv.reader(target.open())) == [["a", "b"], ["3", "4"]]
    assert [p.name for p in tmp_path.iterdir()] == ["out.csv"]


def _plan(given_path: Path):
    given = SimpleNamespace(
        instance_id="g1", dtype_name="ns::seed", ResolvePath=lambda: given_path,
    )
    return SimpleNamespace(given=[given], steps=[])


def _produced(slot_id: str, name: str, dtype: str, parents: list[str]) -> dict:
    return {"relpath": f"out/{name}", "slot_id": slot_id, "dtype_name": dtype, "parents": parents}


def test_records_hits_and_givens_are_read_for_this_session_only(tmp_path):
    workspace, cache_root = tmp_path / "ws", tmp_path / "cache"
    (workspace / "_metasmith").mkdir(parents=True)
    (workspace / "_metasmith" / "trace.jsonl").write_text(
        json.dumps({"event": "session_start", "session_id": 5}) + "\n"
    )
    task_dir = workspace / "nxf_work" / "ab" / "cdef"
    task_dir.mkdir(parents=True)
    ours = {"session": 5, "member": 1, "produces": [_produced("sa", "1-1-1.t-a.txt", "ns::a", ["g1"])]}
    foreign = {"session": 4, "member": 0, "produces": [_produced("sa", "1-9-1.t-a.txt", "ns::a", ["g1"])]}
    (task_dir / CACHE_RECORD_FILE).write_text(
        json.dumps(ours) + "\n" + json.dumps(foreign) + "\n" + '{"session": 5, "memb'
    )

    key = "ab" * 16
    shard = shard_for(cache_root, key, "lineage")
    shard.mkdir(parents=True)
    hit_file = _produced("sb", "1-1-1.t-b.txt", "ns::b", [LinPayload.mint_file_id("sa", "out/1-1-1.t-a.txt")])
    (shard / MANIFEST_NAME).write_bytes(cbor2.dumps({"files": [hit_file]}))
    (workspace / CACHE_HITS_LOG).write_text(json.dumps({"step": 2, "key": key}) + "\n")

    nodes = read_nodes(workspace, _plan(tmp_path / "in.txt"), cache_root)

    a_id = LinPayload.mint_file_id("sa", "out/1-1-1.t-a.txt")
    b_id = LinPayload.mint_file_id("sb", "out/1-1-1.t-b.txt")
    assert set(nodes) == {"g1", a_id, b_id}
    assert nodes[a_id].path == str(task_dir / "2-1-1.t-a.txt")
    assert nodes[b_id].path == str(shard / "out/1-1-1.t-b.txt")
    rows, _ = explode(nodes, ["ns::seed", "ns::a", "ns::b"])
    assert rows == [(str(tmp_path / "in.txt"), nodes[a_id].path, nodes[b_id].path)]
