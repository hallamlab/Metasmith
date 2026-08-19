from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

from tests.metasmith.cache._cache_harness import capture_run, clear_trace
from tests.metasmith.cache.fixtures.cache_fixtures import linear_3step


def _trace_path(virtual_runtime, task) -> Path:
    return (
        virtual_runtime.home
        / "runs"
        / task.GetKey()
        / "_metasmith"
        / "trace.jsonl"
    )


def _cache_origins(cache_root: Path) -> dict[str, int]:
    db = cache_root / "cache.sqlite"
    if not db.exists():
        return {}
    conn = sqlite3.connect(db)
    try:
        rows = conn.execute(
            "SELECT origin, COUNT(*) FROM entries "
            "WHERE tombstoned_at IS NULL GROUP BY origin"
        ).fetchall()
    finally:
        conn.close()
    return dict(rows)


def test_shared_task_cache_origin_lineage(tmp_path):
    from metasmith.constants import AgentPaths
    from metasmith.telemetry import TraceIndex
    from metasmith.testing.virtual_runtime import VirtualE2ERuntime

    task = linear_3step.build_task(tmp_path)

    runtime_a_root = tmp_path / "runtime_a"
    monkey_a = pytest.MonkeyPatch()
    try:
        runtime_a = VirtualE2ERuntime(
            runtime_a_root, host="virt-host", force_bounce=False
        )
        runtime_a.setup(monkey_a)
        monkey_a.setattr(AgentPaths, "HOME_ROOT", runtime_a.home)
        monkey_a.setattr(AgentPaths, "WORK_ROOT", runtime_a.home / "_ws")

        capture_run(runtime_a, task)
        p1_cache = runtime_a.home / "task_cache"
        p1_origins = _cache_origins(p1_cache)
        assert p1_origins.get("lineage", 0) > 0, (
            f"P1 did not promote any lineage-origin rows: {p1_origins}"
        )
        assert "imported" not in p1_origins, (
            f"P1 should have only lineage-origin rows: {p1_origins}"
        )

        p2_seed_dir = tmp_path / "_p2_seed_cache"
        shutil.copytree(p1_cache, p2_seed_dir)
    finally:
        monkey_a.undo()

    monkey_b = pytest.MonkeyPatch()
    try:
        runtime_b = VirtualE2ERuntime(
            tmp_path / "runtime_b", host="virt-host", force_bounce=False
        )
        runtime_b.setup(monkey_b)
        monkey_b.setattr(AgentPaths, "HOME_ROOT", runtime_b.home)
        monkey_b.setattr(AgentPaths, "WORK_ROOT", runtime_b.home / "_ws")

        b_cache = runtime_b.home / "task_cache"
        shutil.copytree(p2_seed_dir, b_cache)

        capture_run(runtime_b, task)

        p2_trace_path = (
            runtime_b.home / "runs" / task.GetKey() / "_metasmith" / "trace.jsonl"
        )
        assert p2_trace_path.exists(), f"no trace.jsonl at {p2_trace_path}"

        idx = TraceIndex.read(p2_trace_path)
        hit_events = [e for e in idx.events if e.status == "hit"]
        assert hit_events, (
            f"P2 (shared task_cache) emitted no hit rows in {p2_trace_path}; "
            f"lineage-origin cache was not addressable across workspaces. "
            f"events: {[(e.status, e.step_name) for e in idx.events]}"
        )
        hit_steps = {e.step_name for e in hit_events}
        assert hit_steps == {"trA", "trB", "trC"}, (
            f"P2 hit rows did not cover every step: {hit_steps}"
        )
    finally:
        monkey_b.undo()


def _build_export_lib(workspace: Path, *, lineage_id_hex: str) -> Path:
    from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
    from metasmith.models.solver import Endpoint

    types = DataTypeLibrary()
    types["seed"] = Endpoint(properties={"seed"})
    types["product"] = Endpoint(properties={"product"})

    src = workspace / "lib.xgdb"
    src.parent.mkdir(parents=True, exist_ok=True)
    lib = DataInstanceLibrary(src)
    lib.AddTypeLibrary(types, namespace="t")

    (src / "leaf.txt").parent.mkdir(parents=True, exist_ok=True)
    (src / "leaf.txt").write_text("leaf payload\n")
    lib.AddItem(Path("leaf.txt"), "t::seed")

    (src / "lineage.txt").write_text("lineage payload\n")
    lib.AddItem(Path("lineage.txt"), "t::product")
    lib.SetLineageInstance(
        Path("lineage.txt"),
        instance_id=lineage_id_hex,
        lineage_payload=b"fake-lineage-cbor-payload",
        origin="lineage",
    )
    lib.Save()
    return src


def test_import_library_origin_imported(tmp_path):
    from metasmith.ops.data import import_library

    lineage_id = "1e20" + "ef" * 32
    src_dir = _build_export_lib(tmp_path / "ws_a", lineage_id_hex=lineage_id)

    dst_dir = tmp_path / "ws_b" / "lib.xgdb"
    cache_root = tmp_path / "ws_b" / "task_cache"

    result = import_library(
        src_uri=str(src_dir),
        dest_path=str(dst_dir),
        cache_root=str(cache_root),
        as_image=False,
    )
    assert result["imported_cache_entries"] == 1
    assert result["skipped_leaf_entries"] == 1

    origins = _cache_origins(cache_root)
    assert origins == {"imported": 1}, (
        f"expected one imported cache row, got {origins}"
    )


def test_import_library_then_probe_hits_imported_key(tmp_path):
    from metasmith.caching.store import CacheStore
    from metasmith.ops.data import import_library

    lineage_id = "1e20" + "12" * 32
    src_dir = _build_export_lib(tmp_path / "ws_a", lineage_id_hex=lineage_id)

    dst_dir = tmp_path / "ws_b" / "lib.xgdb"
    cache_root = tmp_path / "ws_b" / "task_cache"
    import_library(
        src_uri=str(src_dir),
        dest_path=str(dst_dir),
        cache_root=str(cache_root),
        as_image=False,
    )

    store = CacheStore.open(cache_root)
    try:
        entry = store.probe(bytes.fromhex(lineage_id))
    finally:
        store.close()

    assert entry is not None, (
        "imported cache row not probe-addressable by its key"
    )
    assert entry.origin == "imported", (
        f"imported row has wrong origin: {entry.origin}"
    )
