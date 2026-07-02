"""Cross-workflow cache reuse.

Two distinct WorkflowPlans, two distinct agent homes (different
`--workspace` paths). P1 runs, promotes outputs into its task_cache;
P2 is then expected to hit the cache via one of two channels:

  (a) **lineage**: the cache shard is shared at the agent_home level
      (`<agent_home>/task_cache/` is shared / copied between homes).
      Entries promoted by `promote_run` carry `origin="lineage"`.

  (b) **imported**: a serialized library that recorded lineage-origin
      DataInstances in workspace_a is brought into workspace_b via
      `metasmith data import-library`, which upserts cache rows as
      `origin="imported"`.

For both sub-cases the contract is the same: the cache.sqlite row for
the relevant key carries the correct `origin`, and (where the planner
can express the demand) P2's trace.jsonl records a `status="hit"` row.

Since R1, leaf ids are content-addressed, so "exact same plan ran in two
workspaces" now hits automatically whenever the input *bytes* match —
even with independently built library objects at different locations.
That auto-resume path is proven directly in `test_cross_run.py`. The two
sub-cases here cover the complementary plumbing: (a) reuses the source
library so the lineage key is byte-stable regardless of input presence,
and (b) asserts the `import-library` bridge that carries already-computed
lineage rows across workspaces (still needed for absent/remote inputs
whose leaves fall back to random ids).
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

from tests.cache._cache_harness import capture_run, clear_trace
from tests.cache.fixtures.cache_fixtures import linear_3step


def _trace_path(virtual_runtime, task) -> Path:
    return (
        virtual_runtime.home
        / "runs"
        / task.GetKey()
        / "_metasmith"
        / "trace.jsonl"
    )


def _cache_origins(cache_root: Path) -> dict[str, int]:
    """Return {origin: count} for non-tombstoned cache.sqlite rows."""
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


# ---------------------------------------------------------------------------
# Sub-case (a): shared task_cache via direct copy (origin="lineage")
# ---------------------------------------------------------------------------


def test_shared_task_cache_origin_lineage(tmp_path):
    """P1 promotes cache rows with origin="lineage"; P2 against the same
    task on a freshly built agent home hits every step at compile time.

    Implementation: stand up two independent VirtualE2ERuntime homes
    sequentially. Run P1 against runtime_a and capture the populated
    cache shard. Stand up runtime_b in a fresh tmpdir, copy P1's
    `task_cache/` wholesale into runtime_b's home BEFORE its first
    run. Reuse the SAME WorkflowTask (same samples library, same leaf
    instance_ids) so the lineage_key chain is byte-stable across the
    two homes and the compile-time probe in runtime_b hits every step.

    The canonical assertion is via the per-run `_metasmith/trace.jsonl`
    — the compile-time probe emits `status="hit"` rows there for every
    matched step. The virtual_runtime executor still rebuilds meta
    files and may emit bootstrap_calls for hits (cached steps don't
    write `workflow.step_*.meta` files in this codegen path, so the
    runtime-side probe sees nothing to skip), so we do NOT assert on
    `snap.executed_steps` here — that gap is virtual-only, not a real
    Nextflow regression.

    Assertions:
      * P1's cache.sqlite contains only origin="lineage" rows.
      * Runtime_b's per-run trace.jsonl carries `status="hit"`
        InvocationEvent rows for every cacheable step.
    """
    from metasmith.constants import AgentPaths
    from metasmith.telemetry import TraceIndex
    from metasmith.testing.virtual_runtime import VirtualE2ERuntime

    task = linear_3step.build_task(tmp_path)

    # ---- P1 ----
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

        # Stage a seed copy of P1's task_cache for runtime_b.
        p2_seed_dir = tmp_path / "_p2_seed_cache"
        shutil.copytree(p1_cache, p2_seed_dir)
    finally:
        monkey_a.undo()

    # ---- P2 ----
    monkey_b = pytest.MonkeyPatch()
    try:
        runtime_b = VirtualE2ERuntime(
            tmp_path / "runtime_b", host="virt-host", force_bounce=False
        )
        runtime_b.setup(monkey_b)
        monkey_b.setattr(AgentPaths, "HOME_ROOT", runtime_b.home)
        monkey_b.setattr(AgentPaths, "WORK_ROOT", runtime_b.home / "_ws")

        # Seed runtime_b's task_cache from runtime_a's promote output
        # BEFORE its run — this is what "shared task_cache" means at
        # the agent-home level.
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
        # Specifically, every plan step should appear as a hit (3 in
        # linear_3step). Allow for mixed promoted rows (the virtual
        # runtime re-executes hits because its runtime-side probe
        # doesn't see meta files for the cached path; see docstring).
        hit_steps = {e.step_name for e in hit_events}
        assert hit_steps == {"trA", "trB", "trC"}, (
            f"P2 hit rows did not cover every step: {hit_steps}"
        )
    finally:
        monkey_b.undo()


# ---------------------------------------------------------------------------
# Sub-case (b): cross-workflow import via `metasmith data import-library`
# ---------------------------------------------------------------------------


def _build_export_lib(workspace: Path, *, lineage_id_hex: str) -> Path:
    """Build a small library in `workspace` containing one leaf + one
    lineage-origin DataInstance, and serialize it for transport."""
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
    """`metasmith data import-library` upserts cache rows as origin="imported".

    Build a library with one origin="lineage" DataInstance in
    workspace_a, then import it into workspace_b's library + cache via
    the public ops API. The destination cache.sqlite must contain
    exactly one entry, and that entry must carry origin="imported".

    The "imported entries are addressable on cross-workflow probe"
    half of the contract is covered structurally: identity is
    preserved across the import (pinned by
    `test_identity.test_import_library_preserves_identity`), and the
    `origin` column is the probe's discriminator at lookup time.
    """
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
    """An imported cache row is probe-addressable by its instance_id key.

    After import_library completes, `CacheStore.probe(<imported_key>)`
    must return an entry whose `origin == "imported"`. This is the
    probe-side half that lets a downstream workflow read the imported
    row as a hit without re-executing the producer transform.
    """
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
