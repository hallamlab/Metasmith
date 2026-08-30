"""`record_run` reads records a task wrote from inside its own container.

A task promotes into the cache root as the container sees it, so the path in
its `.command.cache` record is `/msm_home/task_cache/...` where the driver
sees the agent home. The key names the shard, so the driver derives it; and
whatever one record does, it must not cost the run the others.
"""

from __future__ import annotations

import json
from pathlib import Path

from metasmith.caching.layout import shard_dir
from metasmith.caching.promote import (
    CACHE_HITS_LOG, CACHE_RECORD_FILE, promote_members, record_run,
)
from metasmith.caching.store import CacheStore
from metasmith.telemetry import TraceIndex

from tests.metasmith.cache.test_promote import KEY, META, _entry, _task_dir

CONTAINER_CACHE = Path("/msm_home/task_cache")


def _workspace(tmp_path: Path) -> tuple[Path, Path]:
    """A workspace whose one task promoted a member, container-side."""
    workspace = tmp_path / "ws"
    cache_root = tmp_path / "home" / "task_cache"
    task_dir = workspace / "nxf_work" / "ab" / "cdef"
    task_dir.mkdir(parents=True)
    (task_dir / "1-1-1.abcdef-step_a.txt").write_text("payload")

    records = promote_members(
        cwd=task_dir, entries=[_entry()], meta=META,
        cache_root=cache_root, successes=[True],
    )
    assert records[0]["status"] == "promoted"

    # Rewrite the record the way the task inside the container would have.
    rec = records[0] | {"shard": str(CONTAINER_CACHE / KEY[:2] / KEY[2:])}
    (task_dir / CACHE_RECORD_FILE).write_text(json.dumps(rec) + "\n")

    (workspace / "workflow.step_1.meta").write_text(
        f"transform_key {META.transform_key}\nsignature {META.signature}\n"
        f"step_name {META.step_name}\ncacheable 1\n"
    )
    return workspace, cache_root


def _indexed(cache_root: Path) -> list[str]:
    store = CacheStore.open(cache_root)
    try:
        return [r[0].hex() for r in store.conn.execute("SELECT key FROM entries")]
    finally:
        store.close()


def test_a_container_side_record_still_indexes(tmp_path):
    workspace, cache_root = _workspace(tmp_path)
    summary = record_run(workspace=workspace, cache_root=cache_root)
    assert summary["promoted"] == [KEY], (
        "a record naming the container's cache root was not indexed"
    )
    assert _indexed(cache_root) == [KEY]
    events = TraceIndex.read(workspace / "_metasmith" / "trace.jsonl").events
    assert [e.status for e in events] == ["promoted"]


def test_the_indexed_output_root_stays_relative(tmp_path):
    workspace, cache_root = _workspace(tmp_path)
    record_run(workspace=workspace, cache_root=cache_root)
    store = CacheStore.open(cache_root)
    try:
        (root,) = next(store.conn.execute(
            "SELECT output_root FROM entries WHERE key = ?", (bytes.fromhex(KEY),)
        ))
    finally:
        store.close()
    assert not Path(root).is_absolute(), (
        f"the shard was indexed by an absolute path from another host: {root}"
    )
    assert (cache_root / root / "out").is_dir()


def test_one_bad_record_does_not_lose_the_rest(tmp_path):
    workspace, cache_root = _workspace(tmp_path)
    broken = workspace / "nxf_work" / "00" / "0000"
    broken.mkdir(parents=True)
    (broken / CACHE_RECORD_FILE).write_text(
        json.dumps({"session": 0, "step": 1, "status": "promoted", "key": "zz"}) + "\n"
    )
    log: list = []
    summary = record_run(workspace=workspace, cache_root=cache_root, log=log)
    assert summary["promoted"] == [KEY], (
        "one unusable record cost the run every other one"
    )
    assert any(level == "warn" for level, _ in log), (
        "a dropped record left no warning behind"
    )


def test_a_container_side_hit_is_served_from_the_shard(tmp_path):
    workspace, cache_root = _workspace(tmp_path)
    record_run(workspace=workspace, cache_root=cache_root)
    (workspace / "_metasmith" / "trace.jsonl").unlink()

    hits = workspace / CACHE_HITS_LOG
    hits.parent.mkdir(parents=True, exist_ok=True)
    hits.write_text(json.dumps({
        "key": KEY, "step": 1, "step_name": META.step_name,
        "shard": str(CONTAINER_CACHE / KEY[:2] / KEY[2:]),
    }) + "\n")

    summary = record_run(workspace=workspace, cache_root=cache_root)
    assert summary["hits"] == [KEY], (
        "a hit naming the container's cache root was not read back"
    )
    events = TraceIndex.read(workspace / "_metasmith" / "trace.jsonl").events
    assert "hit" in {e.status for e in events}
    assert shard_dir(cache_root, KEY).is_dir()
