from __future__ import annotations

import shutil
import time
from pathlib import Path

import pytest


from metasmith.caching.layout import shard_dir
from metasmith.caching.promote import StepCacheMeta, promote_members
from metasmith.models.lineage import LinPayload
from metasmith.models.workflow.payload import build_entry

KEY = "1e20" + "ef" * 32
META = StepCacheMeta(
    order=1, transform_key="trA", signature="sig", step_name="trA", cacheable=True,
    slot_files=[{
        "dtype_key": "step_a", "dtype_name": "mock::step_a", "ext": ".txt",
        "branch_idx": 0, "slot_id": "a" * 64,
    }],
    slot_channels={"seed_dep": "seed"},
)


def _task_dir(tmp_path: Path, name: str, payload: str = "payload") -> Path:
    cwd = tmp_path / name
    cwd.mkdir()
    (cwd / "1-1-1.abcdef-step_a.txt").write_text(payload)
    return cwd


def _entry() -> dict:
    e = build_entry([("seed", [("/w/in.txt", {"seed": ["1e20aaaa"]})])])
    e[LinPayload.KEY_KEY] = KEY
    return e


def test_a_promotion_leaves_no_staging_behind(tmp_path):
    cache_root = tmp_path / "task_cache"
    records = promote_members(
        cwd=_task_dir(tmp_path, "t1"), entries=[_entry()], meta=META,
        cache_root=cache_root, successes=[True],
    )
    assert records[0]["status"] == "promoted"
    assert not list(cache_root.glob("*.tmp")), "the staging directory outlived the rename"
    assert (shard_dir(cache_root, KEY) / "manifest.cbor").exists()


def test_an_existing_shard_wins(tmp_path):
    cache_root = tmp_path / "task_cache"
    promote_members(
        cwd=_task_dir(tmp_path, "t1", "first"), entries=[_entry()], meta=META,
        cache_root=cache_root, successes=[True],
    )
    records = promote_members(
        cwd=_task_dir(tmp_path, "t2", "second"), entries=[_entry()], meta=META,
        cache_root=cache_root, successes=[True],
    )
    assert records[0]["status"] == "exists"
    assert (shard_dir(cache_root, KEY) / "out" / "1-1-1.abcdef-step_a.txt").read_text() == "first"
    assert not list(cache_root.glob("*.tmp"))


def test_an_evicted_shard_is_replaced(tmp_path):
    from metasmith.caching.invocation import probe
    from metasmith.caching.promote import tombstone_shard

    cache_root = tmp_path / "task_cache"
    promote_members(
        cwd=_task_dir(tmp_path, "t1", "first"), entries=[_entry()], meta=META,
        cache_root=cache_root, successes=[True],
    )
    tombstone_shard(cache_root, KEY)
    assert probe(cache_root, bytes.fromhex(KEY)) is None

    records = promote_members(
        cwd=_task_dir(tmp_path, "t2", "second"), entries=[_entry()], meta=META,
        cache_root=cache_root, successes=[True],
    )

    assert records[0]["status"] == "promoted"
    assert probe(cache_root, bytes.fromhex(KEY)) is not None
    assert (shard_dir(cache_root, KEY) / "out" / "1-1-1.abcdef-step_a.txt").read_text() == "second"
    assert not list(cache_root.glob("*.tmp")) and not list(cache_root.glob("*.dead"))


def test_a_failed_member_is_recorded_and_not_promoted(tmp_path):
    cache_root = tmp_path / "task_cache"
    records = promote_members(
        cwd=_task_dir(tmp_path, "t1"), entries=[_entry()], meta=META,
        cache_root=cache_root, successes=[False],
    )
    assert records[0]["status"] == "failed"
    assert not shard_dir(cache_root, KEY).exists()


def test_a_product_is_stored_under_its_canonical_position(tmp_path):
    # The member sat third in its batch; in the shard it is member one.
    cwd = tmp_path / "t3"
    cwd.mkdir()
    (cwd / "3-1-1.abcdef-step_a.txt").write_text("third")
    entries = [{LinPayload.KEY_KEY: "-"}, {LinPayload.KEY_KEY: "-"}, _entry()]
    cache_root = tmp_path / "task_cache"
    records = promote_members(
        cwd=cwd, entries=entries, meta=META, cache_root=cache_root, successes=[True] * 3,
    )
    assert [r["status"] for r in records] == ["uncacheable", "uncacheable", "promoted"]
    assert (shard_dir(cache_root, KEY) / "out" / "1-1-1.abcdef-step_a.txt").read_text() == "third"
    assert records[2]["produces"][0]["relpath"] == "out/1-1-1.abcdef-step_a.txt"




def test_gc_tombstone_delay(tmp_path):
    from metasmith.caching.store import CacheStore
    from metasmith.ops.cache import gc_cache

    cache_root = tmp_path / "task_cache"
    cache_root.mkdir()
    store = CacheStore.open(cache_root)
    try:
        key_hex = "1e20" + "ab" * 32
        key = bytes.fromhex(key_hex)
        output_root = cache_root / key_hex[:2] / key_hex[2:]
        (output_root / "out").mkdir(parents=True)
        (output_root / "out" / "f.txt").write_text("payload")
        store.upsert(
            key=key,
            transform_key="tr.test",
            payload=b"\x00manifest",
            output_root=str(output_root.relative_to(cache_root)),
            size_bytes=7,
            origin="lineage",
        )
        store.conn.execute(
            "UPDATE entries SET last_hit_at = ? WHERE key = ?",
            (1, key),
        )
        store.conn.commit()
    finally:
        store.close()

    summary = gc_cache(
        cache_root=str(cache_root),
        older_than_seconds=10,
        delete=False,
    )
    assert summary["tombstoned"] == [key_hex]
    assert summary["deleted"] == []
    assert (cache_root / key_hex[:2] / key_hex[2:] / "out" / "f.txt").exists()
    from metasmith.caching.invocation import TOMBSTONE_NAME, probe

    assert (cache_root / key_hex[:2] / key_hex[2:] / TOMBSTONE_NAME).exists(), (
        "a tombstoned row left its shard servable to a task that never opens sqlite"
    )
    assert probe(cache_root, key) is None

    summary = gc_cache(
        cache_root=str(cache_root),
        delete=True,
    )
    assert summary["deleted"] == [], (
        f"entry unlinked while still inside grace window: {summary}"
    )
    assert (cache_root / key_hex[:2] / key_hex[2:] / "out" / "f.txt").exists()

    store = CacheStore.open(cache_root)
    try:
        store.conn.execute(
            "UPDATE entries SET tombstoned_at = ? WHERE key = ?",
            (1, key),
        )
        store.conn.commit()
    finally:
        store.close()

    summary = gc_cache(
        cache_root=str(cache_root),
        delete=True,
        grace_seconds=10,
    )
    assert summary["deleted"] == [key_hex]
    assert not (cache_root / key_hex[:2] / key_hex[2:]).exists(), (
        "output_root should have been unlinked after grace elapsed"
    )


def test_a_promoted_file_names_its_type(tmp_path):
    from metasmith.caching.invocation import read_manifest

    cache_root = tmp_path / "task_cache"
    records = promote_members(
        cwd=_task_dir(tmp_path, "t1"), entries=[_entry()], meta=META,
        cache_root=cache_root, successes=[True],
    )
    assert records[0]["status"] == "promoted"
    manifest = read_manifest(shard_dir(cache_root, KEY))
    assert manifest is not None
    assert [f["dtype_name"] for f in manifest["files"]] == ["mock::step_a"]


def test_a_slot_with_no_name_promotes_anyway(tmp_path):
    # Shards written before the compiler carried a name are still readable;
    # what they cannot say is which type they hold.
    from dataclasses import replace
    from metasmith.caching.invocation import read_manifest

    meta = replace(META, slot_files=[
        {"dtype_key": "step_a", "ext": ".txt", "branch_idx": 0, "slot_id": "a" * 64},
    ])
    cache_root = tmp_path / "task_cache"
    records = promote_members(
        cwd=_task_dir(tmp_path, "t1"), entries=[_entry()], meta=meta,
        cache_root=cache_root, successes=[True],
    )
    assert records[0]["status"] == "promoted"
    manifest = read_manifest(shard_dir(cache_root, KEY))
    assert [f["dtype_name"] for f in manifest["files"]] == [""]


def test_a_directory_product_is_linked_into_the_shard(tmp_path):
    cwd = tmp_path / "t1"
    product = cwd / "1-1-1.abcdef-step_a.txt"
    (product / "sub").mkdir(parents=True)
    (product / "sub" / "part.bin").write_text("bulk")
    cache_root = tmp_path / "task_cache"

    records = promote_members(
        cwd=cwd, entries=[_entry()], meta=META, cache_root=cache_root, successes=[True],
    )

    assert records[0]["status"] == "promoted"
    stored = shard_dir(cache_root, KEY) / "out" / "1-1-1.abcdef-step_a.txt" / "sub" / "part.bin"
    assert stored.stat().st_ino == (product / "sub" / "part.bin").stat().st_ino
