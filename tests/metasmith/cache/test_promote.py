from __future__ import annotations

import shutil
import time
from pathlib import Path

import pytest


def test_promote_lockfile_orphan_recovery(tmp_path):
    from metasmith.caching.promote import _acquire_lock

    cache_root = tmp_path / "task_cache"
    cache_root.mkdir()
    key_hex = "abc123"
    lock = cache_root / f"{key_hex}.lock"
    lock.write_text(
        f"99999999 {__import__('socket').gethostname()} {time.time():.6f}\n"
    )
    acquired = _acquire_lock(cache_root, key_hex)
    assert acquired is not None, "stale-pid lock was not reclaimed"
    assert lock.exists(), "lock file should be re-created by the reclaim path"


def test_promote_picks_up_orphan_tmp(tmp_path):
    from metasmith.caching.promote import recover_orphan_tmp_dirs

    cache_root = tmp_path / "task_cache"
    cache_root.mkdir()
    key_hex = "deadbeef"
    tmp = cache_root / f"{key_hex}.tmp"
    (tmp / "out").mkdir(parents=True)
    (tmp / "out" / "f.txt").write_text("payload")
    (tmp / "manifest.cbor").write_bytes(b"\x00manifest")

    actions = recover_orphan_tmp_dirs(cache_root, [key_hex])
    assert actions == {key_hex: "promoted"}
    assert not tmp.exists(), "orphan .tmp should have been renamed"
    final = cache_root / key_hex[:2] / key_hex[2:]
    assert (final / "manifest.cbor").exists()


def test_promote_discards_incomplete_tmp(tmp_path):
    from metasmith.caching.promote import recover_orphan_tmp_dirs

    cache_root = tmp_path / "task_cache"
    cache_root.mkdir()
    key_hex = "feedface"
    tmp = cache_root / f"{key_hex}.tmp"
    (tmp / "out").mkdir(parents=True)
    (tmp / "out" / "partial.txt").write_text("incomplete")

    actions = recover_orphan_tmp_dirs(cache_root, [key_hex])
    assert actions == {key_hex: "deleted"}
    assert not tmp.exists()
    assert not (cache_root / key_hex[:2] / key_hex[2:]).exists()


def test_reclaim_leaves_another_runs_staging_alone(tmp_path):
    from metasmith.caching.promote import recover_orphan_tmp_dirs

    cache_root = tmp_path / "task_cache"
    cache_root.mkdir()

    mine = cache_root / "aaaa1111.tmp"
    (mine / "out").mkdir(parents=True)
    (mine / "out" / "partial.txt").write_text("mine, abandoned")

    theirs = cache_root / "bbbb2222.tmp"
    (theirs / "out").mkdir(parents=True)
    (theirs / "out" / "big.bin").write_text("another run, still writing")

    actions = recover_orphan_tmp_dirs(cache_root, ["aaaa1111"])

    assert actions == {"aaaa1111": "deleted"}
    assert not mine.exists()
    assert (theirs / "out" / "big.bin").read_text() == "another run, still writing"


def test_promote_run_reclaims_only_its_own_keys(tmp_path, monkeypatch):
    from metasmith.caching import promote as promote_mod

    workspace = tmp_path / "ws"
    workspace.mkdir()
    cache_root = tmp_path / "task_cache"
    cache_root.mkdir()

    (workspace / "workflow.step_00.meta").write_text("{}")

    spec = promote_mod.StepPromoteSpec(
        order=0,
        cache_key=bytes.fromhex("aaaa1111"),
        cacheable=True,
        transform_key="t",
        signature="s",
        out_identities={},
        dep_out=[],
    )
    monkeypatch.setattr(promote_mod, "_read_step_meta", lambda _p: spec)

    mine = cache_root / "aaaa1111.tmp"
    (mine / "out").mkdir(parents=True)
    (mine / "out" / "partial.txt").write_text("mine, abandoned")

    theirs = cache_root / "bbbb2222.tmp"
    (theirs / "out").mkdir(parents=True)
    (theirs / "out" / "big.bin").write_text("another run, still writing")

    summary = promote_mod.promote_run(workspace=workspace, cache_root=cache_root)

    assert summary["orphan_recovery"] == {"aaaa1111": "deleted"}
    assert (theirs / "out" / "big.bin").exists(), (
        "promote_run swept a .tmp belonging to another run"
    )


def test_network_fs_uses_copy_strategy(tmp_path, monkeypatch):
    from metasmith.caching import fs as fs_module

    fake_mp = str(tmp_path)
    fake_mountinfo = (
        f"100 1 0:42 / {fake_mp} rw,relatime shared:1 - lustre lustre rw\n"
        "1 0 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw\n"
    )
    monkeypatch.setattr(
        Path, "read_text", lambda self, *a, **k: (
            fake_mountinfo if str(self) == "/proc/self/mountinfo"
            else _orig_read_text(self, *a, **k)
        ),
    )
    fs_module._read_mountinfo.cache_clear()

    cache_root = tmp_path / "task_cache"
    cache_root.mkdir()
    assert fs_module.detect_strategy(cache_root) == "copy"
    fs_module._read_mountinfo.cache_clear()


def test_straddle_mount_init_fails(tmp_path, monkeypatch):
    from metasmith.caching import fs as fs_module

    cache_dir = tmp_path / "a"
    work_dir = tmp_path / "b"
    cache_dir.mkdir()
    work_dir.mkdir()
    fake_mountinfo = (
        f"100 1 0:42 / {cache_dir} rw,relatime - ext4 /dev/sda1 rw\n"
        f"200 1 0:43 / {work_dir} rw,relatime - lustre lustre rw\n"
        "1 0 8:1 / / rw,relatime - ext4 /dev/sda1 rw\n"
    )
    monkeypatch.setattr(
        Path, "read_text", lambda self, *a, **k: (
            fake_mountinfo if str(self) == "/proc/self/mountinfo"
            else _orig_read_text(self, *a, **k)
        ),
    )
    fs_module._read_mountinfo.cache_clear()

    with pytest.raises(fs_module.StraddleMountError):
        fs_module.assert_same_mount(cache_dir, work_dir)
    fs_module._read_mountinfo.cache_clear()


_orig_read_text = Path.read_text


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
