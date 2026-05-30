"""Tests for the post-exec promote step + S6 FS detection.

Coverage: S5 (lockfile + orphan recovery + tmp promote/discard), S8
(gc tombstone delay), S6 (network FS detection + straddle-mount refusal).
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

import pytest


def test_promote_lockfile_orphan_recovery(tmp_path):
    """S5: a dead-PID lockfile gets cleaned then promote proceeds.

    Pre-place a lockfile whose PID does not exist on this host; the
    promote routine must recognize it as stale, remove it, acquire the
    lock, and complete.
    """
    from metasmith.caching.promote import _acquire_lock

    cache_root = tmp_path / "task_cache"
    cache_root.mkdir()
    key_hex = "abc123"
    # PID 99999999 is reliably non-existent under Linux's default pid_max.
    lock = cache_root / f"{key_hex}.lock"
    lock.write_text(
        f"99999999 {__import__('socket').gethostname()} {time.time():.6f}\n"
    )
    acquired = _acquire_lock(cache_root, key_hex)
    assert acquired is not None, "stale-pid lock was not reclaimed"
    assert lock.exists(), "lock file should be re-created by the reclaim path"


def test_promote_picks_up_orphan_tmp(tmp_path):
    """S5: orphan <key>.tmp/ with a sentinel manifest gets promoted.

    Captures mid-run-kill recovery: a previous interrupted run left a
    complete <key>.tmp/ on disk; the next run's promote pass detects
    it via manifest.cbor and finishes the rename.
    """
    from metasmith.caching.promote import recover_orphan_tmp_dirs

    cache_root = tmp_path / "task_cache"
    cache_root.mkdir()
    key_hex = "deadbeef"
    tmp = cache_root / f"{key_hex}.tmp"
    (tmp / "out").mkdir(parents=True)
    (tmp / "out" / "f.txt").write_text("payload")
    (tmp / "manifest.cbor").write_bytes(b"\x00manifest")

    actions = recover_orphan_tmp_dirs(cache_root)
    assert actions == {key_hex: "promoted"}
    assert not tmp.exists(), "orphan .tmp should have been renamed"
    final = cache_root / key_hex[:2] / key_hex[2:]
    assert (final / "manifest.cbor").exists()


def test_promote_discards_incomplete_tmp(tmp_path):
    """S5: orphan <key>.tmp/ WITHOUT a manifest.cbor is deleted.

    A .tmp dir that doesn't have the sentinel cbor file represents a
    truly-interrupted write; the next pass should remove it rather than
    promote it.
    """
    from metasmith.caching.promote import recover_orphan_tmp_dirs

    cache_root = tmp_path / "task_cache"
    cache_root.mkdir()
    key_hex = "feedface"
    tmp = cache_root / f"{key_hex}.tmp"
    (tmp / "out").mkdir(parents=True)
    (tmp / "out" / "partial.txt").write_text("incomplete")

    actions = recover_orphan_tmp_dirs(cache_root)
    assert actions == {key_hex: "deleted"}
    assert not tmp.exists()
    assert not (cache_root / key_hex[:2] / key_hex[2:]).exists()


def test_network_fs_uses_copy_strategy(tmp_path, monkeypatch):
    """S6: a Lustre mount detected -> 'copy' strategy returned.

    Mock /proc/self/mountinfo to claim the cache_root is on lustre and
    assert detect_strategy returns 'copy'.
    """
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
    """S6: cache_root and workDir on different mounts -> compile refuses.

    The rename in promote is only atomic on the same FS. Compiling a
    workflow where the two roots straddle mount boundaries must surface
    a structured error.
    """
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


@pytest.mark.xfail(strict=True, reason="S8 GC tombstone delay not landed")
def test_gc_tombstone_delay(tmp_path):
    """S8: tombstoning an entry K leaves it readable for the grace period.

    Tombstone, then an in-flight materialize against K must succeed;
    only after the 24h grace expires should the unlink run.
    """
    from metasmith.caching.store import CacheStore  # noqa: F401

    assert False
