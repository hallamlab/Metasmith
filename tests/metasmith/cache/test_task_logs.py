"""A grouped task's logs reach every member shard, but occupy disk once."""

from __future__ import annotations

from pathlib import Path

from metasmith.caching.promote import TASK_LOG_NAMES, _copy_task_logs


def _task_dir(tmp_path: Path) -> Path:
    task_dir = tmp_path / "nxf_work" / "ab" / "cdef"
    task_dir.mkdir(parents=True)
    for name in TASK_LOG_NAMES:
        (task_dir / name).write_text(f"body of {name}")
    return task_dir


def test_later_members_link_the_first_members_logs(tmp_path):
    task_dir = _task_dir(tmp_path)
    shards = [tmp_path / "task_cache" / s for s in ("aa", "bb", "cc")]
    placed: dict[Path, Path] = {}
    for shard in shards:
        shard.mkdir(parents=True)
        _copy_task_logs(task_dir, shard, placed)

    for name in TASK_LOG_NAMES:
        inodes = {(shard / "logs" / name).stat().st_ino for shard in shards}
        assert len(inodes) == 1, f"{name} was copied per member, not linked"
        assert (shards[2] / "logs" / name).read_text() == f"body of {name}"
    assert (task_dir / TASK_LOG_NAMES[0]).stat().st_ino not in {
        (shards[0] / "logs" / TASK_LOG_NAMES[0]).stat().st_ino
    }, "a shard's log shares the task dir's inode, so pruning the task dir would not free it"


def test_a_shard_that_has_logs_keeps_them(tmp_path):
    task_dir = _task_dir(tmp_path)
    shard = tmp_path / "task_cache" / "aa"
    (shard / "logs").mkdir(parents=True)
    (shard / "logs" / ".command.out").write_text("earlier run")
    _copy_task_logs(task_dir, shard, {})
    assert (shard / "logs" / ".command.out").read_text() == "earlier run"
    assert not (shard / "logs" / ".command.err").exists()
