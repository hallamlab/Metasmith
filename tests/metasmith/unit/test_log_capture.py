from __future__ import annotations
from pathlib import Path

from metasmith.caching.promote import _copy_task_logs
from metasmith.models.lineage import LogBundle


def test_the_producing_tasks_logs_land_in_the_member_shard(tmp_path):
    task_dir = tmp_path / "nxf_work" / "ab" / "cd"
    task_dir.mkdir(parents=True)
    for ext in ("sh", "out", "err", "log"):
        (task_dir / f".command.{ext}").write_text(f"<{ext}>")
    (task_dir / "output.fa").write_text(">c1\nACGT\n")
    shard = tmp_path / "task_cache" / "1e" / "20ab"
    shard.mkdir(parents=True)

    _copy_task_logs(task_dir, shard)
    names = sorted(p.name for p in (shard / "logs").iterdir())
    assert names == sorted(
        [".command.err", ".command.log", ".command.out", ".command.sh"]
    )

    # A shard that already has logs keeps them: the first producer's run is
    # the one the shard's products came from.
    (task_dir / ".command.out").write_text("<later run>")
    _copy_task_logs(task_dir, shard)
    assert (shard / "logs" / ".command.out").read_text() == "<out>"


def test_log_bundle_status_discriminates_available_vs_pruned():
    available = LogBundle(
        status="available",
        stdout=Path("/cache/00/aa/logs/.command.out"),
        stderr=Path("/cache/00/aa/logs/.command.err"),
    )
    pruned = LogBundle(status="pruned", reason="cache GC removed shard")
    legacy = LogBundle(status="legacy_shard_no_logs")
    remote = LogBundle(status="remote_cache_no_logs")
    missing = LogBundle(status="missing", reason="shard not found")
    not_app = LogBundle(status="not_applicable")

    assert available.is_available()
    for b in (pruned, legacy, remote, missing, not_app):
        assert not b.is_available()
    assert pruned.reason and "GC" in pruned.reason
