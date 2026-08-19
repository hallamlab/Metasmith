from __future__ import annotations
from pathlib import Path

from metasmith.caching.promote import _find_step_logs
from metasmith.models.lineage import LogBundle


def test_find_step_logs_locates_command_files(tmp_path):
    ws = tmp_path / "ws"
    step_dir = ws / "nxf_work" / "step_03" / "ab" / "cd"
    step_dir.mkdir(parents=True)
    for ext in ("sh", "out", "err", "log"):
        (step_dir / f".command.{ext}").write_text(f"<{ext}>")
    (step_dir / "output.fa").write_text(">c1\nACGT\n")

    found = _find_step_logs(ws, 3)
    names = sorted(p.name for p in found)
    assert names == sorted(
        [".command.err", ".command.log", ".command.out", ".command.sh"]
    )
    assert _find_step_logs(ws, 4) == []


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
