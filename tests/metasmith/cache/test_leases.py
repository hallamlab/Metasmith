"""A run's deletions leave the shards another live run on the same store leased."""

from __future__ import annotations

import time
from pathlib import Path

from metasmith.caching import leases
from metasmith.caching.layout import shard_dir

KEY = "1e20" + "ab" * 32
NAME = "1-1-1.abcdef-out0.txt"


def _product(cache_root: Path) -> Path:
    out = shard_dir(cache_root, KEY) / "out"
    out.mkdir(parents=True)
    (out / NAME).write_text("payload")
    return out / NAME


def _live(cache_root: Path, run: str, beat: int | None = None) -> None:
    leases._beat(cache_root, run)
    if beat is not None:
        with leases._locked(cache_root) as conn:
            conn.execute("UPDATE runs SET beat = ? WHERE run = ?", (beat, run))


def test_a_read_lease_defers_the_deletion_to_its_release(tmp_path):
    path = _product(tmp_path)
    _live(tmp_path, "y")
    leases.take(tmp_path, "y", [KEY], keep=False)
    assert leases.delete(tmp_path, "x", [(KEY, NAME)]) == ["deferred"]
    assert path.exists()
    assert leases.release(tmp_path, "y") == 1
    assert not path.exists()
    assert list(path.parent.iterdir()) == []


def test_a_keep_lease_drops_the_deletion(tmp_path):
    path = _product(tmp_path)
    _live(tmp_path, "y")
    leases.take(tmp_path, "y", [KEY], keep=False)
    assert leases.delete(tmp_path, "x", [(KEY, NAME)]) == ["deferred"]
    leases.take(tmp_path, "y", [KEY], keep=True)
    assert leases.delete(tmp_path, "x", [(KEY, NAME)]) == ["dropped"]
    leases.release(tmp_path, "y")
    assert path.exists(), "the run that kept it released, and a deferred deletion took it anyway"


def test_a_run_ignores_its_own_lease(tmp_path):
    path = _product(tmp_path)
    _live(tmp_path, "x")
    leases.take(tmp_path, "x", [KEY], keep=True)
    assert leases.delete(tmp_path, "x", [(KEY, NAME)]) == ["deleted"]
    assert not path.exists()


def test_a_dead_holder_pins_nothing(tmp_path):
    path = _product(tmp_path)
    _live(tmp_path, "y", beat=int(time.time()) - leases.TTL_S - 1)
    leases.take(tmp_path, "y", [KEY], keep=True)
    assert leases.live_keys(tmp_path) == set()
    assert leases.delete(tmp_path, "x", [(KEY, NAME)]) == ["deleted"]
    assert not path.exists()
    leases.release(tmp_path, "x")
    with leases._locked(tmp_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM leases").fetchone() == (0,), "a dead run's leases outlived a release"


def test_held_beats_and_releases(tmp_path):
    with leases.held(tmp_path, "y", beat_s=0.05):
        leases.take(tmp_path, "y", [KEY], keep=False)
        assert leases.live_keys(tmp_path) == {KEY}
        time.sleep(0.2)
    assert leases.live_keys(tmp_path) == set()
