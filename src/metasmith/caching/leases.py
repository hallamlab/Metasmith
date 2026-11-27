"""Which shards a live run reads or keeps, so another run's deletions leave them be.

Several runs share one agent home and one store, and each deletes the products
its own targets do not name (`Orchestrator._maybeDelete`). A product one run
deletes may be another run's cache hit, its input between its probe and its
real pass, or its target. So a run leases every member key it decides, before
it reads the shard's state (`invocation.probe_members`), and a deletion skips a
key another live run holds. Both happen inside one `BEGIN IMMEDIATE` on the
store's sqlite, so a lease either lands before the deletion looks, and the
deletion is skipped, or after the file is gone, and the lease holder sees a
ghost. A skipped deletion of a read lease is deferred and done when the last
holder releases; one of a keep lease is dropped, since that run means to keep it.

A run is live while its driver beats (`held`). One whose beat is older than
`TTL_S` is dead, and its leases bind nobody.

Stdlib only: the member-key helper imports this and must start fast.
"""

from __future__ import annotations

import os
import shutil
import socket
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from .layout import GHOSTS_MARK, shard_dir

TTL_S = 600
BEAT_S = 60

_CREATE_SQL = [
    "CREATE TABLE IF NOT EXISTS runs(run TEXT PRIMARY KEY, host TEXT NOT NULL, beat INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS leases(run TEXT NOT NULL, key TEXT NOT NULL, keep INTEGER NOT NULL, PRIMARY KEY (run, key))",
    "CREATE INDEX IF NOT EXISTS idx_leases_key ON leases(key)",
    "CREATE TABLE IF NOT EXISTS deferred(key TEXT NOT NULL, name TEXT NOT NULL, PRIMARY KEY (key, name))",
]


def _connect(cache_root: Path) -> sqlite3.Connection:
    cache_root = Path(cache_root)
    cache_root.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(cache_root / "cache.sqlite", timeout=300, isolation_level=None)
    # Mirrors CacheStore.open: the journal_mode marker is how an NFS store opts out of WAL.
    marker = cache_root / "journal_mode"
    conn.execute(f"PRAGMA journal_mode = {marker.read_text().strip() if marker.exists() else 'WAL'}")
    # A lease lost to a power cut is a lease nobody holds any more anyway; skipping
    # the per-commit fsync keeps every decision of every run off the disk's latency.
    conn.execute("PRAGMA synchronous = NORMAL")
    for stmt in _CREATE_SQL:
        conn.execute(stmt)
    return conn


@contextmanager
def _locked(cache_root: Path):
    conn = _connect(cache_root)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


def _others(conn, key: str, run: str) -> tuple[bool, bool]:
    live, keep = conn.execute(
        "SELECT COUNT(*), MAX(l.keep) FROM leases l JOIN runs r ON r.run = l.run"
        " WHERE l.key = ? AND l.run != ? AND r.beat > ?",
        (key, run, int(time.time()) - TTL_S),
    ).fetchone()
    return live > 0, bool(keep)


def take(cache_root: Path, run: str, keys, keep: bool) -> None:
    keys = sorted(set(keys) - {"-"})
    if not run or not keys:
        return
    with _locked(cache_root) as conn:
        conn.executemany(
            "INSERT INTO leases(run, key, keep) VALUES (?, ?, ?)"
            " ON CONFLICT(run, key) DO UPDATE SET keep = MAX(keep, excluded.keep)",
            [(run, k, int(keep)) for k in keys],
        )
        if keep:
            conn.executemany("DELETE FROM deferred WHERE key = ?", [(k,) for k in keys])


def _unlink(cache_root: Path, key: str, name: str) -> Path | None:
    path = shard_dir(Path(cache_root), key) / "out" / name
    if not os.path.lexists(path):
        return None
    # Marked before the rename, so no ghost exists without the mark; renamed away
    # under the lock and removed after it, so a driver killed mid-delete leaves
    # the product absent rather than half there and still probing as a hit.
    (Path(cache_root) / GHOSTS_MARK).touch()
    gone = path.with_name(f".msm-gone.{name}.{time.time_ns()}")
    os.rename(path, gone)
    return gone


def _rmtree(paths: list[Path]) -> None:
    for p in paths:
        if p.is_dir() and not p.is_symlink():
            shutil.rmtree(p, ignore_errors=True)
        else:
            p.unlink(missing_ok=True)


def delete(cache_root: Path, run: str, items) -> list[str]:
    """`deleted`, `absent`, `deferred` or `dropped` per `(key, name)`."""
    out, trash = [], []
    with _locked(cache_root) as conn:
        for key, name in items:
            live, keep = _others(conn, key, run)
            if live:
                if not keep:
                    conn.execute("INSERT OR IGNORE INTO deferred(key, name) VALUES (?, ?)", (key, name))
                out.append("dropped" if keep else "deferred")
                continue
            gone = _unlink(cache_root, key, name)
            out.append("absent" if gone is None else "deleted")
            if gone is not None:
                trash.append(gone)
    _rmtree(trash)
    return out


def release(cache_root: Path, run: str) -> int:
    """Drop `run`'s leases and every dead run's, then do the deferred deletions nobody holds."""
    trash = []
    with _locked(cache_root) as conn:
        dead = int(time.time()) - TTL_S
        conn.execute("DELETE FROM runs WHERE run = ? OR beat <= ?", (run, dead))
        conn.execute("DELETE FROM leases WHERE run NOT IN (SELECT run FROM runs)")
        for key, name in conn.execute("SELECT key, name FROM deferred").fetchall():
            if _others(conn, key, "")[0]:
                continue
            conn.execute("DELETE FROM deferred WHERE key = ? AND name = ?", (key, name))
            gone = _unlink(cache_root, key, name)
            if gone is not None:
                trash.append(gone)
    _rmtree(trash)
    return len(trash)


def live_keys(cache_root: Path) -> set[str]:
    conn = _connect(cache_root)
    try:
        rows = conn.execute(
            "SELECT DISTINCT l.key FROM leases l JOIN runs r ON r.run = l.run WHERE r.beat > ?",
            (int(time.time()) - TTL_S,),
        ).fetchall()
    finally:
        conn.close()
    return {k for (k,) in rows}


def _beat(cache_root: Path, run: str) -> None:
    with _locked(cache_root) as conn:
        conn.execute(
            "INSERT INTO runs(run, host, beat) VALUES (?, ?, ?)"
            " ON CONFLICT(run) DO UPDATE SET beat = excluded.beat",
            (run, socket.gethostname(), int(time.time())),
        )


@contextmanager
def held(cache_root: Path, run: str, beat_s: float = BEAT_S):
    """Keep `run` live for the block, then release it."""
    _beat(cache_root, run)
    stop = threading.Event()

    def loop():
        while not stop.wait(beat_s):
            try:
                _beat(cache_root, run)
            except sqlite3.Error:
                continue

    thread = threading.Thread(target=loop, name="msm-run-lease", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=5)
        release(cache_root, run)
