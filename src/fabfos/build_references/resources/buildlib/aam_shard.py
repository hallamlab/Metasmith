"""Deterministic sharding and the attempted-ids sidecar, shared by the mapper lanes.

WHY THIS IS ONE MODULE AND NOT TWO COPIES. The sidecar discipline was written for Indigo,
whose compiled search can hang where no signal reaches it. RXNMapper does not hang -- it
gets OOM-killed, and the effect on a resume is the same: the run dies inside a reaction
having written nothing about it, and the next run picks the same reaction and dies again.
The fix is identical in both places, and a fix that exists twice is a fix that will be
corrected once.

THE ORDERING IS THE ENTIRE TRICK: the reaction id is written and FLUSHED before the
attempt, never after. A run that dies mid-reaction leaves that id as the last line of the
sidecar, so the next run knows exactly which reaction to skip and loses nothing but the
one it was on.

THE SHARD SPEC IS PART OF WHAT A SIDECAR MEANS. Assignment is `crc32(mnxr) % n`, so
changing `n` re-partitions the universe: a sidecar written under 6 shards names a
different set under 16 -- shard 0 would skip ids that are no longer its own and
re-attempt ids that are. That is not a crash, it is a silent coverage hole in the one
file whose job is to guarantee there isn't one. So the spec is the sidecar's first line
and a mismatch REFUSES.
"""
from __future__ import annotations

import os
import zlib
from pathlib import Path


def shard_of(mnxr: str, n: int) -> int:
    """Stable across processes and python versions -- `hash()` is not (PYTHONHASHSEED),
    and a shard assignment that moves between runs makes resume meaningless."""
    return zlib.crc32(mnxr.encode()) % n


def parse_spec(text: str | None):
    """`"i/n"` -> `(i, n)`, or None."""
    if not text:
        return None
    i, n = text.split("/")
    i, n = int(i), int(n)
    if not (0 <= i < n):
        raise SystemExit(f"[shard] '{text}' is not a valid i/n")
    return i, n


def spec_header(shard) -> str:
    return f"#shard {shard[0]}/{shard[1]}" if shard else "#shard none"


def select(universe: dict, shard) -> dict:
    if not shard:
        return universe
    i, n = shard
    return {m: s for m, s in universe.items() if shard_of(m, n) == i}


def read_sidecar(path: Path, shard, who="shard") -> set[str]:
    """Ids already ATTEMPTED under this exact partition, or refuse."""
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        return set()
    spec = spec_header(shard)
    lines = [l.strip() for l in path.read_text().splitlines() if l.strip()]
    head = lines[0] if lines and lines[0].startswith("#shard") else None
    if head is None:
        raise SystemExit(
            f"[{who}] {path} carries no shard header. It was written before the spec was "
            f"recorded, so which partition it describes is unknowable -- delete it and "
            f"re-run rather than resume against a guess.")
    if head != spec:
        raise SystemExit(
            f"[{who}] {path} was written under '{head}' and this run is '{spec}'. The "
            f"assignment is crc32 % n, so a different n is a different partition and "
            f"resuming across the change leaves a silent coverage hole. Delete the "
            f"sidecar and its cache, or re-run with the original n.")
    return set(lines[1:])


class Sidecar:
    """Append-only attempted-ids log. `mark(id)` writes and fsyncs BEFORE the attempt."""

    def __init__(self, path: Path, shard):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fresh = not path.exists() or path.stat().st_size == 0
        self.fh = open(path, "a", buffering=1)
        if fresh:
            self.fh.write(spec_header(shard) + "\n")
            self.fh.flush()

    def mark(self, mnxr: str):
        self.fh.write(mnxr + "\n")
        os.fsync(self.fh.fileno())

    def close(self):
        try:
            self.fh.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
