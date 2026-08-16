"""Deterministic sharding, the attempted-ids sidecar, and the durable cache.

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

THE SHARD SPEC USED TO BE PART OF WHAT A SIDECAR MEANT, and it no longer is. Assignment is
`crc32(mnxr) % n`, so a sidecar written under 6 shards names a different set under 16, and
a single file read as "this shard's attempted set" would skip ids that are no longer its
own and re-attempt ids that are. That was answered by REFUSING across a spec change, which
turned a re-shard into a full re-map of a lane that had been killed two thirds of the way
through -- the exact situation the sidecar exists for.

The partition is RECOMPUTABLE, so the answer is to recompute it: read EVERY sidecar of the
prior run, union the ids, and re-filter them onto this run's spec. What a file's header
says stops mattering, because the ids themselves carry the partition. A prior shard whose
file is absent contributes nothing and its ids are simply re-attempted -- work, not a
coverage hole, which is the direction a resume is allowed to be wrong in.

THE CACHE IS KEYED ON THE SUBMISSION STRING, NOT ON THE REACTION ID. A reaction has one
MNXR and, across this graph's three submission classes, several possible strings: the
whole reaction, the stoichiometric collapse of it, the rescue's completion of it, an
element reduction of it. Serving a cached map for the wrong one is not a stale row, it is
a map of a different molecule filed under this one's name -- so a cached row is reused only
when the string it was produced from is the string this run would send.
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


def read_sidecars(paths, shard, who="shard") -> set[str]:
    """Every id ANY prior shard recorded as attempted, re-partitioned onto this run's spec.

    Pass all of the prior run's sidecars plus this run's own. The union is what makes the
    re-partition sound: filtering one file onto a new `n` would drop ids that now belong
    here but were attempted under another file's number, and those are precisely the ones
    a resume must not re-offer to a search that hung on them.

    A file whose header names a different spec is READ, not refused -- see the module
    docstring. Missing files are skipped: their ids get re-attempted, which costs work and
    loses nothing.
    """
    ids, seen_specs = set(), set()
    for p in paths or ():
        p = Path(p)
        if not p.exists() or p.stat().st_size == 0:
            continue
        lines = [l.strip() for l in p.read_text().splitlines() if l.strip()]
        if lines and lines[0].startswith("#shard"):
            seen_specs.add(lines[0])
            lines = lines[1:]
        else:
            # No header. Under the old rule this was unknowable and refused; under the
            # union rule an id is an id and the partition is recomputed from it.
            seen_specs.add("#shard unrecorded")
        ids |= set(lines)
    if not ids:
        return set()
    kept = {m for m in ids if shard is None or shard_of(m, shard[1]) == shard[0]}
    note = ""
    if seen_specs - {spec_header(shard)}:
        note = (f" (re-partitioned from {sorted(seen_specs)} onto "
                f"{spec_header(shard)})")
    print(f"[{who}] {len(ids):,} ids attempted by a prior run, {len(kept):,} of them "
          f"this shard's{note}", flush=True)
    return kept


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


# =====================================================================
# the durable cache
# =====================================================================
# A member's cache lives in the Nextflow task work directory, which on the cluster is
# node-local scratch and is discarded on retry -- so its per-reaction resume protected a
# run against nothing that actually happens. The cache directory is the same file, staged
# as an input, and the only thing that makes a killed run resume rather than restart.

CACHE_NAME = "cache.tsv"
SIDECAR_GLOB = "*.attempted"


def cache_files(cache_dir) -> tuple[list[Path], list[Path]]:
    """`(caches, sidecars)` under a staged member cache directory, or two empty lists.

    An ABSENT or EMPTY directory is the ordinary first-run state and is not an error: the
    cache is a staged given with no producer, so a graph that has never run stages an
    empty one and every member simply has nothing to resume from.
    """
    if not cache_dir:
        return [], []
    d = Path(cache_dir)
    if not d.is_dir():
        return [], []
    caches = sorted(p for p in d.rglob(CACHE_NAME) if p.stat().st_size > 0)
    caches += sorted(p for p in d.glob("*.tsv")
                     if p.name != CACHE_NAME and p.stat().st_size > 0)
    sidecars = sorted(d.rglob(SIDECAR_GLOB))
    return caches, sidecars


def read_cache(paths, universe: dict, who="cache"):
    """Prior rows whose SUBMISSION STRING is the one this run would send.

    Returns `(rows, n_stale, n_foreign)` where `rows` is a list of the raw TSV fields in
    the file's own column order, ready to be carried forward into this run's cache.

    THE STRING IS HALF THE KEY. `mnxr` alone identifies a reaction, not a submission, and
    this graph now sends up to four different strings for one reaction -- whole, collapsed,
    rescue-completed, element-reduced. A row keyed on the id alone would serve the map of
    whichever string ran last, which is not staleness but a wrong answer wearing the right
    id. `n_stale` counts rows dropped for exactly that reason and it is worth printing:
    a large number after a method change is the cache doing its job.
    """
    rows, n_stale, n_foreign = [], 0, 0
    seen = set()
    for p in paths or ():
        p = Path(p)
        if not p.exists() or p.stat().st_size == 0:
            continue
        with open(p) as fh:
            header = fh.readline().rstrip("\n").split("\t")
            try:
                i_id, i_smi = header.index("mnxr"), header.index("rxn_smiles")
            except ValueError:
                continue
            for line in fh:
                f = line.rstrip("\n").split("\t")
                if len(f) <= max(i_id, i_smi):
                    continue
                key = f[i_id]
                if key in seen:
                    continue
                want = universe.get(key)
                if want is None:
                    n_foreign += 1
                    continue
                if f[i_smi] != want:
                    n_stale += 1
                    continue
                seen.add(key)
                rows.append(f)
    if rows or n_stale or n_foreign:
        print(f"[{who}] cache: {len(rows):,} rows reusable, {n_stale:,} stale "
              f"(the submission string changed), {n_foreign:,} for submissions this "
              f"shard was not given", flush=True)
    return rows, n_stale, n_foreign
