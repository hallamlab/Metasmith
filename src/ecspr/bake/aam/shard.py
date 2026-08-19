from __future__ import annotations

import os
import zlib
from pathlib import Path


def shard_of(mnxr: str, n: int) -> int:
    return zlib.crc32(mnxr.encode()) % n


def parse_spec(text: str | None):
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
