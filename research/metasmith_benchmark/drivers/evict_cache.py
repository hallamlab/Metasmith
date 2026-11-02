#!/usr/bin/env python3
"""Evict chosen task-cache entries from one agent home's store, to reclaim bytes.

Each key is tombstoned in the index and on its shard, then its shard directory is removed and its
row deleted, the same steps `metasmith cache gc --delete` takes. Only the listed keys are touched,
so tombstones other callers left in their grace period survive. An evicted entry reads as a cache
miss and recomputes if a later plan asks for it.

Dry run by default. Imports are refused: an import may be the only copy of its data.

    evict_cache.py --home <agent home> --keys <file of hex keys> [--protect-run KEY ...] [--apply]
"""

import argparse
import shutil
import sqlite3
import time
from pathlib import Path

from metasmith.caching.admission import IMPORTED
from metasmith.caching.promote import tombstone_shard
from metasmith.caching.store import CacheStore
from metasmith.ops.cache import _inside, resolve_store_root


def _with_retry(fn, tries=30):
    for attempt in range(tries):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            if "locked" not in str(e) or attempt == tries - 1:
                raise
            time.sleep(2)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--home", required=True)
    ap.add_argument("--keys", required=True, type=Path)
    ap.add_argument("--protect-run", action="append", default=[])
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    root = resolve_store_root(agent_home=args.home)
    wanted = list(dict.fromkeys(args.keys.read_text().split()))
    bad = [k for k in wanted if len(k) % 2 or any(ch not in "0123456789abcdef" for ch in k)]
    if bad:
        raise SystemExit(f"not hex keys, nothing touched: {bad[:3]}")
    store = CacheStore.open(root)
    freed = 0
    try:
        for key_hex in wanted:
            e = store.probe(bytes.fromhex(key_hex))
            if e is None:
                print(f"skip {key_hex[:12]}: absent or already tombstoned")
                continue
            if e.origin == IMPORTED or e.run in args.protect_run:
                print(f"REFUSE {key_hex[:12]}: origin={e.origin} run={e.run}")
                continue
            if not _inside(root, e.output_root):
                print(f"REFUSE {key_hex[:12]}: output_root {e.output_root} is outside {root}")
                continue
            print(f"{'evict' if args.apply else 'would evict'} {key_hex[:12]} run={e.run} {e.size_bytes} B {e.output_root}")
            freed += e.size_bytes
            if not args.apply:
                continue
            _with_retry(lambda: store.tombstone(e.key))
            tombstone_shard(root, key_hex, origin=e.origin)
            shutil.rmtree(e.output_root, ignore_errors=True)
            _with_retry(lambda: (store.conn.execute("DELETE FROM entries WHERE key = ?", (e.key,)), store.conn.commit()))
    finally:
        store.close()
    print(f"{'evicted' if args.apply else 'would evict'} {freed} B ({freed / 2**30:.1f} GiB) from {root}")


if __name__ == "__main__":
    main()
