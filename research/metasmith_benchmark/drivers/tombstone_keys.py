"""Tombstone chosen product shards on disk, so probe misses them and the step recomputes.

    tombstone_keys.py <cache root> <file of hex keys> [--apply]

Use it for shards the index does not hold yet, which evict_cache.py cannot reach. Each key must
name an existing lineage shard. --apply touches the tombstone and leaves the shard's files, manifest
and logs in place. The next write_shard at that key replaces the shard.
"""
import argparse
import sys
from pathlib import Path

from metasmith.caching.admission import shard_for
from metasmith.caching.invocation import TOMBSTONE_NAME
from metasmith.caching.promote import tombstone_shard


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cache_root", type=Path); ap.add_argument("keys", type=Path)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    keys = sorted(set(a.keys.read_text().split()))
    missing, already, todo = [], [], []
    for k in keys:
        shard = shard_for(a.cache_root, k, "lineage")
        if not shard.is_dir():
            missing.append(k)
        elif (shard / TOMBSTONE_NAME).exists():
            already.append(k)
        else:
            todo.append(k)
    print(f"{len(keys)} keys: {len(todo)} to tombstone, {len(already)} already, {len(missing)} missing")
    for k in missing:
        print(f"missing {k}")
    if missing:
        return 1
    if a.apply:
        for k in todo:
            tombstone_shard(a.cache_root, k)
        left = [k for k in todo if not (shard_for(a.cache_root, k, "lineage") / TOMBSTONE_NAME).exists()]
        print(f"applied: {len(todo) - len(left)} tombstoned, {len(left)} failed")
        return 1 if left else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
