"""Tombstone and empty the cache shards a finished run promoted for one transform, found through cache.sqlite.

    evict_index_shards.py <cache root> <run key> <transform key> [--apply]

The index records which run promoted each entry, so this works after the run's nxf_work is gone,
unlike evict_step_shards.py. --apply touches each shard's tombstone, then deletes the files under its
out/. The manifest, logs and tombstone stay, so `probe` misses at the tombstone.
WARNING evict only a finished batch's shards, after its chinook archive is verified.
CAUTION a later run recomputes this step instead of hitting the cache.
"""
import argparse, sqlite3, sys
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("cache_root"); ap.add_argument("run"); ap.add_argument("transform_key")
ap.add_argument("--apply", action="store_true")
a = ap.parse_args()
root = Path(a.cache_root).resolve()

db = sqlite3.connect(f"file:{root / 'cache.sqlite'}?mode=ro", uri=True)
keys = [k.hex() for (k,) in db.execute(
    "select key from entries where run = ? and transform_key = ? and tombstoned_at is null", (a.run, a.transform_key))]

ok, skipped, freed = [], {}, 0
for k in keys:
    shard = root / k[:2] / k[2:]
    why = None
    if not shard.is_dir():
        why = "shard dir missing"
    elif (shard / "tombstone").exists():
        why = "already tombstoned"
    if why:
        skipped[why] = skipped.get(why, 0) + 1
        continue
    size = sum(f.stat().st_size for f in (shard / "out").rglob("*") if f.is_file())
    ok.append(shard); freed += size

print(f"run {a.run} transform {a.transform_key}: {len(keys)} entries, {len(ok)} shards qualify, {freed} B; skipped {skipped}")
for shard in ok[:3]:
    print(f"  e.g. {shard}")
if not a.apply:
    sys.exit(0)
for shard in ok:
    (shard / "tombstone").touch()
    for f in sorted((shard / "out").rglob("*"), reverse=True):
        f.unlink() if f.is_file() or f.is_symlink() else f.rmdir()
left = sum(1 for shard in ok for f in (shard / "out").rglob("*") if f.is_file())
print(f"applied: {len(ok)} tombstoned, files left under out/: {left}")
