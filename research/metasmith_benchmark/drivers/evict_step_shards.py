"""Tombstone and empty the promoted cache shards of one step of one live run, to free the bytes they duplicate.

    evict_step_shards.py <run dir> <cache root> <step name> [--apply]

A shard qualifies when the task's .command.cache record says `promoted`, its shard sits under the
cache root, and the task dir still holds every produced file at the shard file's size, so the run's
consumers keep their copy. --apply touches the tombstone, then deletes the files under the shard's
out/. The manifest, logs and tombstone stay, so `probe` misses at the tombstone and `record_run`
finds an existing dir.
CAUTION a later run recomputes this step instead of hitting the cache.
"""
import argparse, json, os, sys
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("run"); ap.add_argument("cache_root"); ap.add_argument("step")
ap.add_argument("--apply", action="store_true")
a = ap.parse_args()
run = Path(a.run).resolve(); root = Path(a.cache_root).resolve()
work = run / "nxf_work"

ok, skipped, freed = [], {}, 0
for rec_file in sorted(work.glob("??/*/.command.cache")):
    task = rec_file.parent
    ec = task / ".exitcode"
    if not ec.exists() or ec.read_text().strip() != "0":
        continue
    for line in rec_file.read_text().splitlines():
        r = json.loads(line)
        if r.get("step_name") != a.step:
            continue
        why = None
        shard = Path(r.get("shard", "")).resolve() if r.get("shard") else None
        if r.get("status") != "promoted" or shard is None:
            why = f"status {r.get('status')}"
        elif root not in shard.parents:
            why = "shard outside cache root"
        elif (shard / "tombstone").exists():
            why = "already tombstoned"
        else:
            for p in r.get("produces", []):
                sf = shard / p["relpath"]; tf = task / Path(p["relpath"]).name
                if not sf.is_file():
                    why = "shard file missing"; break
                if not tf.is_file() or tf.stat().st_size != sf.stat().st_size:
                    why = "task copy missing or differs"; break
        if why:
            skipped[why] = skipped.get(why, 0) + 1
            continue
        size = sum(f.stat().st_size for f in (shard / "out").rglob("*") if f.is_file())
        ok.append((shard, size)); freed += size

print(f"run {run.name} step {a.step}: {len(ok)} shards qualify, {freed} B; skipped {skipped}")
if not a.apply:
    sys.exit(0)
for shard, _ in ok:
    (shard / "tombstone").touch()
    for f in sorted((shard / "out").rglob("*"), reverse=True):
        f.unlink() if f.is_file() or f.is_symlink() else f.rmdir()
left = sum(1 for shard, _ in ok for f in (shard / "out").rglob("*") if f.is_file())
print(f"applied: {len(ok)} tombstoned, files left under out/: {left}")
