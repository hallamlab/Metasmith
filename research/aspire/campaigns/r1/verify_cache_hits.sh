#!/bin/bash
# Report, for a STAGED run, which steps the cache can already satisfy.
#
#     ssh fir 'bash -s -- <run_dir> [cache_root]' < verify_cache_hits.sh
#
# The point is to make a resubmission's reuse checkable BEFORE it submits. A
# step's cache key is written into <run_dir>/workflow.step_N.meta at staging
# time -- it is not in the plan and not printed by --dry-run -- so `run
# --stage-only` then this script is the only way to see whether the banked work
# will actually be picked up. A key that has drifted costs a full recompute of
# that step, and on this run step 1 is 372 GB of interleaving.
#
# A key is a function of the transform's own identity (its key, its I/O
# topology hash, and the digest of its definition file) and the instance_ids of
# its inputs. Nothing downstream contributes. So a MISS on an upstream step
# means one of those three moved, and the transform file is the first place to
# look.

set -uo pipefail

RUN=${1:?usage: verify_cache_hits.sh <run_dir> [cache_root]}
CACHE=${2:-/scratch/phyberos/gmcf3495/metasmith/task_cache}
IMG=${IMG:-/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_metasmith..0.19.1.sif}
DEV=${DEV:-/scratch/phyberos/gmcf3495/metasmith/dev/metasmith}

[ -d "$RUN" ] || { echo "no such run dir: $RUN (this script runs ON fir)"; exit 2; }
[ -d "$CACHE" ] || { echo "no such cache root: $CACHE"; exit 2; }

# Not /tmp: login1's /tmp is littered with stray modules that shadow the
# stdlib (a /tmp/dis.py once broke `import inspect` inside the container).
W=$(dirname "$RUN")/.cache_hit_check
mkdir -p "$W"

cat > "$W/probe.py" <<'PY'
import sys
from pathlib import Path
from metasmith.caching.promote import _read_step_meta, _shard_dir

run, cache = Path(sys.argv[1]), Path(sys.argv[2])
metas = sorted(run.glob("workflow.step_*.meta"),
               key=lambda p: int(p.name.split("_")[1].split(".")[0]))
if not metas:
    print("no workflow.step_*.meta in the run dir -- was it staged?")
    raise SystemExit(2)

hits = misses = 0
for m in metas:
    spec = _read_step_meta(m)
    order = m.name.split("_")[1].split(".")[0]
    if spec is None:
        print(f"  {order:>3}  (unreadable meta)"); continue
    if not spec.cacheable:
        print(f"  {order:>3}  not cacheable"); continue
    key = spec.cache_key.hex()
    shard = _shard_dir(cache, key)
    if shard.exists() and (shard / "manifest.cbor").is_file():
        n = len(list((shard / "out").iterdir())) if (shard / "out").is_dir() else 0
        print(f"  {order:>3}  HIT   {key[:16]}…  out={n}")
        hits += 1
    else:
        print(f"  {order:>3}  miss  {key[:16]}…")
        misses += 1
print(f"\n{hits} hit / {misses} miss over {hits + misses} cacheable steps")

# THE REUSE THAT MATTERS IS INVISIBLE ABOVE. A step whose outputs are already
# in the cache is ELIDED from the plan -- no workflow.step_N.meta is written for
# it at all -- so the loop above cannot see it, and a plan that reuses
# everything reports "0 hit". That reads exactly like a plan that reuses
# nothing, which is the opposite conclusion and the expensive one to act on.
#
# So account for the cache from the other side: every promoted shard, and
# whether any planned step still claims it. A shard nothing references is
# banked work this plan is already skipping.
planned = set()
for m in metas:
    s = _read_step_meta(m)
    if s is not None and s.cacheable:
        planned.add(s.cache_key.hex())

shards = []
for two in sorted(cache.glob("??")):
    if not two.is_dir():
        continue
    for rest in sorted(two.iterdir()):
        if (rest / "manifest.cbor").is_file():
            shards.append(two.name + rest.name)

if shards:
    elided = [k for k in shards if k not in planned]
    print(f"\ncache holds {len(shards)} promoted shard(s); "
          f"{len(elided)} not referenced by any planned step:")
    for k in elided:
        out = _shard_dir(cache, k) / "out"
        n = len(list(out.iterdir())) if out.is_dir() else 0
        print(f"  ELIDED (reused)  {k[:16]}…  out={n}")
    if not elided:
        print("  (none -- every shard is still claimed by a planned step)")
else:
    print("\ncache holds no promoted shards yet")
PY

apptainer exec --no-home --cleanenv --env TMPDIR=/tmp \
    --bind /scratch/phyberos \
    --bind "$DEV:/opt/conda/envs/metasmith_env/lib/python3.12/site-packages/metasmith" \
    "$IMG" /opt/conda/envs/metasmith_env/bin/python3.12 "$W/probe.py" "$RUN" "$CACHE"
