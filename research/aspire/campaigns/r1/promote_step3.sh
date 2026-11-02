#!/bin/bash
# Bank the centrifuger step (step_3) into the metasmith cache, by hand.
#
# WHY BY HAND. metasmith stages each task's outputs into `<cache_root>/<key>.tmp/`
# as they finish, but the manifest, the shard rename and the CacheStore row all
# happen in promote_run(), which fires ONCE after the Nextflow process returns.
# A head that dies before then banks nothing. CDUMaintenance2 takes all 1032
# nodes at 15:00Z, so the head is being retired deliberately -- which means
# promotion has to happen first, or 34 centrifuger runs are recomputed.
#
# WHY THE HELD DIRECTORY. When S25/S27/S9 blew their wall the step stood at
# 31/34, and promoting a partial step would have banked a permanently incomplete
# result under a key that then reads as a HIT forever (the F2 trap). So the 93
# finished files were moved OUT of the cache root and parked, because
# recover_orphan_tmp_dirs is cache-root-WIDE: it rmtree's every `*.tmp` lacking a
# manifest.cbor regardless of which run staged it. That was proven with a decoy,
# not read off the source. Nextflow then republishes the 3 retried samples into a
# fresh `<key>.tmp`, and this script rejoins 93 + 9 = 102 before promoting.
#
# Usage:
#   promote_step3.sh --check    read-only; verifies every precondition
#   promote_step3.sh --go       does it
set -uo pipefail

MODE=${1:---check}
KEY=1e207cb95d61ad9366cfc59bbe3c3288d03e1a4db7dc3836bcad78ad510882b3fc0b
BASE=/scratch/phyberos/gmcf3495
CACHE=$BASE/metasmith/task_cache
HELD=$BASE/held_centrifuger_$KEY.tmp
FRESH=$CACHE/$KEY.tmp
PW=$BASE/pw
JOBS=51345043,51345044,51345045
EXPECT_HELD=93          # 31 samples x 3 products
EXPECT_TOTAL=102        # 34 samples x 3 products
IMG=/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_metasmith..0.19.1.sif

fail () { echo "REFUSE: $*" >&2; exit 1; }

echo "== 1. centrifuger retries =="
st=$(sacct -j "$JOBS" -X --noheader --format=JobID,State 2>/dev/null)
echo "$st"
[ -n "$st" ] || fail "sacct returned nothing for $JOBS"
echo "$st" | grep -qE "RUNNING|PENDING" && fail "still running -- nothing to promote yet"
bad=$(echo "$st" | grep -Evc "COMPLETED")
[ "$bad" -eq 0 ] || fail "$bad job(s) did not COMPLETE. Leave the held dir alone;
  step_3 will miss and centrifuger reruns all 34 under the raised 6h wall.
  NEVER promote at 31/34."

echo
echo "== 2. held directory =="
[ -d "$HELD" ] || fail "no held dir at $HELD"
nh=$(ls "$HELD" | wc -l)
echo "  $HELD  files=$nh"
[ "$nh" -eq "$EXPECT_HELD" ] || fail "held dir has $nh files, expected $EXPECT_HELD"

echo
echo "== 3. fresh .tmp published by nextflow =="
[ -d "$FRESH" ] || fail "no $FRESH -- nextflow has not published the retried samples.
  The head must still be alive for this; do not kill it before promoting."
nf=$(ls "$FRESH" | wc -l)
echo "  $FRESH  files=$nf"
[ "$nf" -eq $((EXPECT_TOTAL - EXPECT_HELD)) ] || \
    fail "fresh .tmp has $nf files, expected $((EXPECT_TOTAL - EXPECT_HELD))"

echo
echo "== 4. other .tmp dirs in the cache root =="
# promote_run's orphan sweep deletes every .tmp without a manifest.cbor, ours
# included if it is not the one being promoted. Anything listed here that is
# worth keeping must be moved OUT before --go, exactly as the held dir was.
others=$(ls -d "$CACHE"/*.tmp 2>/dev/null | grep -v "^$FRESH$")
if [ -n "$others" ]; then
    echo "$others" | sed 's/^/  WILL BE DELETED BY THE ORPHAN SWEEP: /'
    [ "$MODE" = "--go" ] && fail "move these out first, or accept losing them"
else
    echo "  none -- the sweep has nothing else to take"
fi

echo
echo "== 5. promote workspace =="
[ -f "$PW/promote_ws3/workflow.step_3.meta" ] || fail "no promote_ws3/workflow.step_3.meta"
[ -f "$PW/promote_ws3/_metasmith/trace.jsonl" ] || fail "no promote_ws3/_metasmith/trace.jsonl"
echo "  ok (key already verified to match the held dir name)"

if [ "$MODE" != "--go" ]; then
    echo
    echo "CHECK PASSED -- rerun with --go to merge $EXPECT_HELD + $nf and promote."
    exit 0
fi

echo
echo "== 6. merge held -> fresh =="
# -n, not a bare mv: the two sets should be disjoint (31 samples vs 3), but if a
# name ever collided a bare mv would silently overwrite a finished product with
# another. -n leaves the collider behind instead, so the count assertion below
# turns a silent corruption into a loud stop.
mv -n "$HELD"/* "$FRESH"/ || fail "merge failed"
rmdir "$HELD" || echo "  (held dir not empty after move -- inspect $HELD)"
nt=$(ls "$FRESH" | wc -l)
echo "  $FRESH now has $nt files"
[ "$nt" -eq "$EXPECT_TOTAL" ] || fail "expected $EXPECT_TOTAL after merge, got $nt"

echo
echo "== 7. promote =="
cat > "$PW/go3.py" <<'PY'
from pathlib import Path
from metasmith.caching.promote import promote_run
r = promote_run(workspace=Path("/scratch/phyberos/gmcf3495/pw/promote_ws3"),
                cache_root=Path("/scratch/phyberos/gmcf3495/metasmith/task_cache"))
print("promoted       :", r["promoted"])
print("skipped        :", r["skipped"])
print("orphan_recovery:", r["orphan_recovery"])
PY
apptainer exec --no-home --cleanenv --env TMPDIR=/tmp --bind /scratch/phyberos \
  --bind /scratch/phyberos/gmcf3495/metasmith/dev/metasmith:/opt/conda/envs/metasmith_env/lib/python3.12/site-packages/metasmith \
  "$IMG" /opt/conda/envs/metasmith_env/bin/python3.12 "$PW/go3.py" || fail "promote_run failed"

echo
echo "== 8. verify the shard =="
SHARD=$CACHE/${KEY:0:2}/${KEY:2}
[ -f "$SHARD/manifest.cbor" ] || fail "no manifest.cbor at $SHARD"
no=$(ls "$SHARD/out" 2>/dev/null | wc -l)
echo "  $SHARD  manifest=yes out=$no"
[ "$no" -eq "$EXPECT_TOTAL" ] || fail "shard has $no outputs, expected $EXPECT_TOTAL"
echo
echo "step_3 BANKED. Now: verify_cache_hits.sh must show 4 HIT, then retire the run."
