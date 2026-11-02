#!/bin/bash
# One row per (arm, max_iter, max_refine): wall seconds, peak RSS, exit, plan size.
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
E=$ROOT/src/metasmith/engine/msm_solver.x86_64-linux
P=${MSM_PROF_DIR:-${CLAUDE_JOB_DIR:-/tmp}/prof}
mkdir -p "$P"
arm=$1; iters=$2; refine=$3; budget=${4:-300}
in=$P/$arm.iter$iters.ref$refine.json
python3 -c "
import json,sys
d=json.load(open('$P/$arm.json')); d['max_iter']=$iters; d['max_refine']=$refine
json.dump(d,open('$in','w'))
"
out=$P/$arm.iter$iters.ref$refine.reply.json
err=$P/$arm.iter$iters.ref$refine.err
/usr/bin/time -v -o "$P/$arm.iter$iters.ref$refine.time" \
  timeout $budget systemd-run --user --scope -q -p MemoryMax=20G \
  "$E" solve < "$in" > "$out" 2> "$err"
rc=$?
rss=$(awk '/Maximum resident/{print $6}' "$P/$arm.iter$iters.ref$refine.time")
wall=$(awk '/Elapsed .wall/{print $8}' "$P/$arm.iter$iters.ref$refine.time")
steps=$(python3 -c "
import json
try:
    d=json.load(open('$out')); print(f\"{len(d['steps'])} complete={d['complete']} its={d.get('iterations')}\")
except Exception as e: print('-')
")
printf '%-10s iter=%-4s ref=%-4s rc=%-3s wall=%-9s peakRSS=%.2fGB  %s\n' \
   "$arm" "$iters" "$refine" "$rc" "$wall" "$(echo "$rss/1048576"|bc -l)" "$steps"
