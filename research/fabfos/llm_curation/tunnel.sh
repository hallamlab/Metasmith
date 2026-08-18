#!/usr/bin/env bash
# Point localhost:8080 at whichever fir job is currently serving.
#
# The MIG jobs are chained on `afterany`, so the serving node changes every three hours
# and the tunnel has to follow it. Run this after a hand-off; it is idempotent.
set -uo pipefail
ssh -O check fir >/dev/null 2>&1 || { echo "no ControlMaster to fir; reconnect first"; exit 3; }

JOB_NODE=$(timeout 60 ssh -o BatchMode=yes fir 'squeue -u phyberos -h -t R -o "%i %N"' | head -1)
[ -n "$JOB_NODE" ] || { echo "no running job"; exit 1; }
JOB=${JOB_NODE%% *}; NODE=${JOB_NODE##* }
echo "serving job $JOB on $NODE"

pkill -f "ssh -N -L 8080:" 2>/dev/null
ssh -o BatchMode=yes -f -N -L 8080:"$NODE":8080 fir
for i in $(seq 1 60); do
    curl -sf --max-time 5 http://127.0.0.1:8080/v1/models && { echo " <- up"; exit 0; }
    sleep 10
done
echo "endpoint never answered"; exit 2
