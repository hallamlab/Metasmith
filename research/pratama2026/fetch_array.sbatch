#!/bin/bash
#SBATCH --job-name=pratama_fetch_array
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=12:00:00
#SBATCH --output=/scratch/phyberos/pratama2026/_logs/%x.%A_%a.out
# One array task per object, because on this cluster concurrency has to be spread across
# NODES to buy anything. fetch.sbatch runs twelve workers inside one job and the whole job
# aggregated to 227 KB/s; the login node pulls 25 MB/s on a single stream. The per-node cap
# is the ceiling, so twelve workers on one node and one worker on twelve nodes are not the
# same transfer.
#
# Submit it WIDE -- `--array=0-N%18` -- because ENA throttles per stream rather than in
# aggregate. A single stream, measured from a login node with nothing else running, gave
# 215 KB/s; the earlier twenty-four-worker run aggregated 57 MiB/s. So concurrency is the
# only lever, and the ceiling is ENA's tolerance, not bandwidth: forty-eight streams got
# every connection refused within 150 ms, twenty-four held for the 2019 project and started
# closing transfers mid-stream on this one.
#
# This is the opposite of the NERSC fetch in research/viromics/cluster/, which is capped by
# compute-node egress and wants FEWER streams. Do not copy a concurrency number between them.
#
# The worklist is built at submission time by build_worklist.py and is whatever was still
# incomplete then. Each task is idempotent: it verifies the published MD5 and promotes out
# of staging only on a match, so re-running the array costs one hash per finished object.
set -euo pipefail

ROOT=${PRATAMA_ROOT:-/scratch/phyberos/pratama2026}
WORKLIST=${WORKLIST:-$ROOT/_worklist.tsv}

LINE=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$WORKLIST")
test -n "$LINE"
IFS=$'\t' read -r DATASET RELPATH URL BYTES MD5 <<<"$LINE"

DEST="$ROOT/$DATASET/$RELPATH"
STAGE="$ROOT/.staging/${DATASET}__${RELPATH//\//__}"
mkdir -p "$(dirname "$DEST")" "$ROOT/.staging"

if [ -s "$DEST" ] && [ "$(stat -c%s "$DEST")" = "$BYTES" ]; then
    echo "have $RELPATH"; exit 0
fi

# ENA does not refuse this transfer, it closes it: attempt after attempt ends in
# "transfer closed with N bytes remaining", each one having moved a few megabytes. So the
# retry loop is the transfer, not a fallback around it. `-C -` makes every attempt pick up
# where the last was cut, and a handful of attempts is nowhere near enough -- a five-attempt
# loop moved 38 MB of an 11 GB object before giving up. Run until the object is whole or
# the wall is nearly spent, and let the resubmission take it from there.
DEADLINE=$(( $(date +%s) + 11 * 3600 ))
attempt=0
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    attempt=$((attempt + 1))
    before=$(stat -c%s "$STAGE" 2>/dev/null || echo 0)
    curl -sS -C - --retry 3 --retry-delay 30 -o "$STAGE" "$URL" || true
    got=$(stat -c%s "$STAGE" 2>/dev/null || echo 0)
    if [ "$got" != "$BYTES" ]; then
        echo "attempt $attempt: $before -> $got of $BYTES"
        # A stalled resume means the partial is unusable rather than slow; start over
        # rather than sleeping against a server that will not extend it.
        if [ "$got" = "$before" ] && [ "$attempt" -gt 3 ]; then
            echo "no progress, restarting $RELPATH"
            rm -f "$STAGE"
        fi
        sleep 60
        continue
    fi
    # Size alone is not integrity: a resumed transfer can land the exact published byte
    # count with a corrupt middle, which is why the published MD5 is the gate.
    if [ -n "$MD5" ] && [ "$(md5sum "$STAGE" | cut -d' ' -f1)" != "$MD5" ]; then
        echo "md5 mismatch after $attempt attempts, restarting"
        rm -f "$STAGE"
        continue
    fi
    mv "$STAGE" "$DEST"
    echo "ok $RELPATH after $attempt attempts"
    exit 0
done
echo "INCOMPLETE $RELPATH at $(stat -c%s "$STAGE" 2>/dev/null || echo 0) of $BYTES"
exit 1
