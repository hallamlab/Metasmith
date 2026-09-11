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
# Submit it NARROW -- `--array=0-N%4`. The temptation is the opposite, because ENA throttles
# per stream and the earlier twenty-four-worker run on the 2019 project aggregated 57 MiB/s.
# On this project it does not hold. At eighteen concurrent tasks ENA answered with
# `curl (7) Failed to connect ... port 443 after 145 ms` on seventeen of them -- a refusal,
# not a timeout, and the same 150 ms signature that forty-eight streams produced before.
# The refusal outlives the run that caused it, so each impatient resubmission makes the next
# one worse.
#
# Read a wall of refusals as "back off and wait". Four streams and a keeper that re-arms
# patiently finishes sooner than eighteen that get turned away.
#
# This is also the opposite of the NERSC fetch in research/viromics/cluster/, which is
# capped by compute-node egress rather than by the server's tolerance. The two numbers are
# unrelated; do not copy one to the other.
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
# FTP is five times faster where it is reachable -- measured against the same object from
# a login node with nothing else running, ftp:// 8.8 MB/s against https:// 1.8 MB/s, so
# ENA's TLS front end is the slow half. But COMPUTE NODES HERE CANNOT REACH PORT 21: the
# connection times out after 135 s. So probe once and commit, rather than alternating and
# spending three quarters of every task on timeouts.
FTP_URL=${URL/https:\/\//ftp:\/\/}
if curl -sS --max-time 25 -r 0-1 -o /dev/null "$FTP_URL" 2>/dev/null; then
    echo "ftp reachable"
else
    echo "ftp unreachable, using https"
    FTP_URL=$URL
fi

DEADLINE=$(( $(date +%s) + 11 * 3600 ))
attempt=0
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    attempt=$((attempt + 1))
    before=$(stat -c%s "$STAGE" 2>/dev/null || echo 0)
    curl -sS -C - --retry 3 --retry-delay 30 -o "$STAGE" "$FTP_URL" || true
    got=$(stat -c%s "$STAGE" 2>/dev/null || echo 0)
    if [ "$got" != "$BYTES" ]; then
        echo "attempt $attempt: $before -> $got of $BYTES"
        # Never discard a partial for being slow. An attempt that moves nothing is the
        # normal shape of this transfer when ENA is busy, and an earlier version that
        # restarted after three such attempts threw away 27 GB of good bytes. The only
        # thing that justifies deleting a partial is the MD5 check below, which is the
        # one place corruption is actually detectable.
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
