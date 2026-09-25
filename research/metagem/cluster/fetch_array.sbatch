#!/bin/bash
#SBATCH --job-name=metagem_fetch_array
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=12:00:00
#SBATCH --output=/scratch/phyberos/metagem/_logs/%x.%A_%a.out
# One array task per object. Same shape as research/pratama2026/fetch_array.sbatch and for
# the same reason: on this cluster concurrency has to be spread across NODES, because the
# per-node egress cap is the ceiling and twelve workers on one node is not twelve nodes.
#
# Submit it NARROW -- `--array=0-N%4`. ENA answers eighteen concurrent streams with
# `curl (7) Failed to connect ... after 145 ms`, a refusal that outlives the run which
# caused it. The metaGEM corpus spans FIVE BioProjects rather than one, and ENA's tolerance
# measured per-project on the Pratama legs (57 MiB/s on one, 220 KB/s on another), so do
# not read one study's throughput as the next study's.
#
# The Pratama 2022 leg draws on the same front end. Count its running tasks before widening
# this array: the two jobs share whatever budget ENA is granting this account.
#
# Each task is idempotent: it promotes out of staging only on the published MD5, so
# re-running the array costs one hash per finished object.
set -euo pipefail

ROOT=${METAGEM_ROOT:-/scratch/phyberos/metagem}
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

# THIS SCRIPT IS THE FALLBACK, NOT THE ARMED PATH, and only because of where it runs. On
# 2026-09-12 every fir compute node measured -- sixteen, spread over three racks -- moved
# 0.02-0.25 MB/s to ENA, to AWS S3 and to cdn.kernel.org alike, while a login node moved
# 1.9 MB/s per stream to ENA and 32-36 MB/s per stream from S3. Concurrency does not recover
# it: ENA HTTPS from a compute node aggregated 0.04 MB/s at one stream and at sixteen. So an
# array of any width finishes 920 GB in months, whatever transport it picks, and the corpus
# goes through fetch_sra.sh plus convert_array.sbatch instead. Keep this path for the case
# where ENA's own byte-exact fastq is required, and run it from a login node if so.
#
# Its two FTP claims are also now contradicted and left here only as the record: port 21 WAS
# reachable from the compute nodes tested, and FTP was not faster than HTTPS -- 0.02 MB/s
# against 0.01 on a compute node, 2.15 against 2.30 on a login node.
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
        # normal shape of this transfer when ENA is busy; only the MD5 below justifies a
        # delete, because it is the one place corruption is detectable.
        sleep 60
        continue
    fi
    # Size alone is not integrity: a resumed transfer can land the exact published byte
    # count with a corrupt middle.
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
