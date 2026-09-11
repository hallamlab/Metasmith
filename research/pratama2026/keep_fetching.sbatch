#!/bin/bash
#SBATCH --job-name=pratama_keeper
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=0:20:00
#SBATCH --output=/scratch/phyberos/pratama2026/_logs/%x.%j.out
# Re-arms the fetch until the manifest is satisfied, because one pass will not do it.
#
# ENA's throughput to this account is the constraint and it is not a per-stream one: a
# single stream from a login node measured 215 KB/s and eighteen concurrent tasks
# aggregated to the same 221 KB/s, while an earlier run on the 2019 project reached
# 57 MiB/s. It comes and goes, so the strategy is to keep a wide fetch armed and let it
# take whatever the server is willing to give that hour. Everything resumes: the fetch
# promotes only on the published MD5.
#
# Runs after the array, rebuilds the worklist from what is actually on disk, and either
# submits a fresh array plus another copy of itself, or stops and lets the verifier run.
set -euo pipefail

ROOT=${PRATAMA_ROOT:-/scratch/phyberos/pratama2026}
DATASETS=${DATASETS:-reads_2022}
MAX_ROUNDS=${MAX_ROUNDS:-20}
ROUND=${ROUND:-1}

cd "$ROOT"
python3 build_worklist.py --manifest manifest.tsv --root . --out _worklist.tsv --dataset $DATASETS
N=$(wc -l < _worklist.tsv)
echo "round $ROUND: $N outstanding"

if [ "$N" -eq 0 ]; then
    echo "ALL FETCHED $(date -Is)"
    TAG=${DATASETS// /_} DATASETS="$DATASETS" sbatch --array=0-7 verify.sbatch
    exit 0
fi
if [ "$ROUND" -ge "$MAX_ROUNDS" ]; then
    echo "STOPPING after $ROUND rounds with $N outstanding"
    exit 1
fi

A=$(sbatch --parsable --array=0-$((N - 1))%18 fetch_array.sbatch)
echo "round $ROUND submitted array $A"
ROUND=$((ROUND + 1)) DATASETS="$DATASETS" MAX_ROUNDS="$MAX_ROUNDS" \
    sbatch --dependency=afterany:$A keep_fetching.sbatch
