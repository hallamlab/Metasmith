#!/bin/bash
#SBATCH --job-name=metagem_verify
#SBATCH --account=rrg-shallam-ab
#SBATCH --array=0-7
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=8:00:00
#SBATCH --output=/scratch/phyberos/metagem/_logs/%x.%A_%a.out
# Shards are sized by bytes rather than by count: 934 objects but up to 5.21 TB of hashing.
# Set DATASETS to check one study while another is still transferring.
set -euo pipefail
ROOT=${METAGEM_ROOT:-/scratch/phyberos/metagem}
MANIFEST=${MANIFEST:-$ROOT/manifest.tsv}
TAG=${TAG:-all}
# Not $(dirname "$0"): sbatch runs a spool copy, so $0 is under /localscratch/spool and the
# script beside it does not exist. Reach for the staged copy.
python3 "${VERIFY_PY:-$ROOT/verify.py}" \
    --manifest "$MANIFEST" \
    --root "$ROOT" \
    ${DATASETS:+--dataset $DATASETS} \
    --out "$ROOT/_verify/${TAG}.${SLURM_ARRAY_TASK_ID}.tsv" \
    --shard "$SLURM_ARRAY_TASK_ID" --shards "$SLURM_ARRAY_TASK_COUNT"
