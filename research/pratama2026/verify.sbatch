#!/bin/bash
#SBATCH --job-name=pratama_verify
#SBATCH --account=rrg-shallam-ab
#SBATCH --array=0-15
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=4:00:00
#SBATCH --output=/scratch/phyberos/pratama2026/_logs/%x.%A_%a.out
# 150 objects but 767 GiB of them, so the shards are sized by bytes rather than by count.
# Set DATASETS to check one leg while another is still transferring; the shard split is over
# whatever the filter leaves, so a leg-limited run still spreads evenly.
set -euo pipefail
ROOT=${PRATAMA_ROOT:-/scratch/phyberos/pratama2026}
MANIFEST=${MANIFEST:-$ROOT/manifest.tsv}
TAG=${TAG:-all}
# Not $(dirname "$0"): sbatch runs a spool copy of this file, so $0 is under
# /localscratch/spool and the script beside it does not exist. Reach for the staged copy.
python3 "${VERIFY_PY:-$ROOT/verify.py}" \
    --manifest "$MANIFEST" \
    --root "$ROOT" \
    ${DATASETS:+--dataset $DATASETS} \
    --out "$ROOT/_verify/${TAG}.${SLURM_ARRAY_TASK_ID}.tsv" \
    --shard "$SLURM_ARRAY_TASK_ID" --shards "$SLURM_ARRAY_TASK_COUNT"
