#!/bin/bash
#SBATCH --job-name=metagem_verify
#SBATCH --account=rrg-shallam-ab
#SBATCH --array=0-7
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=8:00:00
#SBATCH --output=/scratch/phyberos/metagem/_logs/%x.%A_%a.out
# Eight shards because the count decompresses every product: ~970 GB for the four in-scope
# studies. --size-only skips that and answers in seconds when presence is the question.
set -euo pipefail
ROOT=${METAGEM_ROOT:-/scratch/phyberos/metagem}
DS=${DS:?set DS}
python3 "$ROOT/verify_sra.py" \
    --table "$ROOT/runs.$DS.tsv" --root "$ROOT" --dataset "$DS" \
    --out "$ROOT/_verify/$DS.${SLURM_ARRAY_TASK_ID}.tsv" \
    --shard "$SLURM_ARRAY_TASK_ID" --shards "$SLURM_ARRAY_TASK_COUNT" \
    ${SIZE_ONLY:+--size-only}
