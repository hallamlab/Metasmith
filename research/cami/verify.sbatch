#!/bin/bash
#SBATCH --job-name=cami_verify
#SBATCH --account=rrg-shallam-ab
#SBATCH --array=0-31
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=8:00:00
#SBATCH --output=%x.%A_%a.out
# Hashing terabytes is IO-bound, so many small tasks beat a few fat ones.
set -euo pipefail
ROOT=${CAMI_ROOT:-/scratch/phyberos/cami}
# Not $(dirname "$0"): under Slurm, $0 is the spool copy of this script, and the
# sibling verify.py is not beside it. That resolves to a path that does not exist,
# every array task dies, and the run leaves zero reports -- which reads exactly
# like "no problems found" if you only check for report content.
python3 "${VERIFY_PY:-$ROOT/verify.py}" \
    --manifest "$ROOT/manifest.tsv" \
    --root "$ROOT" \
    --out "$ROOT/_verify/report.${SLURM_ARRAY_TASK_ID}.tsv" \
    --shard "$SLURM_ARRAY_TASK_ID" --shards "$SLURM_ARRAY_TASK_COUNT"
