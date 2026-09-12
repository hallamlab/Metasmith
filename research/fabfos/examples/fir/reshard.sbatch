#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=cv_reshard
#SBATCH --array=0-15
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=3:00:00
#SBATCH --output=/project/rpp-shallam/phyberos/cyanoverse/gpr/audit/logs/reshard_%A_%a.out
set -euo pipefail
module load apptainer

ROOT=/project/rpp-shallam/phyberos/cyanoverse
GPR=$ROOT/gpr
SIF=/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_python_for_data_science..1.2.5.sif

# 994 shards over 16 array tasks, by contiguous range. Sequential fill means a
# shard range maps to a contiguous SAMPLE range, so the tasks barely overlap in
# what they read -- a strided split would have every task read the whole corpus.
N_SHARDS=994
N_TASKS=16
LO=$(( SLURM_ARRAY_TASK_ID * N_SHARDS / N_TASKS ))
HI=$(( (SLURM_ARRAY_TASK_ID + 1) * N_SHARDS / N_TASKS ))

apptainer exec -B /project -B /scratch "$SIF" \
    python3 "$GPR/scripts/reshard.py" \
        --orfs-dir "$ROOT/data/orfs_and_metabuli/sequences-open_reading_frames" \
        --output "$GPR" --only "${LO}:${HI}"
