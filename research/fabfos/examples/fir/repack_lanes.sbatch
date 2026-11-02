#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=cv_repack
#SBATCH --array=0-15
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=3:00:00
#SBATCH --output=/project/rpp-shallam/phyberos/cyanoverse/gpr/audit/logs/repack_%A_%a.out
set -euo pipefail
module load apptainer

ROOT=/project/rpp-shallam/phyberos/cyanoverse
GPR=$ROOT/gpr
SIF=/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_python_for_data_science..1.2.5.sif

export SHARD_N=16
apptainer exec -B /project -B /scratch "$SIF" \
    python3 "$GPR/scripts/repack_lanes.py" "$GPR" "$ROOT/data/annot1"
