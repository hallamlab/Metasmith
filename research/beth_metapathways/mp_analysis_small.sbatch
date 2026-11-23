#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=beth_mp4s
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=96G
#SBATCH --time=8:00:00
#SBATCH --output=/scratch/phyberos/beth_mp/logs/mp_small_%A_%a.log
# Usage: sbatch --array=0-13%4 mp_analysis_small.sbatch SAMPLE_LIST
#   SAMPLE_LIST holds one sample id per line; array index N runs line N+1.
# One output root per sample: analysis_wf holds an exclusive lock on its -o directory.
set -euo pipefail
B=/scratch/phyberos/beth_mp
SAMPLE=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$1")
test -n "$SAMPLE"
exec bash $B/jobs/mp_analysis_one.sh "$SAMPLE" 8
