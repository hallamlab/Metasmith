#!/bin/bash
#SBATCH -A rrg-shallam-ab
#SBATCH -c 8
#SBATCH --mem=16G
#SBATCH --time=3:00:00
#SBATCH --array=0-48
#SBATCH -o /scratch/phyberos/bench/e4_mm/ryan_pgdb_tables/logs/%A_%a.log
set -euo pipefail
W=/scratch/phyberos/bench/e4_mm
s=$(ls $W/ryan_pgdb | sed -n "$((SLURM_ARRAY_TASK_ID + 1))p")
python3 $W/pgdb_extract.py $W/ryan_pgdb/$s $W/ryan_pgdb_tables --procs 8
