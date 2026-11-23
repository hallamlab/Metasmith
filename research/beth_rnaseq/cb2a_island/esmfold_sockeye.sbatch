#!/bin/bash
#SBATCH --account=st-shallam-1-gpu
#SBATCH --partition=gpu
#SBATCH --nodes=1
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=01:30:00
#SBATCH --job-name=cb2a_fold
#SBATCH --output=/scratch/st-shallam-1/txyliu/cb2a_island/fold_%j.log
set -euo pipefail
R=/scratch/st-shallam-1/txyliu/cb2a_island
cd $R
module load gcc apptainer/1.3.1
export HOME=$R
apptainer exec --nv -B $R ${SIF:-esmfold.sif} python $R/esmfold.py --weights $R/weights --fasta $R/nomodel.faa \
  --out-dir $R/out --device cuda --max-len 2048 --chunk-size 64
ls $R/out | wc -l
