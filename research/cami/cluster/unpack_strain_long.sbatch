#!/bin/bash
#SBATCH --job-name=cami_unpack_strain_long
#SBATCH --account=rrg-shallam-ab
#SBATCH --array=0-99
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=1:00:00
#SBATCH --output=/scratch/phyberos/cami/_logs/unpack_strain_long.%A_%a.out
set -euo pipefail
SRC=/scratch/phyberos/cami/cami2_challenge/strain/long_read
DST=/scratch/phyberos/cami/work/strain_long_read
i=$SLURM_ARRAY_TASK_ID
mkdir -p "$DST"
for kind in reads contigs; do
  tar -xzf "$SRC/strmgCAMI2_sample_${i}_${kind}.tar.gz" -C "$DST"
done
echo "sample_${i} unpacked"
find "$DST" -path "*sample_${i}/*" -type f | sort
