#!/bin/bash
#SBATCH --job-name=cami_unpack_marine_long
#SBATCH --account=rrg-shallam-ab
#SBATCH --array=0-9
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=1:00:00
#SBATCH --output=/scratch/phyberos/cami/_logs/unpack_marine_long.%A_%a.out
set -euo pipefail
SRC=/scratch/phyberos/cami/cami2_challenge/marine/long_read
DST=/scratch/phyberos/cami/work/marine_long_read
i=$SLURM_ARRAY_TASK_ID
mkdir -p "$DST"
# bam.tar.gz is not unpacked, matching unpack_marine.sbatch's short-read sibling: nothing
# downstream of this corpus reads the alignment, only reads + the gold-standard contigs.
for kind in reads contigs; do
  tar -xzf "$SRC/marmgCAMI2_sample_${i}_${kind}.tar.gz" -C "$DST"
done
echo "sample_${i} unpacked"
find "$DST" -path "*sample_${i}*" -type f | head -10
