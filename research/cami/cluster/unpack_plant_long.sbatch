#!/bin/bash
#SBATCH --job-name=cami_unpack_plant_long
#SBATCH --account=rrg-shallam-ab
#SBATCH --array=0-20
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=1:00:00
#SBATCH --output=/scratch/phyberos/cami/_logs/unpack_plant_long.%A_%a.out
set -euo pipefail
# PLATFORM selects which of the two long-read platforms to unpack -- plant_associated is
# the one CAMI II dataset with two long-read technologies (nano, pacbio), not one.
# sbatch --export=PLATFORM=nano   unpack_plant_long.sbatch
# sbatch --export=PLATFORM=pacbio unpack_plant_long.sbatch
PLATFORM=${PLATFORM:?set PLATFORM=nano or PLATFORM=pacbio}
SRC=/scratch/phyberos/cami/cami2_challenge/plant_associated/long_read_${PLATFORM}
DST=/scratch/phyberos/cami/work/plant_long_read_${PLATFORM}
i=$SLURM_ARRAY_TASK_ID
mkdir -p "$DST"
for kind in reads contigs; do
  tar -xzf "$SRC/rhimgCAMI2_sample_${i}_${kind}.tar.gz" -C "$DST"
done
echo "sample_${i} ($PLATFORM) unpacked"
find "$DST" -path "*sample_${i}/*" -type f | sort
