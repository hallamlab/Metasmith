#!/bin/bash
#SBATCH --job-name=cami_unpack_cami3_humangut
#SBATCH --account=rrg-shallam-ab
#SBATCH --array=0-19
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=1:00:00
#SBATCH --output=/scratch/phyberos/cami/_logs/unpack_cami3_humangut.%A_%a.out
set -euo pipefail
# PLATFORM selects CAMI III's short or long read set.
#   sbatch --export=PLATFORM=short unpack_cami3_toyhumangut.sbatch
#   sbatch --export=PLATFORM=long  unpack_cami3_toyhumangut.sbatch
# Unlike CAMI II, CAMI III ships the gold-standard assembly as its own `_gsa` tarball,
# separate from `_contigs` (a per-sample assembly, not the truth). `_bam` is skipped for
# the same reason it is skipped everywhere else in this corpus: nothing downstream reads
# the alignment. Unverified against CAMI III's own documentation -- confirm the first
# unpacked sample's contents name what you expect before trusting this at scale.
PLATFORM=${PLATFORM:?set PLATFORM=short or PLATFORM=long}
SRC=/scratch/phyberos/cami/cami3_toy_humangut/$PLATFORM
DST=/scratch/phyberos/cami/work/toy_humangut_${PLATFORM}_read
i=$SLURM_ARRAY_TASK_ID
mkdir -p "$DST"
for kind in reads gsa; do
  tar -xzf "$SRC/sample_${i}_${kind}.tar.gz" -C "$DST"
done
echo "sample_${i} ($PLATFORM) unpacked"
find "$DST" -path "*sample_${i}*" -type f | sort
