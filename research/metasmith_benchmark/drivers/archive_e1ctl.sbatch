#!/bin/bash
# Tar one E1ctl arm's products (out/, reports/, .nextflow.log) for chinook, and check the tar holds every entry.
#   sbatch archive_e1ctl.sbatch <short|long>
#SBATCH --account=rpp-shallam
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=3:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/archive_e1ctl.%j.out
set -euo pipefail
ARM=$1
S=/scratch/phyberos/bench/e1ctl/$ARM
ARCHIVE=/scratch/phyberos/bench/archive/e1ctl_$ARM.tar

cd "$S"
tar -cf "$ARCHIVE" out reports .nextflow.log
tar -tf "$ARCHIVE" > "${ARCHIVE%.tar}.manifest.txt"
members=$(wc -l < "${ARCHIVE%.tar}.manifest.txt")
entries=$(( $(find out reports | wc -l) + 1 ))
echo "TAR_MEMBER_COUNT=$members SOURCE_ENTRY_COUNT=$entries ARCHIVE_BYTES=$(stat -c %s "$ARCHIVE")"
[ "$members" -eq "$entries" ] || { echo "MISMATCH - ABORT, DO NOT DELETE"; exit 1; }
echo "VERIFIED OK"
