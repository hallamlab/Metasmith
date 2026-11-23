#!/bin/bash
# Pack each listed scratch directory into one tar under bench/_tars, check the tar lists every entry, then delete the directory.
#
#   sbatch <checkout>/research/metasmith_benchmark/drivers/tar_and_drop.sbatch <dir>...
#
# Returns a directory's inodes while keeping its bytes on fir.
# CAUTION a directory aborts the job before anything is deleted when it is outside /scratch/phyberos/bench,
# when its tar already exists, or when the tar lists fewer entries than the directory holds.
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=6:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/tar_and_drop.%j.out
set -euo pipefail
OUT=/scratch/phyberos/bench/_tars
quota() { lfs quota -p 83115734 /scratch | tail -1 | awk '{printf "%.3f TiB, %s inodes\n", $2/2^30, $6}'; }
mkdir -p "$OUT"

for a in "$@"; do
    d="$(realpath "$a")"
    [[ "$d" =~ ^/scratch/phyberos/bench/[A-Za-z0-9_.-]+$ ]] || { echo "ABORT: $a is not a top-level dir under bench"; exit 1; }
    [ -e "$OUT/$(basename "$d").tar" ] && { echo "ABORT: $OUT/$(basename "$d").tar exists"; exit 1; }
done
echo "quota before: $(quota)"
for a in "$@"; do
    d="$(realpath "$a")"; n="$(basename "$d")"; t="$OUT/$n.tar"
    have=$(find "$d" | wc -l)
    tar -cf "$t" -C "$(dirname "$d")" "$n"
    listed=$(tar -tf "$t" | wc -l)
    echo "$n: $have entries, tar lists $listed, $(stat -c %s "$t") B"
    [ "$listed" -ge "$have" ] || { echo "ABORT: tar of $n lists fewer entries than the dir"; exit 1; }
    find "$d" -mindepth 1 -delete
    rmdir "$d"
done
sleep 300
echo "quota 5 min after: $(quota)"
