#!/bin/bash
# Delete listed nextflow task work directories of one run, to reclaim bytes that task_cache also holds.
#
#   sbatch <checkout>/research/metasmith_benchmark/drivers/prune_work.sbatch <run dir> <list file>
#
# The list holds one work directory per line. Take it from the FILES manifests of the step's
# consumers, and submit only after every consumer of those directories has finished.
# CAUTION a path outside <run dir>/nxf_work/ aborts the job before anything is deleted.
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=3:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/prune.%j.out
set -euo pipefail

RUN="$(realpath "$1")"
LIST="$2"
WORK="$RUN/nxf_work/"
quota() { lfs quota -p 83115734 /scratch | tail -1 | awk '{printf "%.3f TiB, %s inodes\n", $2/2^30, $6}'; }

mapfile -t dirs < <(grep -v '^\s*$' "$LIST")
for d in "${dirs[@]}"; do
    r="$(realpath -m "$d")"
    [[ "$r" == "$WORK"??/* ]] || { echo "ABORT: $d is not a task dir under $WORK"; exit 1; }
done
echo "run $RUN: ${#dirs[@]} dirs, $(du -scb "${dirs[@]}" 2>/dev/null | tail -1 | cut -f1) B"
echo "quota before: $(quota)"
for d in "${dirs[@]}"; do rm -rf -- "$d"; done
echo "left: $(for d in "${dirs[@]}"; do [ -e "$d" ] && echo "$d"; done | wc -l)"
sleep 300
echo "quota 5 min after: $(quota)"
