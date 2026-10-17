#!/bin/bash
# Empty listed nextflow task work directories of one run, to reclaim bytes and inodes that task_cache also holds.
#
#   sbatch <checkout>/research/metasmith_benchmark/drivers/prune_work.sbatch <run dir> <list file> [work subdir]
#
# The work subdir defaults to nxf_work. Pass `work` for an E1 nf-core head.
# WARNING an emptied nf-core task directory cannot serve -resume. A resumed head reruns that task and every task after it.
#
# The list holds one work directory per line. Build it with _step_refs.py, and submit only after
# every consumer of those directories has finished.
# CAUTION each directory keeps its .command.cache. record_run reads that file at run end to index
# the task's promoted shards and write its trace events.
# CAUTION a path outside <run dir>/nxf_work/ aborts the job before anything is deleted.
#SBATCH --account=rpp-shallam
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=3:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/prune.%j.out
set -euo pipefail

RUN="$(realpath "$1")"
LIST="$2"
WORK="$RUN/${3:-nxf_work}/"
quota() { lfs quota -p 83115734 /scratch | tail -1 | awk '{printf "%.3f TiB, %s inodes\n", $2/2^30, $6}'; }

mapfile -t dirs < <(grep -v '^\s*$' "$LIST")
for d in "${dirs[@]}"; do
    r="$(realpath -m "$d")"
    [[ "$r" == "$WORK"??/* ]] || { echo "ABORT: $d is not a task dir under $WORK"; exit 1; }
done
echo "run $RUN: ${#dirs[@]} dirs, $(du -scb "${dirs[@]}" 2>/dev/null | tail -1 | cut -f1) B"
echo "quota before: $(quota)"
for d in "${dirs[@]}"; do find "$d" -mindepth 1 ! -path "$d/.command.cache" -delete; done
echo "left with more than .command.cache: $(for d in "${dirs[@]}"; do [ -n "$(find "$d" -mindepth 1 ! -path "$d/.command.cache" -print -quit)" ] && echo "$d"; done | wc -l)"
echo "kept .command.cache: $(for d in "${dirs[@]}"; do [ -f "$d/.command.cache" ] && echo "$d"; done | wc -l) of ${#dirs[@]}"
sleep 300
echo "quota 5 min after: $(quota)"
