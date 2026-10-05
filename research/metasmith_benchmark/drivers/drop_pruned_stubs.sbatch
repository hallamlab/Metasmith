#!/bin/bash
# Delete the task-dir stubs that prune_work.sbatch leaves in a finished run's nxf_work, to return their inodes.
#
#   sbatch <checkout>/research/metasmith_benchmark/drivers/drop_pruned_stubs.sbatch <run dir>...
#
# A stub is a task dir holding only .command.cache. record_run reads it once, at run end, and
# evict_step_shards.py --finished reads it to find the shards a run promoted.
# WARNING keep the stubs of a run whose promoted shards may still be evicted.
# CAUTION a run aborts the job before anything is deleted when its trace is missing, its nxf_work
# holds anything but stubs, or a queued job works inside it.
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=3:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/drop_stubs.%j.out
set -euo pipefail
quota() { lfs quota -p 83115734 /scratch | tail -1 | awk '{printf "%.3f TiB, %s inodes\n", $2/2^30, $6}'; }

runs=()
for a in "$@"; do
    r="$(realpath "$a")"
    [[ "$r" =~ ^/scratch/phyberos/e5/metasmith/runs/[A-Za-z0-9]{8}$ ]] || { echo "ABORT: $a is not an E5 run dir"; exit 1; }
    [ -s "$r/_metasmith/trace.jsonl" ] || { echo "ABORT: $r has no recorded trace"; exit 1; }
    extra=$(find "$r/nxf_work" -mindepth 3 ! -name .command.cache -print -quit)
    [ -z "$extra" ] || { echo "ABORT: $r/nxf_work holds more than stubs: $extra"; exit 1; }
    squeue -u "$USER" -h -o "%Z" | grep -q "^$r" && { echo "ABORT: a queued job works in $r"; exit 1; }
    runs+=("$r")
done
echo "quota before: $(quota)"
for r in "${runs[@]}"; do
    echo "$r: $(find "$r/nxf_work" -mindepth 1 | wc -l) entries"
    find "$r/nxf_work" -mindepth 1 -delete
done
sleep 300
echo "quota 5 min after: $(quota)"
