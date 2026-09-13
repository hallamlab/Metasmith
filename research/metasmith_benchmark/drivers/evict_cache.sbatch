#!/bin/bash
# Run evict_cache.py against one agent home as a compute-node job.
#
#   sbatch evict_cache.sbatch <checkout> <agent home> <keys file> [--apply] [--protect-run KEY ...]
#
# Submit without --apply first and read the log, then resubmit with --apply.
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=3:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/evict.%j.out
set -euo pipefail

CHECKOUT="$1"
HOME_DIR="$2"
KEYS="$3"
shift 3

source "$CHECKOUT/research/metasmith_benchmark/drivers/host_env.sh"
export PYTHONPATH="$CHECKOUT/src"
quota() { lfs quota -p 83115734 /scratch | tail -1 | awk '{printf "%.3f TiB, %s inodes\n", $2/2^30, $6}'; }

echo "quota before: $(quota)"
"$BENCH_PYTHON" -u "$CHECKOUT/research/metasmith_benchmark/drivers/evict_cache.py" --home "$HOME_DIR" --keys "$KEYS" "$@"
sleep 300
echo "quota 5 min after: $(quota)"
