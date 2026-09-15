#!/bin/bash
# Run tombstone_keys.py on a compute node.
#   sbatch <checkout>/research/metasmith_benchmark/drivers/tombstone_keys.sbatch <checkout> <cache root> <keys file> [--apply]
# WARNING --apply turns every listed shard into a cache miss. Run it without --apply first.
# CAUTION Slurm runs a spooled copy of this script, so the checkout comes in as an argument.
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=0:30:00
#SBATCH --output=/scratch/phyberos/bench/logs/tombstone_keys.%j.out
set -euo pipefail
CHECKOUT="$1"; shift
source "$CHECKOUT/research/metasmith_benchmark/drivers/host_env.sh"
export PYTHONPATH="$CHECKOUT/src"
"$BENCH_PYTHON" -u "$CHECKOUT/research/metasmith_benchmark/drivers/tombstone_keys.py" "$@"
