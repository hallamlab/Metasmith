#!/bin/bash
# Run evict_step_shards.py on a compute node, with the quota before and 5 min after.
#   sbatch <checkout>/research/metasmith_benchmark/drivers/evict_step_shards.sbatch <checkout> <run dir> <cache root> <step> [--apply]
# WARNING --apply turns every qualifying shard into a cache miss. Run it without --apply first and read the count.
# CAUTION Slurm runs a spooled copy of this script, so the checkout comes in as an argument.
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=3:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/evict_shards.%j.out
set -euo pipefail
CHECKOUT="$1"; shift
quota() { lfs quota -p 83115734 /scratch | tail -1 | awk '{printf "%.3f TiB, %s inodes\n", $2/2^30, $6}'; }
echo "quota before: $(quota)"
python3 "$CHECKOUT/research/metasmith_benchmark/drivers/evict_step_shards.py" "$@"
if [[ " $* " == *" --apply "* ]]; then sleep 300; echo "quota 5 min after: $(quota)"; fi
