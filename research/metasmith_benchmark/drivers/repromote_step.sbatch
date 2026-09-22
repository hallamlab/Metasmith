#!/bin/bash
# Run repromote_step.py on a compute node, with the quota before and 5 min after.
#   sbatch <checkout>/research/metasmith_benchmark/drivers/repromote_step.sbatch <checkout> <run dir> <cache root> <step> [--exclude FILE] [--bam-min-align PCT] [--apply]
# Run it without --apply first and read the count.
# CAUTION Slurm runs a spooled copy of this script, so the checkout comes in as an argument.
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=3:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/repromote.%j.out
set -euo pipefail
CHECKOUT="$1"; shift
source "$CHECKOUT/research/metasmith_benchmark/drivers/host_env.sh"
export PYTHONPATH="$CHECKOUT/src"
quota() { lfs quota -p 83115734 /scratch | tail -1 | awk '{printf "%.3f TiB, %s inodes\n", $2/2^30, $6}'; }
echo "quota before: $(quota)"
"$BENCH_PYTHON" -u "$CHECKOUT/research/metasmith_benchmark/drivers/repromote_step.py" "$@"
if [[ " $* " == *" --apply "* ]]; then sleep 300; echo "quota 5 min after: $(quota)"; fi
