#!/bin/bash
# One benchmark driver as a Slurm job on fir. The driver's client, its nextflow and the run's
# local steps all live inside this job, which ends when the run does.
#
# Submit from a fir login node, from a checkout sync.sh wrote:
#   sbatch -J <lane> <checkout>/research/metasmith_benchmark/drivers/submit_driver.sbatch \
#       <checkout> <driver.py> run --launch --import [driver args...]
#
# Stop a run with `runctl.py cancel <corpus> <key>`, never scancel. Slurm sends USR1 30 min
# before the wall, and the driver turns it into the same cancel.
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=10
#SBATCH --mem=48G
#SBATCH --time=7-00:00:00
#SBATCH --signal=B:USR1@1800
#SBATCH --output=/scratch/phyberos/bench/logs/%x.%j.out
set -euo pipefail

CHECKOUT="$1"
DRIVER="$2"
shift 2

source "$CHECKOUT/research/metasmith_benchmark/drivers/host_env.sh"
export BENCH_ON_HOST=1
export PYTHONPATH="$CHECKOUT/src"
echo "commit $(cat "$CHECKOUT/COMMIT") on $(hostname), job ${SLURM_JOB_ID}: $DRIVER $*"

cd "$CHECKOUT/research/metasmith_benchmark/drivers"
"$BENCH_PYTHON" -u "$DRIVER" "$@" &
child=$!
trap 'kill -USR1 "$child" 2>/dev/null || true' USR1

status=0
while :; do
    wait "$child" && status=0 || status=$?
    kill -0 "$child" 2>/dev/null || break
done
exit "$status"
