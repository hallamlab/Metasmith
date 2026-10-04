#!/bin/bash
# One benchmark driver as a Slurm job on fir. The driver's client, its nextflow and the run's
# local steps all live inside this job, which ends when the run does.
#
# Submit from a fir login node, from a checkout sync.sh wrote:
#   sbatch -J <lane> <checkout>/research/metasmith_benchmark/drivers/submit_driver.sbatch \
#       <checkout> <driver.py> run --materialise --import [driver args...]
# then, once that job passes, the same line with --launch and no --import.
# CAUTION one importing job per agent home at a time: concurrent imports lock the pool's sqlite.
# CAUTION start two drivers on one home at least 3 min apart (--dependency=after:<prev>+3): each links
# relay/<hostname> in the home, and two starting together on one node race on it.
# E5's batch 0 drivers averaged under 0.1 core at 1.8 GB MaxRSS; 12G covers nextflow's -Xmx6g and the
# client. A run longer than 3 days is cancelled at the wall and resumes from the task cache on resubmit.
#
# Stop a run with `scancel --batch --signal=USR1 <job>` or `runctl.py cancel <corpus> <key>`, never a
# plain scancel. Slurm sends USR1 30 min before the wall, and the driver turns it into the same cancel.
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=2
#SBATCH --mem=12G
#SBATCH --time=3-00:00:00
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
