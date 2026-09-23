#!/bin/bash
# E1: one nf-core/mag 5.5.0 nextflow head for one sample sheet, as a Slurm job on fir.
#   sbatch -J e1_short <checkout>/research/metasmith_benchmark/drivers/e1_nfcore/run_e1.sbatch <checkout> short
#
# The run-to-run control (E1ctl) is the same head with a third argument `ctl`: the subset
# sheets control_subset.py wrote, E1's own assemblies as --assembly_input, and new seeds.
#   sbatch -J e1ctl_short .../run_e1.sbatch <checkout> short ctl
#
# The head cannot fetch. Before the first submission, on a login node: prepull.sh finishes, and
# `NXF_HOME=$HOME/.nextflow26 nextflow26 info nf-core/mag` resolves revision 5.5.0 locally.
#
# Stop with `scancel --batch --signal=USR1 <job>`. The trap sends nextflow TERM, and its shutdown
# cancels the grid jobs. A plain scancel kills the head before it does.
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=10
#SBATCH --mem=48G
#SBATCH --time=7-00:00:00
#SBATCH --signal=B:USR1@1800
#SBATCH --output=/scratch/phyberos/bench/logs/%x.%j.out
set -euo pipefail

CHECKOUT="$1"
ARM="$2"
VARIANT="${3:-}"
HERE="$CHECKOUT/research/metasmith_benchmark/drivers/e1_nfcore"
WORK="/scratch/phyberos/bench/e1$VARIANT/$ARM"
SHEET="$HERE/samplesheet.$ARM${VARIANT:+.$VARIANT}.csv"
EXTRA=()
if [ "$VARIANT" = ctl ]; then
    EXTRA=(-c "$HERE/control_seed.config" --assembly_input "$HERE/assembly_input.$ARM.ctl.csv")
    [ "$ARM" = long ] && EXTRA+=(-c "$HERE/control_long.config")
elif [ -n "$VARIANT" ]; then
    echo "unknown variant: $VARIANT" >&2
    exit 2
fi
JOB="${SLURM_JOB_ID}"

# nextflow26 runs on the module's Java.
module load nextflow/25.04.6 apptainer/1.3.5
export NXF_HOME="$HOME/.nextflow26"
export NXF_OFFLINE=true
export NXF_OPTS="-Xmx6g -XX:ActiveProcessorCount=4"
export NXF_APPTAINER_CACHEDIR=/scratch/phyberos/wave2_b3_nfcore/apptainer_cache/nxf
export NXF_SINGULARITY_CACHEDIR="$NXF_APPTAINER_CACHEDIR"
export APPTAINER_TMPDIR="${SLURM_TMPDIR:-/tmp}"
export SINGULARITY_TMPDIR="$APPTAINER_TMPDIR"

mkdir -p "$WORK/reports"
cd "$WORK"
echo "commit $(cat "$CHECKOUT/COMMIT") on $(hostname), job $JOB: E1$VARIANT $ARM"

"$HOME/bin/nextflow26" run nf-core/mag -r 5.5.0 \
    -profile fir -c "$HERE/fir.config" -c "$HERE/control.config" "${EXTRA[@]}" \
    --input "$SHEET" --outdir "$WORK/out" \
    -work-dir "$WORK/work" -resume -ansi-log false \
    -with-dag "$WORK/reports/dag.$JOB.dot" \
    -with-trace "$WORK/reports/trace.$JOB.tsv" \
    -with-report "$WORK/reports/report.$JOB.html" &
nf=$!
echo "$nf $(hostname)" > "$WORK/nextflow.pid"
trap 'kill -TERM "$nf" 2>/dev/null || true' USR1

status=0
while :; do
    wait "$nf" && status=0 || status=$?
    kill -0 "$nf" 2>/dev/null || break
done
echo "NXF_EXIT=$status"
exit "$status"
