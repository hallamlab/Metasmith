#!/bin/bash
# One array task = one part of one `ecspr fba` job. Submitted by run.py --submit, which
# passes JOB / SIF / ROOT / MNX / FRAME / GPR / NTASKS through --export.
#
# --account, --array and --time come from the sbatch command line because they are
# per-job; everything constant is here.
#
# `--mem` IS NOT OPTIONAL. Omitting it gives the partition default of 256 MB and cobra
# OOMs while loading iML1515, which SLURM reports as an ordinary non-zero exit.
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --output=/scratch/phyberos/fabfos_fba/logs/%x_%A_%a.out
set -euo pipefail
module load apptainer

# ONE THREAD, DELIBERATELY. The LP is small and the array is wide, so a BLAS that grabs
# every core turns 32 one-core tasks into 32 tasks fighting over one node's worth of
# threads. Each variable is read by a different backend and all of them ship in the image.
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
       NUMEXPR_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1

IN=$ROOT/inputs/$JOB
RUN=$ROOT/runs/$JOB
mkdir -p "$RUN/shards"

# COLON-SEPARATED, because `--export` reads a comma as the end of the value and cannot
# carry a space at all -- see run.py --submit.
GPR_ARGS=()
IFS=':' read -ra _gpr <<< "$GPR"
for g in "${_gpr[@]}"; do
    [ -f "$IN/gpr/$g" ] || { echo "no such GPR table: $IN/gpr/$g" >&2; exit 1; }
    GPR_ARGS+=("$IN/gpr/$g")
done

MNX_ARGS=()
if [ -n "${MNX:-}" ]; then
    # The sliced parquets are staged read-only beside the release they came from, so the
    # cache is pointed at OUR copy rather than at the release directory.
    MNX_ARGS=(--mnx "$MNX" --mnx-cache "$ROOT/inputs/mnx_cache")
fi

apptainer exec -B /scratch "$SIF" \
    ecspr fba \
        --gpr "${GPR_ARGS[@]}" \
        --conditions "$IN/conditions.parquet" \
        --tests "$IN/gem_tests.parquet" \
        --media "$IN/media.parquet" \
        --frame "$ROOT/inputs/frames/$FRAME.json" \
        "${MNX_ARGS[@]}" \
        --no-gapfill \
        --shard-dir "$RUN/shards" \
        --part "$SLURM_ARRAY_TASK_ID" --of "$NTASKS" \
        --census "$RUN/census_${SLURM_ARRAY_TASK_ID}.json" \
        --out "$RUN/part_${SLURM_ARRAY_TASK_ID}.parquet"
