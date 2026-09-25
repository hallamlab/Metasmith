#!/bin/bash
# Score one staged bin of e4_quality_sample.py three ways: its restored model on MEMOTE 0.9.13 as
# metaGEM ran it, and both its restored model and metaGEM's GEM on MEMOTE 0.17 as the modern
# lane ran it (memote_score.py). Array index = line of <work>/bins.txt.
#
#   sbatch --array=1-<n> e4_quality_sample.sbatch <work> <images dir> <memote_score.py>
#SBATCH --account=rpp-shallam
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=3:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/e4_quality_sample.%A_%a.out
set -euo pipefail

WORK="$1"
IMAGES="$2"
SCORE="$3"
BIN=$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$WORK/bins.txt" | cut -f1)
T="${SLURM_TMPDIR:-/tmp}/e4q.$SLURM_ARRAY_TASK_ID"
mkdir -p "$T" "$WORK"/memote/{restored_0913,restored_017,metagem_017}
module load apptainer 2>/dev/null || true

in_image() {
    apptainer exec --cleanenv --env "PYTHONNOUSERSITE=1,HOME=$T" -B "$WORK,$T,$(dirname "$SCORE")" --pwd "$T" "$IMAGES/$1.sif" "${@:2}"
}

in_image memote_0913 memote run \
    --skip test_find_metabolites_produced_with_closed_bounds \
    --skip test_find_metabolites_consumed_with_closed_bounds \
    --skip test_find_metabolites_not_produced_with_open_bounds \
    --skip test_find_metabolites_not_consumed_with_open_bounds \
    --skip test_find_incorrect_thermodynamic_reversibility \
    --filename "$WORK/memote/restored_0913/$BIN.json.gz" "$WORK/models/restored/$BIN.xml"

for run in restored metagem; do
    in_image memote_017 python "$SCORE" "$WORK/models/$run/$BIN.xml" "$T/report.html" \
        "$WORK/memote/${run}_017/$BIN.json.gz" "$WORK/memote/${run}_017/$BIN.score.json"
done
