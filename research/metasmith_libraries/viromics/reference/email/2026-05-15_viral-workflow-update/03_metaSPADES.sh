#!/bin/bash
#SBATCH --job-name=02_SPAdes_onlyasm
#SBATCH --output=02_SPAdes_onlyasm_%A_%a.out
#SBATCH --error=02_SPAdes_onlyasm_%A_%a.err
#SBATCH --partition=albaicin-fat
#SBATCH --array=0-11%2

set -euo pipefail

source /LUSTRE/home/RNM270/ach/miniforge3/etc/profile.d/conda.sh
conda activate viromics

BASE="/SCRATCH/RNM270/ach/Viromics"
TRIM="${BASE}/01_QC_trimmed_trimmomatic/trimmed"
OUT="${BASE}/02_SPAdes_only_assembler"

THREADS=24
MEM=240

SAMPLES=(
VSG15W5
VSG15X5
VSG20W5
VSG20X5
VSG1W5
VSG1X5
VSG22W5
VSG22X5
VSG40W5
VSG40X5
VSG38W5
VSG38X53
)

TASK_ID=${SLURM_ARRAY_TASK_ID:-0}

SAMPLE=${SAMPLES[$TASK_ID]}

R1="${TRIM}/${SAMPLE}_R1.paired.fq.gz"
R2="${TRIM}/${SAMPLE}_R2.paired.fq.gz"
SAMPLE_OUT="${OUT}/${SAMPLE}"

mkdir -p "${OUT}"

echo "Sample: ${SAMPLE}"
echo "R1: ${R1}"
echo "R2: ${R2}"
echo "Output: ${SAMPLE_OUT}"

if [[ -s "${SAMPLE_OUT}/contigs.fasta" ]]; then
    echo "SPAdes output already exists for ${SAMPLE}. Skipping."
    exit 0
fi

rm -rf "${SAMPLE_OUT}"

spades.py \
    --meta \
    --only-assembler \
    -1 "${R1}" \
    -2 "${R2}" \
    -t "${THREADS}" \
    -m "${MEM}" \
    -o "${SAMPLE_OUT}"

echo "Finished metaSPAdes only-assembler for ${SAMPLE}"