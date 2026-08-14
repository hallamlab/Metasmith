#!/bin/bash
#SBATCH --job-name=03_megahit
#SBATCH --output=03_megahit_%j.out
#SBATCH --error=03_megahit_%j.err
#SBATCH --partition=albaicin-fat

set -euo pipefail

source /LUSTRE/home/RNM270/ach/miniforge3/etc/profile.d/conda.sh
conda activate viromics

BASE="/SCRATCH/RNM270/ach/Viromics"
TRIM="${BASE}/01_QC_trimmed_trimmomatic/trimmed"
OUT="${BASE}/03_MEGAHIT"

mkdir -p ${OUT}

THREADS=24

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

for SAMPLE in "${SAMPLES[@]}"; do

    echo "Running MEGAHIT for ${SAMPLE}"

    megahit \
        -1 ${TRIM}/${SAMPLE}_R1.paired.fq.gz \
        -2 ${TRIM}/${SAMPLE}_R2.paired.fq.gz \
        -t ${THREADS} \
        -m 0.85 \
        -o ${OUT}/${SAMPLE}

done