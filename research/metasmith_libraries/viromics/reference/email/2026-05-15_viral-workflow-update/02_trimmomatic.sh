#!/bin/bash
#SBATCH --job-name=01b_trimmomatic_viromics
#SBATCH --output=01b_trimmomatic_%j.out
#SBATCH --error=01b_trimmomatic_%j.err
#SBATCH --partition=albaicin-fat
#SBATCH --cpus-per-task=12
#SBATCH --mem=120G
#SBATCH --time=24:00:00

set -euo pipefail

source /LUSTRE/home/RNM270/ach/miniforge3/etc/profile.d/conda.sh
conda activate viromics

BASE="/SCRATCH/RNM270/ach/Viromics"
RAW="${BASE}"
OUT="${BASE}/01_QC_trimmed_trimmomatic"

mkdir -p "${OUT}"/{trimmed,logs}

THREADS=12

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

echo "Checking Trimmomatic..."
which trimmomatic
trimmomatic -version

echo "Starting Trimmomatic..."

for SAMPLE in "${SAMPLES[@]}"; do

    R1=$(ls ${RAW}/${SAMPLE}_*_1.fq.gz)
    R2=$(ls ${RAW}/${SAMPLE}_*_2.fq.gz)

    echo "Processing ${SAMPLE}"
    echo "R1: ${R1}"
    echo "R2: ${R2}"

    rm -f "${OUT}/trimmed/${SAMPLE}"_R*.fq.gz

    trimmomatic PE \
        -Xmx100G \
        -threads ${THREADS} \
        -phred33 \
        "${R1}" "${R2}" \
        "${OUT}/trimmed/${SAMPLE}_R1.paired.fq.gz" \
        "${OUT}/trimmed/${SAMPLE}_R1.unpaired.fq.gz" \
        "${OUT}/trimmed/${SAMPLE}_R2.paired.fq.gz" \
        "${OUT}/trimmed/${SAMPLE}_R2.unpaired.fq.gz" \
        LEADING:3 \
        TRAILING:3 \
        SLIDINGWINDOW:4:15 \
        MINLEN:50 \
        2> "${OUT}/logs/${SAMPLE}.trimmomatic.log"

done

echo "Trimmomatic completed successfully."