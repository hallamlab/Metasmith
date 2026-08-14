#!/bin/bash
#SBATCH --job-name=01_fastqc_trimmomatic
#SBATCH --output=01_fastqc_trimmomatic_%j.out
#SBATCH --error=01_fastqc_trimmomatic_%j.err

set -euo pipefail

source /LUSTRE/home/RNM270/ach/miniforge3/etc/profile.d/conda.sh

conda activate viromics

BASE="/SCRATCH/RNM270/ach/Viromics"
RAW="${BASE}"
OUT="${BASE}/01_QC_trimmed_trimmomatic"

mkdir -p "${OUT}"/{fastqc_raw,trimmed,fastqc_trimmed,logs}

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

echo "Checking tools..."
which fastqc
which trimmomatic

echo "Starting raw FastQC..."

for SAMPLE in "${SAMPLES[@]}"; do

    R1=$(ls ${RAW}/${SAMPLE}_*_1.fq.gz)
    R2=$(ls ${RAW}/${SAMPLE}_*_2.fq.gz)

    echo "FastQC raw: ${SAMPLE}"

    fastqc \
        -t ${THREADS} \
        -o "${OUT}/fastqc_raw" \
        "${R1}" "${R2}"

done

echo "Starting trimming with Trimmomatic..."

for SAMPLE in "${SAMPLES[@]}"; do

    R1=$(ls ${RAW}/${SAMPLE}_*_1.fq.gz)
    R2=$(ls ${RAW}/${SAMPLE}_*_2.fq.gz)

    echo "Trimming: ${SAMPLE}"

    trimmomatic PE \
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

echo "Starting FastQC on trimmed paired reads..."

for SAMPLE in "${SAMPLES[@]}"; do

    fastqc \
        -t ${THREADS} \
        -o "${OUT}/fastqc_trimmed" \
        "${OUT}/trimmed/${SAMPLE}_R1.paired.fq.gz" \
        "${OUT}/trimmed/${SAMPLE}_R2.paired.fq.gz"

done

echo "FastQC + Trimmomatic completed successfully."