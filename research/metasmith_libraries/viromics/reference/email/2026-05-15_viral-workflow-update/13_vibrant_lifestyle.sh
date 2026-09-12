#!/bin/bash
#SBATCH --job-name=13_vibrant_lifestyle
#SBATCH --output=13_vibrant_lifestyle_%j.out
#SBATCH --error=13_vibrant_lifestyle_%j.err
#SBATCH --partition=albaicin-big


set -euo pipefail

source /LUSTRE/home/RNM270/ach/miniforge3/etc/profile.d/conda.sh
conda activate vibrant

BASE="/SCRATCH/RNM270/ach/Viromics"

IN="${BASE}/09_vOTUs_filter3_large_mobile/vOTUs.filter3.no_large_mobile.fasta"
OUT="${BASE}/11_VIBRANT_final_vOTUs"

VIBRANT_DB="${BASE}/databases/VIBRANT/databases"
VIBRANT_FILES="${BASE}/databases/VIBRANT/files"

mkdir -p "${OUT}"

echo "Input final vOTUs:"
echo "${IN}"

echo "VIBRANT database:"
echo "${VIBRANT_DB}"

echo "VIBRANT files:"
echo "${VIBRANT_FILES}"

echo "Checking files..."
test -s "${IN}"
test -d "${VIBRANT_DB}"
test -d "${VIBRANT_FILES}"

echo "Checking VIBRANT..."
which VIBRANT_run.py

echo "Checking VIBRANT database HMM files..."
find "${VIBRANT_DB}" -name "*.HMM" | head

echo "Running VIBRANT..."

VIBRANT_run.py \
    -i "${IN}" \
    -folder "${OUT}" \
    -t 24 \
    -f nucl \
    -no_plot \
    -d "${VIBRANT_DB}" \
    -m "${VIBRANT_FILES}"

echo "VIBRANT finished."
echo "Output folder:"
echo "${OUT}"