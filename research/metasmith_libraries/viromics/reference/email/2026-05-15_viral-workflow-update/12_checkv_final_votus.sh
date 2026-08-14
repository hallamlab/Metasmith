#!/bin/bash
#SBATCH --job-name=12_checkv_final_votus
#SBATCH --output=12_checkv_final_votus_%j.out
#SBATCH --error=12_checkv_final_votus_%j.err
#SBATCH --partition=albaicin-big

set -euo pipefail

source /LUSTRE/home/RNM270/ach/miniforge3/etc/profile.d/conda.sh
conda activate checkv

BASE="/SCRATCH/RNM270/ach/Viromics"

IN="${BASE}/09_vOTUs_filter3_large_mobile/vOTUs.filter3.no_large_mobile.fasta"
OUT="${BASE}/10_CheckV_final_vOTUs"
CHECKV_DB="${BASE}/databases/checkv/checkv-db-v1.5"

THREADS=24

mkdir -p "${OUT}"

echo "Input final curated vOTUs:"
echo "${IN}"

echo "CheckV database:"
echo "${CHECKV_DB}"

which checkv

checkv end_to_end \
    "${IN}" \
    "${OUT}" \
    -d "${CHECKV_DB}" \
    -t "${THREADS}"

echo "CheckV finished."

echo "Main output files:"
echo "${OUT}/quality_summary.tsv"
echo "${OUT}/completeness.tsv"
echo "${OUT}/contamination.tsv"
echo "${OUT}/complete_genomes.tsv"