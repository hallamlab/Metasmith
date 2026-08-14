#!/bin/bash
#SBATCH --job-name=06_genomad
#SBATCH --output=06_genomad_%j.out
#SBATCH --error=06_genomad_%j.err
#SBATCH --partition=albaicin-fat


set -euo pipefail

source /LUSTRE/home/RNM270/ach/miniforge3/etc/profile.d/conda.sh
conda activate genomad

BASE="/SCRATCH/RNM270/ach/Viromics"

IN="${BASE}/04_combined_contigs/viral_precluster_rep_seq.renamed.fasta"
OUT="${BASE}/06_geNomad"
DB="${BASE}/databases/genomad/genomad_db"

THREADS=24

mkdir -p "${OUT}"

echo "Input:"
echo "${IN}"

echo "Database:"
echo "${DB}"

which genomad
genomad --version || true

genomad end-to-end \
    --cleanup \
    --splits 8 \
    --threads "${THREADS}" \
    "${IN}" \
    "${OUT}" \
    "${DB}"

echo "geNomad finished."

echo "Virus FASTA:"
find "${OUT}" -name "*_virus.fna"

echo "Virus summary:"
find "${OUT}" -name "*_virus_summary.tsv"

echo "Number of geNomad viral sequences:"
grep -c "^>" $(find "${OUT}" -name "*_virus.fna" | head -n 1)