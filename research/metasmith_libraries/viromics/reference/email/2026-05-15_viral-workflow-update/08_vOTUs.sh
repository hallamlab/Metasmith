#!/bin/bash
#SBATCH --job-name=08_votu_clustering
#SBATCH --output=08_votu_clustering_%j.out
#SBATCH --error=08_votu_clustering_%j.err
#SBATCH --partition=albaicin-fat

set -euo pipefail

source /LUSTRE/home/RNM270/ach/miniforge3/etc/profile.d/conda.sh

conda activate genomad

BASE="/SCRATCH/RNM270/ach/Viromics"

INPUT="${BASE}/07_filtered_viral_contigs/checkv/viruses.fna"

OUT="${BASE}/08_vOTUs"

TMP="${OUT}/tmp"

THREADS=24

mkdir -p "${OUT}"
mkdir -p "${TMP}"

# =====================================================
# Create MMseqs database
# =====================================================

mmseqs createdb \
    "${INPUT}" \
    "${OUT}/viral_db"

# =====================================================
# Cluster vOTUs
# 95% identity
# 80% coverage of shorter sequence
# =====================================================

mmseqs cluster \
    "${OUT}/viral_db" \
    "${OUT}/viral_clusters" \
    "${TMP}" \
    --min-seq-id 0.95 \
    -c 0.80 \
    --cov-mode 1 \
    --threads "${THREADS}"

# =====================================================
# Export cluster table
# =====================================================

mmseqs createtsv \
    "${OUT}/viral_db" \
    "${OUT}/viral_db" \
    "${OUT}/viral_clusters" \
    "${OUT}/vOTU_clusters.tsv"

# =====================================================
# Export representative sequences
# =====================================================

mmseqs createsubdb \
    "${OUT}/viral_clusters" \
    "${OUT}/viral_db" \
    "${OUT}/vOTU_representatives"

mmseqs convert2fasta \
    "${OUT}/vOTU_representatives" \
    "${OUT}/vOTU_representatives.fna"

echo "Number of vOTUs:"
grep -c "^>" "${OUT}/vOTU_representatives.fna"

echo "Done."