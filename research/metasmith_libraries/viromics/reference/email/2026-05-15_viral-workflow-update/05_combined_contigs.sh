#!/bin/bash
#SBATCH --job-name=04_combine_derep_contigs
#SBATCH --output=04_combine_derep_%j.out
#SBATCH --error=04_combine_derep_%j.err
#SBATCH --partition=albaicin-fat

set -euo pipefail

source /LUSTRE/home/RNM270/ach/miniforge3/etc/profile.d/conda.sh
conda activate viromics

BASE="/SCRATCH/RNM270/ach/Viromics"

SPADES="${BASE}/02_SPAdes_only_assembler"
MEGAHIT="${BASE}/03_MEGAHIT"
OUT="${BASE}/04_combined_contigs"

mkdir -p "${OUT}"

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

echo "Checking tools..."
which seqkit
which mmseqs

echo "Collecting and renaming contigs..."

> "${OUT}/all_assemblies_combined.fasta"

for SAMPLE in "${SAMPLES[@]}"; do

    SPADES_CONTIGS="${SPADES}/${SAMPLE}/contigs.fasta"
    MEGAHIT_CONTIGS="${MEGAHIT}/${SAMPLE}/final.contigs.fa"

    if [[ -s "${SPADES_CONTIGS}" ]]; then
        echo "Adding metaSPAdes contigs for ${SAMPLE}"
        seqkit replace \
            -p '^(.+)' \
            -r "${SAMPLE}|metaSPAdes|\$1" \
            "${SPADES_CONTIGS}" \
            >> "${OUT}/all_assemblies_combined.fasta"
    else
        echo "WARNING: missing SPAdes contigs for ${SAMPLE}"
    fi

    if [[ -s "${MEGAHIT_CONTIGS}" ]]; then
        echo "Adding MEGAHIT contigs for ${SAMPLE}"
        seqkit replace \
            -p '^(.+)' \
            -r "${SAMPLE}|MEGAHIT|\$1" \
            "${MEGAHIT_CONTIGS}" \
            >> "${OUT}/all_assemblies_combined.fasta"
    else
        echo "WARNING: missing MEGAHIT contigs for ${SAMPLE}"
    fi

done

echo "Filtering contigs >= 1000 bp..."

seqkit seq \
    -m 1000 \
    "${OUT}/all_assemblies_combined.fasta" \
    > "${OUT}/all_assemblies_combined.min1kb.fasta"

echo "Dereplicating contigs with MMseqs2..."

cd "${OUT}"

mmseqs easy-cluster \
    all_assemblies_combined.min1kb.fasta \
    viral_precluster \
    tmp_mmseqs \
    --min-seq-id 0.95 \
    -c 0.80 \
    --cov-mode 0 \
    --threads "${THREADS}"

echo "Renaming representative contigs..."

seqkit replace \
    -p '^(.+)' \
    -r 'vPre_\$1' \
    viral_precluster_rep_seq.fasta \
    > viral_precluster_rep_seq.renamed.fasta

echo "Generating summary statistics..."

seqkit stats \
    all_assemblies_combined.fasta \
    all_assemblies_combined.min1kb.fasta \
    viral_precluster_rep_seq.fasta \
    viral_precluster_rep_seq.renamed.fasta \
    > contig_combination_dereplication_stats.txt

echo "Done."
echo "Main output:"
echo "${OUT}/viral_precluster_rep_seq.renamed.fasta"