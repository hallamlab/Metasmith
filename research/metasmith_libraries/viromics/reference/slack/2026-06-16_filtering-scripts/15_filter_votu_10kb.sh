#!/bin/bash
#SBATCH --job-name=14_virus_overview_table
#SBATCH --output=14_virus_overview_table_%j.out
#SBATCH --error=14_virus_overview_table_%j.err
#SBATCH --partition=albaicin-big

source /LUSTRE/home/RNM270/ach/miniforge3/etc/profile.d/conda.sh
conda activate genomad

BASE="/SCRATCH/RNM270/ach/Viromics"

mkdir -p "${BASE}/13_DRAMv"

seqkit seq -m 10000 \
  "${BASE}/09_vOTUs_filter3_large_mobile/vOTUs.filter3.no_large_mobile.fasta" \
  > "${BASE}/13_DRAMv/vOTUs.filter3.no_large_mobile.min10kb.fasta"

grep -c "^>" "${BASE}/13_DRAMv/vOTUs.filter3.no_large_mobile.min10kb.fasta"

seqkit stats "${BASE}/13_DRAMv/vOTUs.filter3.no_large_mobile.min10kb.fasta"