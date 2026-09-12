#!/bin/bash
#SBATCH --job-name=14_virus_overview_table
#SBATCH --output=14_virus_overview_table_%j.out
#SBATCH --error=14_virus_overview_table_%j.err
#SBATCH --partition=albaicin-big

set -euo pipefail

source /LUSTRE/home/RNM270/ach/miniforge3/etc/profile.d/conda.sh
conda activate genomad

BASE="/SCRATCH/RNM270/ach/Viromics"

CHECKV="${BASE}/10_CheckV_final_vOTUs/quality_summary.tsv"

VIBRANT_PHAGE_DIR="${BASE}/11_VIBRANT_final_vOTUs/VIBRANT_vOTUs.filter3.no_large_mobile/VIBRANT_phages_vOTUs.filter3.no_large_mobile"

LYTIC="${VIBRANT_PHAGE_DIR}/vOTUs.filter3.no_large_mobile.phages_lytic.fna"
LYSOGENIC="${VIBRANT_PHAGE_DIR}/vOTUs.filter3.no_large_mobile.phages_lysogenic.ffn"

OUT="${BASE}/12_final_tables"
mkdir -p "${OUT}"

LIFESTYLE="${OUT}/VIBRANT_lifestyle_by_vOTU.tsv"
FINAL_TSV="${OUT}/Supplementary_Data_2_virus_overview_vOTU_ge5kb.tsv"
FINAL_XLSX="${OUT}/Supplementary_Data_2_virus_overview_vOTU_ge5kb.xlsx"

echo "Creating VIBRANT lifestyle table..."

echo -e "contig_id\tVirus_lifestyle_VIBRANT" > "${LIFESTYLE}"

grep "^>" "${LYTIC}" \
    | sed 's/^>//' \
    | awk '{print $1"\tvirulent"}' \
    >> "${LIFESTYLE}"

grep "^>" "${LYSOGENIC}" \
    | sed 's/^>//' \
    | awk '{print $1}' \
    | sed 's/_fragment.*//' \
    | sort -u \
    | awk '{print $1"\ttemperate"}' \
    >> "${LIFESTYLE}"

echo "Merging CheckV + VIBRANT lifestyle..."

python <<EOF
import pandas as pd

checkv = pd.read_csv("${CHECKV}", sep="\t")
life = pd.read_csv("${LIFESTYLE}", sep="\t")

# Keep only vOTUs >=5 kb, as in the Supplementary Data 2 example
checkv = checkv[checkv["contig_length"] >= 5000].copy()

# Merge lifestyle
df = checkv.merge(life, on="contig_id", how="left")

# If VIBRANT did not classify lifestyle
df["Virus_lifestyle_VIBRANT"] = df["Virus_lifestyle_VIBRANT"].fillna("not_classified")

# Select columns matching the paper-style table
cols = [
    "contig_id",
    "contig_length",
    "provirus",
    "proviral_length",
    "gene_count",
    "viral_genes",
    "host_genes",
    "checkv_quality",
    "miuvig_quality",
    "completeness",
    "completeness_method",
    "contamination",
    "kmer_freq",
    "warnings",
    "Virus_lifestyle_VIBRANT"
]

df = df[[c for c in cols if c in df.columns]]

df.to_csv("${FINAL_TSV}", sep="\t", index=False)
df.to_excel("${FINAL_XLSX}", index=False)

print("Rows:", len(df))
print(df["Virus_lifestyle_VIBRANT"].value_counts(dropna=False))
EOF

echo "Done."
echo "TSV:"
echo "${FINAL_TSV}"
echo "Excel:"
echo "${FINAL_XLSX}"