#!/bin/bash
#SBATCH --job-name=07_filter_checkv
#SBATCH --output=07_filter_checkv_%j.out
#SBATCH --error=07_filter_checkv_%j.err
#SBATCH --partition=albaicin-fat

set -euo pipefail

source /LUSTRE/home/RNM270/ach/miniforge3/etc/profile.d/conda.sh

BASE="/SCRATCH/RNM270/ach/Viromics"

GENOMAD_SUM="${BASE}/06_geNomad/viral_precluster_rep_seq.renamed_summary"
VIRAL_FNA="${GENOMAD_SUM}/viral_precluster_rep_seq.renamed_virus.fna"
GENES_TSV="${GENOMAD_SUM}/viral_precluster_rep_seq.renamed_virus_genes.tsv"

OUT="${BASE}/07_filtered_viral_contigs"
CHECKV_DB="${BASE}/databases/checkv/checkv-db-v1.5"

THREADS=24

mkdir -p "${OUT}"

# =====================================================
# 1. Activate geNomad environment
# =====================================================

conda activate genomad

echo "Input viral FASTA:"
echo "${VIRAL_FNA}"

echo "Input geNomad genes table:"
echo "${GENES_TSV}"

which python
which seqkit

# =====================================================
# 2. Filter contigs using geNomad gene table + length >= 1 kb
# =====================================================

python <<EOF
import pandas as pd
from Bio import SeqIO

genes = pd.read_csv("${GENES_TSV}", sep="\t")

# Contig name = gene name without final _number
genes["contig"] = genes["gene"].str.replace(r"_[0-9]+$", "", regex=True)

# Unknown genes
genes["is_unknown"] = genes["marker"].isna() | (genes["marker"].astype(str) == "NA")

# Viral genes: viral hallmark OR geNomad viral marker
genes["is_viral"] = (
    (genes["virus_hallmark"] == 1) |
    genes["marker"].astype(str).str.contains(r"\.V", regex=True, na=False)
)

# Host/cellular genes: universal single-copy genes OR chromosome-like markers
genes["is_host"] = (
    (genes["uscg"] == 1) |
    genes["marker"].astype(str).str.contains(r"\.C", regex=True, na=False)
)

summary = genes.groupby("contig").agg(
    n_genes=("gene", "count"),
    viral_genes=("is_viral", "sum"),
    host_genes=("is_host", "sum"),
    unknown_genes=("is_unknown", "sum")
).reset_index()

summary["unknown_fraction"] = summary["unknown_genes"] / summary["n_genes"]

# Add contig length from FASTA
lengths = {}
for record in SeqIO.parse("${VIRAL_FNA}", "fasta"):
    lengths[record.id] = len(record.seq)

summary["length"] = summary["contig"].map(lengths)

# Keep contigs satisfying:
# i) viral genes > 0
# ii) viral genes = 0 and host genes = 0
# iii) unknown genes >= 75%
# and always length >= 1000 bp
summary["keep"] = (
    (
        (summary["viral_genes"] > 0) |
        ((summary["viral_genes"] == 0) & (summary["host_genes"] == 0)) |
        (summary["unknown_fraction"] >= 0.75)
    )
    & (summary["length"] >= 1000)
)

summary.to_csv("${OUT}/geNomad_gene_filter_summary.tsv", sep="\t", index=False)

summary.loc[summary["keep"], "contig"].to_csv(
    "${OUT}/kept_contig_ids.txt",
    sep="\t",
    index=False,
    header=False
)

print(summary["keep"].value_counts())
print("Kept contigs:", int(summary["keep"].sum()))
print("Total contigs:", len(summary))
print("Contigs without length:", int(summary["length"].isna().sum()))
EOF

# =====================================================
# 3. Extract retained viral contigs
# =====================================================

seqkit grep \
    -f "${OUT}/kept_contig_ids.txt" \
    "${VIRAL_FNA}" \
    > "${OUT}/viral_contigs.filtered_before_checkv.fna"

echo "Number of filtered contigs before CheckV:"
grep -c "^>" "${OUT}/viral_contigs.filtered_before_checkv.fna"

# =====================================================
# 4. Switch to CheckV environment
# =====================================================

conda deactivate
conda activate checkv

which checkv

# =====================================================
# 5. Trim host regions and assess quality with CheckV
# =====================================================

checkv end_to_end \
    "${OUT}/viral_contigs.filtered_before_checkv.fna" \
    "${OUT}/checkv" \
    -d "${CHECKV_DB}" \
    -t "${THREADS}"

echo "CheckV finished."

echo "Main CheckV output:"
echo "${OUT}/checkv/viruses.fna"

echo "Number of contigs after CheckV host trimming:"
grep -c "^>" "${OUT}/checkv/viruses.fna" || true

echo "Done."