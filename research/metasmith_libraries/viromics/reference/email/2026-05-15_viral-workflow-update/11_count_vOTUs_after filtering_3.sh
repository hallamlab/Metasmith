#!/bin/bash
#SBATCH --job-name=11_count_final_votus
#SBATCH --output=11_count_final_votus_%j.out
#SBATCH --error=11_count_final_votus_%j.err
#SBATCH --partition=albaicin-big
#SBATCH --time=01:00:00

set -euo pipefail

source /LUSTRE/home/RNM270/ach/miniforge3/etc/profile.d/conda.sh
conda activate genomad   # aquí tienes seqkit instalado

BEFORE="/SCRATCH/RNM270/ach/Viromics/08_vOTUs/vOTU_representatives.fna"
AFTER="/SCRATCH/RNM270/ach/Viromics/09_vOTUs_filter3_large_mobile/vOTUs.filter3.no_large_mobile.fasta"
REMOVED="/SCRATCH/RNM270/ach/Viromics/09_vOTUs_filter3_large_mobile/removed_large_mobile_vOTUs.txt"
OUT="/SCRATCH/RNM270/ach/Viromics/09_vOTUs_filter3_large_mobile/final_vOTU_summary.txt"

which seqkit

count_fasta () {
    FASTA="$1"

    seqkit fx2tab -n -l "${FASTA}" | awk '
    {
        total++
        if($2 >= 1000) kb1++
        if($2 >= 5000) kb5++
        if($2 >= 10000) kb10++
    }
    END{
        print "Total vOTUs        :", total+0
        print "vOTUs >=1 kb       :", kb1+0
        print "vOTUs >=5 kb       :", kb5+0
        print "vOTUs >=10 kb      :", kb10+0
    }'
}

{
echo "=============================================="
echo "vOTU conservative curation summary"
echo "=============================================="
echo ""

echo "Removed large/mobile-artifact vOTUs:"
wc -l "${REMOVED}"

echo ""
echo "-----------------------------------"
echo "Counts BEFORE filtering"
echo "-----------------------------------"
count_fasta "${BEFORE}"

echo ""
echo "-----------------------------------"
echo "Counts AFTER filtering"
echo "-----------------------------------"
count_fasta "${AFTER}"

echo ""
echo "=============================================="
echo "Done."
echo "=============================================="

} | tee "${OUT}"