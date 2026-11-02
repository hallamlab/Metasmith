#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=verifyprod
#SBATCH --cpus-per-task=4
#SBATCH --mem=4G
#SBATCH --time=2:00:00
#SBATCH --array=0-33
#SBATCH --output=/scratch/phyberos/gmcf3495/truth/verify-%A_%a.out

# Compare each step-1 interleaved product against the independent ground truth
# in truth/<sample>.truth (see truth_counts.sh for why bbmap's own reported
# counts do not qualify as evidence).
#
# Expects product_map.tsv (sample \t absolute product path), built by
# build_product_map.sh from the nextflow work dirs -- each step-1 task dir names
# its source library in .command.sh and holds exactly one .fq.gz product.

set -uo pipefail

TRUTH=/scratch/phyberos/gmcf3495/truth
MAP="$TRUTH/product_map.tsv"

mapfile -t ROWS < "$MAP"
row="${ROWS[$SLURM_ARRAY_TASK_ID]:-}"
[ -n "$row" ] || { echo "no row at index $SLURM_ARRAY_TASK_ID"; exit 1; }

sample=$(cut -f1 <<<"$row")
product=$(cut -f2 <<<"$row")

echo "sample=$sample product=$product start=$(date -u +%FT%TZ)"

[ -f "$product" ] || { echo "VERDICT $sample MISSING_PRODUCT $product"; exit 1; }
[ -f "$TRUTH/$sample.truth" ] || { echo "VERDICT $sample NO_TRUTH"; exit 1; }

expected=$(cut -f4 "$TRUTH/$sample.truth")

lines=$(pigz -p 4 -dc "$product" | wc -l)
rc=${PIPESTATUS[0]}
if [ "$rc" -ne 0 ]; then
    echo "VERDICT $sample PIGZ_FAIL rc=$rc"
    exit 1
fi
if [ $(( lines % 4 )) -ne 0 ]; then
    echo "VERDICT $sample TRUNCATED lines=$lines"
    exit 1
fi
actual=$(( lines / 4 ))

if [ "$actual" -eq "$expected" ]; then
    verdict=OK
else
    verdict=MISMATCH
fi

printf '%s\t%s\t%s\t%s\n' "$sample" "$expected" "$actual" "$verdict" > "$TRUTH/$sample.verify"
echo "VERDICT $sample expected=$expected actual=$actual $verdict end=$(date -u +%FT%TZ)"
[ "$verdict" = OK ]
