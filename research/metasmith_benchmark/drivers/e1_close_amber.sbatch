#!/bin/bash
#SBATCH --account=rpp-shallam
#SBATCH --job-name=e1_close_amber
#SBATCH --time=02:00:00
#SBATCH --mem=48G
#SBATCH --cpus-per-task=8
#SBATCH --exclude=fc30557,fc30564
#SBATCH --output=/scratch/phyberos/bench/e1_close/logs/amber.%A_%a.log
# AMBER on one E1 sample, all four binners, through research/cami/score_reference_amber.py.
set -euo pipefail
W=/scratch/phyberos/bench/e1_close
IFS=$'\t' read -r idx arm sample dataset asm_id asm bam reads truth < <(awk -F'\t' -v i=$SLURM_ARRAY_TASK_ID '$1==i' $W/sheet.tsv)
echo "START $(date -Is) $sample on $(hostname)"
module load apptainer
python3 $W/score_reference_amber.py \
    --sample-id $sample --assembly $asm --bam $bam --reads-mapping $truth \
    --nfcore-contig-to-bin $W/maps/$sample.tsv \
    --threads $SLURM_CPUS_PER_TASK --lib $W/cami_gold_standard.py \
    --outdir $W/amber/$sample
rm -rf $W/amber/$sample/work
echo "EXIT 0 $(date -Is)"
