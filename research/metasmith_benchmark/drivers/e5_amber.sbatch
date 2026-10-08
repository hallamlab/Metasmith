#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=e5_amber
#SBATCH --time=02:00:00
#SBATCH --mem=48G
#SBATCH --cpus-per-task=8
#SBATCH --output=/scratch/phyberos/bench/e5_amber/logs/amber.%A_%a.log
# AMBER on one E5 sample's DAS Tool bins: sbatch --array=<sheet idx list> e5_amber.sbatch <checkout> <sheet.tsv>
set -euo pipefail
CK=$1
SHEET=$2
W=/scratch/phyberos/bench/e5_amber
IFS=$'\t' read -r idx run study sample asm bam das reads truth < <(awk -F'\t' -v i=$SLURM_ARRAY_TASK_ID '$1==i' $SHEET)
echo "START $(date -Is) $sample ($run, $reads reads) on $(hostname)"
[ -n "$das" ] || { echo "no DAS Tool table: nothing to score"; exit 0; }
module load apptainer
if ! python3 $CK/research/cami/score_reference_amber.py \
    --sample-id $sample --assembly $asm --bam $bam --reads-mapping $truth \
    --contig-to-bin DASTool=$das \
    --threads $SLURM_CPUS_PER_TASK --lib $CK/research/metasmith_benchmark/library/resources/e2/cami_gold_standard.py \
    --outdir $W/amber/$sample; then
    # CAUTION AMBER 2.0.7 saves its metrics, then dies plotting a one-bin sample (KeyError precision_avg_bp_var).
    O=$W/amber/$sample/work/amber_out.DASTool
    grep -q "Saving computed metrics" $O/log.txt
    [ -s $O/results.tsv ] && [ -s $O/genome/DASTool/metrics_per_bin.tsv ]
    mkdir -p $W/amber/$sample/DASTool
    cp $O/results.tsv $O/genome/DASTool/metrics_per_bin.tsv $W/amber/$sample/DASTool/
    echo "kept AMBER's saved metrics: its plots failed on $(tail -n +2 $O/genome/DASTool/metrics_per_bin.tsv | wc -l) bin(s)"
fi
rm -rf $W/amber/$sample/work
echo "EXIT 0 $(date -Is)"
