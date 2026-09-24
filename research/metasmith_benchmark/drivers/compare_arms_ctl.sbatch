#!/bin/bash
#SBATCH --account=rpp-shallam
#SBATCH --job-name=compare_arms_ctl
#SBATCH --time=01:30:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=8
#SBATCH --exclude=fc30557,fc30564
#SBATCH --output=/scratch/phyberos/bench/compare_arms_ctl/logs/%x.%A_%a.log
# E1 vs the E1ctl control, through compare_arms_sample.py with E1ctl in the E2 slot.
#   prep, once, after both E1ctl heads finish:  sbatch compare_arms_ctl.sbatch <checkout> prep
#   one sample per e1_close sheet idx:          sbatch --array=<idx list> compare_arms_ctl.sbatch <checkout>
#   gather:                                     compare_arms_gather.sh /scratch/phyberos/bench/compare_arms_ctl
set -euo pipefail
CHECKOUT=$1
MODE=${2:-sample}
D=$CHECKOUT/research/metasmith_benchmark/drivers
W=/scratch/phyberos/bench/e1_close
C=/scratch/phyberos/bench/e1ctl
OUT=/scratch/phyberos/bench/compare_arms_ctl
echo "START $(date -Is) $MODE ${SLURM_ARRAY_TASK_ID:-} on $(hostname), checkout $(cat $CHECKOUT/COMMIT)"

if [ "$MODE" = prep ]; then
    # nf-core names an --assembly_input long assembly Flye-<sample>, where E1 has FLYE-<sample>.
    samples=$(tail -q -n +2 $D/e1_nfcore/samplesheet.short.ctl.csv $D/e1_nfcore/samplesheet.long.ctl.csv | cut -d, -f1)
    awk -F'\t' -v OFS='\t' -v keep="$(echo $samples)" '
        BEGIN { n = split(keep, k, " "); for (i = 1; i <= n; i++) want[k[i]] = 1 }
        NR == 1 { print; next }
        $3 in want { if ($2 == "long") sub(/^FLYE-/, "Flye-", $5); print }' $W/sheet.tsv > $OUT/sheet.tsv
    [ "$(tail -n +2 $OUT/sheet.tsv | wc -l)" -eq 25 ] || { echo "ABORT: sheet has $(tail -n +2 $OUT/sheet.tsv | wc -l) rows"; exit 1; }
    python3 $D/e1_close.py split --sheet $OUT/sheet.tsv --outdir $OUT/maps \
        --maps $C/short/out/GenomeBinning/contig_to_bin/contig_to_bin_map.tsv $C/long/out/GenomeBinning/contig_to_bin/contig_to_bin_map.tsv
    cat $OUT/maps/bin_counts.tsv
    python3 $D/checkm2_tables.py e1 --sheet $OUT/sheet.tsv --out $OUT/e1ctl_checkm2.tsv.gz \
        --nfcore $C/short/out/GenomeBinning/QC/checkm2_summary.tsv $C/long/out/GenomeBinning/QC/checkm2_summary.tsv
    tail -n +2 $OUT/sheet.tsv | cut -f1 | paste -sd, > $OUT/array_idx.txt
    echo "array: $(cat $OUT/array_idx.txt)"
    echo "EXIT 0 $(date -Is)"
    exit 0
fi

module load apptainer
python3 $D/compare_arms_sample.py --sheet $W/sheet.tsv --maps $W/maps \
    --side2-sheet $OUT/sheet.tsv --side2-maps $OUT/maps \
    --skani-sif /scratch/phyberos/cache/apptainer/docker..staphb_skani..0.2.2.sif \
    --minimap2-sif /scratch/phyberos/cache/apptainer/docker..community.wave.seqera.io_library_minimap2_samtools..33bb43c18d22e29c.sif \
    --index $SLURM_ARRAY_TASK_ID --threads $SLURM_CPUS_PER_TASK \
    --tmp $SLURM_TMPDIR --outdir $OUT
echo "EXIT 0 $(date -Is)"
