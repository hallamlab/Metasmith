#!/bin/bash
#SBATCH --account=rpp-shallam
#SBATCH --job-name=e1_close_checkm2
#SBATCH --time=02:00:00
#SBATCH --mem=48G
#SBATCH --cpus-per-task=16
#SBATCH --exclude=fc30557,fc30564
#SBATCH --output=/scratch/phyberos/bench/e1_close/logs/checkm2.%A_%a.log
# nf-core/mag 5.5.0's CHECKM2_PREDICT on a long sample's b19 MetaBAT2 and three-binner DAS Tool sets.
# The b19 rerun wrote contig-to-bin tables but no CheckM2, and no DAS Tool bin FASTAs, so both sets
# are rebuilt from the b19 map and E1's own Flye assembly.
set -euo pipefail
W=/scratch/phyberos/bench/e1_close
SIF=/scratch/phyberos/cache/apptainer/docker..community.wave.seqera.io_library_checkm2..1.1.0--60f287bc25d7a10d.sif
DB=/scratch/phyberos/wave2_b3_nfcore/checkm2_db/CheckM2_database/uniref100.KO.1.dmnd
IFS=$'\t' read -r idx arm sample dataset asm_id asm bam reads truth < <(awk -F'\t' -v i=$SLURM_ARRAY_TASK_ID '$1==i' $W/sheet.tsv)
[ "$arm" = long ] || { echo "task $SLURM_ARRAY_TASK_ID is $arm"; exit 1; }
echo "START $(date -Is) $sample on $(hostname)"
mkdir -p $W/checkm2 $W/bins_long
module load apptainer
cd $SLURM_TMPDIR
for b in MetaBAT2 DASTool; do
    python3 $W/e1_close.py derive --sheet $W/sheet.tsv --maps $W/maps --sample $sample --binner $b --outdir bins_$b
    apptainer exec --no-home --cleanenv -B /scratch/phyberos -B $SLURM_TMPDIR --pwd $SLURM_TMPDIR $SIF \
        checkm2 predict --input bins_$b -x fa --output-directory checkm2_$b --threads $SLURM_CPUS_PER_TASK --database_path $DB
    rows=$(( $(wc -l < checkm2_$b/quality_report.tsv) - 1 ))
    [ "$rows" -eq "$(ls bins_$b | wc -l)" ] || { echo "$b: $rows CheckM2 rows for $(ls bins_$b | wc -l) bins"; exit 1; }
    cp checkm2_$b/quality_report.tsv $W/checkm2/$asm_id-$b-$sample.quality_report.tsv
done
tar -cf $W/bins_long/$sample.tar bins_MetaBAT2 bins_DASTool
echo "EXIT 0 $(date -Is)"
