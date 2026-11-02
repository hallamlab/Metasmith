#!/bin/bash
#SBATCH --account=rpp-shallam
#SBATCH --job-name=e1_close_bam
#SBATCH --time=01:30:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=16
#SBATCH --exclude=fc30557,fc30564
#SBATCH --output=/scratch/phyberos/bench/e1_close/logs/bam.%A_%a.log
# nf-core/mag 5.5.0 BOWTIE2_ASSEMBLY_BUILD + BOWTIE2_ASSEMBLY_ALIGN, transcribed in library/transforms/e2/bowtie2_binning_bam.py.
set -euo pipefail
W=/scratch/phyberos/bench/e1_close
SIF=/scratch/phyberos/cache/apptainer/docker..quay.io_biocontainers_mulled-v2-ac74a7f02cebcfcc07d8e8d1d750af9c83b4d45a..577a697be67b5ae9b16f637fd723b8263a3898b3-0.sif
IFS=$'\t' read -r idx arm sample dataset asm_id asm bam reads truth < <(awk -F'\t' -v i=$SLURM_ARRAY_TASK_ID '$1==i' $W/sheet.tsv)
[ "$arm" = short ] || { echo "task $SLURM_ARRAY_TASK_ID is $arm"; exit 1; }
echo "START $(date -Is) $sample on $(hostname)"
mkdir -p $W/bam $W/logs/bowtie2
module load apptainer
cd $SLURM_TMPDIR
C=$SLURM_CPUS_PER_TASK
apptainer exec --no-home --cleanenv -B /scratch/phyberos -B $SLURM_TMPDIR --pwd $SLURM_TMPDIR $SIF bash -c "
    set -euo pipefail
    bowtie2-build --threads $C $asm bt2_index_base > build.log
    bowtie2 -p $C -x bt2_index_base --interleaved $reads 2> bowtie2.log \
        | samtools view -@ $C -bS \
        | samtools sort -@ $C -T $SLURM_TMPDIR/sort -o $SLURM_TMPDIR/out.bam
"
cp bowtie2.log $W/logs/bowtie2/$sample.log
cat bowtie2.log
cp out.bam $bam.part
mv $bam.part $bam
ls -l $bam
echo "EXIT 0 $(date -Is)"
