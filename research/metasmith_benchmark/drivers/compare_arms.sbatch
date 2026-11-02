#!/bin/bash
#SBATCH --account=rpp-shallam
#SBATCH --job-name=compare_arms
#SBATCH --time=01:30:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=8
#SBATCH --exclude=fc30557,fc30564
#SBATCH --output=/scratch/phyberos/bench/compare_arms/logs/sample.%A_%a.log
# One sample's raw E1-vs-E2 comparison inputs, through compare_arms_sample.py. Array index = sheet idx.
set -euo pipefail
W=/scratch/phyberos/bench/e1_close
echo "START $(date -Is) task $SLURM_ARRAY_TASK_ID on $(hostname)"
module load apptainer
python3 $W/compare_arms_sample.py --sheet $W/sheet.tsv --maps $W/maps \
    --manifest /scratch/phyberos/bench/archive/e2/archive_manifest.tsv \
    --cache /scratch/phyberos/cami/metasmith/task_cache \
    --skani-sif /scratch/phyberos/cache/apptainer/docker..staphb_skani..0.2.2.sif \
    --minimap2-sif /scratch/phyberos/cache/apptainer/docker..community.wave.seqera.io_library_minimap2_samtools..33bb43c18d22e29c.sif \
    --index $SLURM_ARRAY_TASK_ID --threads $SLURM_CPUS_PER_TASK \
    --tmp $SLURM_TMPDIR --outdir /scratch/phyberos/bench/compare_arms
echo "EXIT 0 $(date -Is)"
