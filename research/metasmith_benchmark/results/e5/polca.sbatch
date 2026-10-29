#!/bin/bash
#SBATCH --job-name=r3_polca
#SBATCH --account=def-shallam
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=12:00:00
#SBATCH --array=1-17
#SBATCH --output=/scratch/phyberos/e5_tax/polish/logs/polca.%A_%a.out
set -euo pipefail
unset -f which 2>/dev/null || true
module load apptainer
D=/scratch/phyberos/e5_tax/polish
S=$(sed -n "${SLURM_ARRAY_TASK_ID}p" $D/runs.txt)
W=$D/work/$S; mkdir -p $W $D/polished; cd $W
cp /scratch/phyberos/e5_tax/e3_r3_opera/$S.fna asm.fa
export TMPDIR=$W
apptainer exec --cleanenv -B /scratch /scratch/phyberos/cache/apptainer/docker..quay.io_biocontainers_masurca..4.1.4--ha5bb246_1.sif \
    polca.sh -a asm.fa -r $D/reads/$S.fq.gz -t 16 -m 1G
cat asm.fa.report
mv asm.fa.PolcaCorrected.fa $D/polished/$S.fna
rm -f asm.fa.unSorted.sam asm.fa.alignSorted.bam* asm.fa.bwa.* 
