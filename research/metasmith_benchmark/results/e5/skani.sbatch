#!/bin/bash
#SBATCH --job-name=r3_skani
#SBATCH --account=def-shallam
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --array=1-17
#SBATCH --output=/scratch/phyberos/e5_tax/polish/logs/skani.%A_%a.out
set -euo pipefail
module load apptainer
D=/scratch/phyberos/e5_tax/polish
S=$(sed -n "${SLURM_ARRAY_TASK_ID}p" $D/runs.txt)
mkdir -p $D/skani
IMG=/scratch/phyberos/cache/apptainer/docker..staphb_skani..0.2.2.sif
for lane in unpolished:/scratch/phyberos/e5_tax/e3_r3_opera/$S.fna polished:$D/polished/$S.fna; do
    name=${lane%%:*}; ref=${lane#*:}
    apptainer exec --cleanenv -B /scratch $IMG skani dist --qi -q $D/pub/$S.fna --ri -r $ref \
        --small-genomes --min-af 15 -o $D/skani/$S.$name.tsv -t 8
done
