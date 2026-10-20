#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=clean_cpu
#SBATCH --cpus-per-task=48
#SBATCH --mem=96G
#SBATCH --time=03:00:00
#SBATCH --array=1-10
#SBATCH --output=/scratch/phyberos/engcyc_pipeline/bench/clean_cpu/logs/%A_%a.log
set -euo pipefail
B=/scratch/phyberos/engcyc_pipeline/bench/clean_cpu
IMG=/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_external_clean..2026.06.14.sif
declare -A INPUT=([mg]=/project/6004975/phyberos/cyanoverse/data/orfs_and_metabuli/sequences-open_reading_frames/SRR27911466.faa
                  [dh]=/scratch/phyberos/bench/e4_mm/ecoli/proteomes/dh10b.faa)
read -r name input n mode procs threads < <(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" $B/configs.tsv)
module load apptainer
lscpu | grep -E "Model name|avx512_bf16" -o | sort -u
export OMP_NUM_THREADS=$threads MKL_NUM_THREADS=$threads
mkdir -p $B/out
run() { apptainer exec --cleanenv --env OMP_NUM_THREADS=$threads,MKL_NUM_THREADS=$threads \
        -B $B,/project/6004975/phyberos/cyanoverse,/scratch/phyberos/bench,$SLURM_TMPDIR $IMG "$@"; }
run python $B/embed.py --fasta ${INPUT[$input]} --n $n --mode $mode --procs $procs --threads $threads --out $B/out/$name
run python $B/calls.py --prefix $B/out/$name --work $SLURM_TMPDIR/clean_ws --threads 48
