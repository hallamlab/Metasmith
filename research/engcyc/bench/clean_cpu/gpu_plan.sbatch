#!/bin/bash
#SBATCH --account=def-shallam_gpu
#SBATCH --job-name=clean_gpuplan
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=03:00:00
#SBATCH --output=/scratch/phyberos/engcyc_pipeline/bench/clean_cpu/logs/%x_%j.log
# Plan-driven ESM-1b runs on one GPU. Usage: sbatch --gpus=<gres>:1 gpu_plan.sbatch <plan.tsv>
# Plan rows: name mode toks n input(mg|dh) save(0|1). A saved row also gets in-memory maxsep calls.
set -euo pipefail
B=/scratch/phyberos/engcyc_pipeline/bench/clean_cpu
IMG=/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_external_clean..2026.06.14.sif
MG=/project/6004975/phyberos/cyanoverse/data/orfs_and_metabuli/sequences-open_reading_frames/SRR27911466.faa
module load apptainer
hostname; nvidia-smi -L
envs="OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK"
case "${CUDA_VISIBLE_DEVICES:-}" in MIG-*) envs="$envs,CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES" ;; esac
mkdir -p $B/out/gpu
tail -n +2 $B/$1 | while read -r name mode toks n input save; do
  FA=$MG; [ "$input" = dh ] && FA=$B/inputs/dh10b.faa
  ns=--no-save; [ "$save" = 1 ] && ns=""
  echo "### $name $(date +%T)"
  apptainer exec --nv --cleanenv --env "$envs" -B $B,/project/6004975/phyberos/cyanoverse,$SLURM_TMPDIR $IMG \
    python $B/embed.py --device cuda --fasta $FA --n $n --mode $mode --procs 1 --threads $SLURM_CPUS_PER_TASK \
    --toks $toks $ns --out $B/out/gpu/$name || echo "### FAILED $name"
  if [ "$save" = 1 ]; then
    apptainer exec --cleanenv -B $B,$SLURM_TMPDIR $IMG python $B/calls.py --prefix $B/out/gpu/$name \
      --work $SLURM_TMPDIR/ws_$name --threads 8 --skip-stock || echo "### FAILED calls $name"
  fi
done
echo "### done $(date +%T)"
