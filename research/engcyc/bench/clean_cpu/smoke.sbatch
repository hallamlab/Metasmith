#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=clean_smoke
#SBATCH --cpus-per-task=8
#SBATCH --mem=48G
#SBATCH --time=01:00:00
#SBATCH --output=/scratch/phyberos/engcyc_pipeline/bench/clean_cpu/logs/%x_%j.log
# Check that each precision mode in quant.py loads and embeds 64 ORFs, before a whole-node round.
set -uo pipefail
B=/scratch/phyberos/engcyc_pipeline/bench/clean_cpu
IMG=/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_external_clean..2026.06.14.sif
MG=/project/6004975/phyberos/cyanoverse/data/orfs_and_metabuli/sequences-open_reading_frames/SRR27911466.faa
W=$B/wheels
module load apptainer
apptainer exec --cleanenv -B $B,$SLURM_TMPDIR $IMG pip install -q --no-deps --target $SLURM_TMPDIR/t25 \
  $W/torch-2.5.1+cpu-cp310-cp310-linux_x86_64.whl $W/fsspec-2024.10.0-py3-none-any.whl $W/torchao-0.7.0-*.whl
apptainer exec --cleanenv -B $B,$SLURM_TMPDIR $IMG python $B/slim_ckpt.py $SLURM_TMPDIR/esm
mkdir -p $SLURM_TMPDIR/out
for spec in "$@"; do
  stack=${spec%%:*} mode=${spec#*:}
  envs="OMP_NUM_THREADS=8"
  [ "$stack" = t25 ] && envs="$envs,PYTHONPATH=$SLURM_TMPDIR/t25"
  echo "### $stack $mode"
  apptainer exec --cleanenv --env "$envs" -B $B,/project/6004975/phyberos/cyanoverse,$SLURM_TMPDIR $IMG \
    python $B/embed.py --fasta $MG --n 64 --mode $mode --procs 1 --threads 8 --toks 1024 \
    --model $SLURM_TMPDIR/esm/esm1b_t33_650M_UR50S.pt --out $SLURM_TMPDIR/out/${stack}_$mode 2>&1 | grep -v -i warn | grep -E "Error|error|orf_per_s|raise|line [0-9]+, in" | tail -8
done
