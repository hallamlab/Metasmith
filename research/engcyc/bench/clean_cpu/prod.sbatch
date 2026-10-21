#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=clean_prod
#SBATCH --time=03:00:00
#SBATCH --output=/scratch/phyberos/engcyc_pipeline/bench/clean_cpu/logs/%x_%j.log
# A production-sized run on a shared by-core node, then CLEAN's stock and in-memory maxsep on its output.
# Usage: sbatch --cpus-per-task=C --mem=M prod.sbatch <name> <mode> <procs> <threads> <n> <pin 0|1> [toks] [slim 0|1]
# slim=1 loads a copy of the checkpoint without its optimizer state, written to node-local disk first.
set -euo pipefail
B=/scratch/phyberos/engcyc_pipeline/bench/clean_cpu
IMG=/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_external_clean..2026.06.14.sif
MG=/project/6004975/phyberos/cyanoverse/data/orfs_and_metabuli/sequences-open_reading_frames/SRR27911466.faa
name=$1 mode=$2 procs=$3 threads=$4 n=$5 pin=$6 toks=${7:-4096} slim=${8:-0}
module load apptainer
hostname; nproc; taskset -cp $$
envs="OMP_NUM_THREADS=$threads,MKL_NUM_THREADS=$threads"
extra=""
if [ "$pin" = 1 ]; then envs="$envs,OMP_PROC_BIND=close,OMP_PLACES=cores"; extra="--pin"; fi
mkdir -p $B/out/prod
if [ "$slim" = 1 ]; then
  apptainer exec --cleanenv -B $B,$SLURM_TMPDIR $IMG python $B/slim_ckpt.py $SLURM_TMPDIR/esm
  extra="$extra --model $SLURM_TMPDIR/esm/esm1b_t33_650M_UR50S.pt"
fi
/usr/bin/time -v apptainer exec --cleanenv --env "$envs" -B $B,/project/6004975/phyberos/cyanoverse,$SLURM_TMPDIR $IMG \
  python $B/embed.py --fasta $MG --n $n --mode $mode --procs $procs --threads $threads --toks $toks $extra --out $B/out/prod/$name \
  2> >(grep -E "Maximum resident|Elapsed|Traceback|Error" >&2)
/usr/bin/time -v apptainer exec --cleanenv -B $B,$SLURM_TMPDIR $IMG python $B/calls.py --prefix $B/out/prod/$name \
  --work $SLURM_TMPDIR/ws --threads $SLURM_CPUS_PER_TASK 2> >(grep -E "Maximum resident|Elapsed|Traceback|Error" >&2)
