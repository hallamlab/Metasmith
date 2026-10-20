#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=clean_scale
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=192
#SBATCH --mem=740G
#SBATCH --exclusive
#SBATCH --time=03:00:00
#SBATCH --output=/scratch/phyberos/engcyc_pipeline/bench/clean_cpu/logs/%x_%j.log
# Usage: sbatch scale.sbatch <plan.tsv>. Each plan row is one embed run on this otherwise idle node:
# name mode procs threads toks n pin(0|1) [stack flags env]. Runs use cpus 0..procs*threads-1.
# stack: stock (the image's torch 2.0), t25 (torch 2.5.1 CPU wheel), ipex (t25 + IPEX 2.5.0), installed
# per job onto node-local $SLURM_TMPDIR so the overlay costs no scratch inodes. flags: extra embed.py
# flags joined by commas, or - (the token "save" keeps the embeddings; "slim" loads slim_ckpt.py's copy). env: extra NAME=VALUE pairs joined by commas, or -.
set -euo pipefail
B=/scratch/phyberos/engcyc_pipeline/bench/clean_cpu
IMG=/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_external_clean..2026.06.14.sif
MG=/project/6004975/phyberos/cyanoverse/data/orfs_and_metabuli/sequences-open_reading_frames/SRR27911466.faa
PLAN=$1
module load apptainer
hostname; nproc
lscpu | grep -E "^Model name|^NUMA|^Socket|^Core|^L3"
lscpu -p=CPU,CORE,SOCKET,NODE,CACHE | grep -v '^#' | awk -F, '{print $5}' | sort | uniq -c | head -3
grep -o -w -E "avx512_bf16|avx512_vnni|amx_bf16" /proc/cpuinfo | sort | uniq -c
mkdir -p $B/out/scale
W=$B/wheels
if cut -f8 $B/$PLAN | grep -q -E "t25|ipex"; then
  apptainer exec --cleanenv -B $B,$SLURM_TMPDIR $IMG pip install -q --no-deps --target $SLURM_TMPDIR/t25 \
    $W/torch-2.5.1+cpu-cp310-cp310-linux_x86_64.whl $W/fsspec-2024.10.0-py3-none-any.whl
  apptainer exec --cleanenv -B $B,$SLURM_TMPDIR $IMG pip install -q --no-deps --target $SLURM_TMPDIR/ipex \
    $W/intel_extension_for_pytorch-2.5.0-cp310-cp310-manylinux2014_x86_64.whl $W/psutil-6.1.0-*.whl
fi
if cut -f9 $B/$PLAN | grep -q slim; then
  apptainer exec --cleanenv -B $B,$SLURM_TMPDIR $IMG python $B/slim_ckpt.py $SLURM_TMPDIR/esm
fi
tail -n +2 $B/$PLAN | while read -r name mode procs threads toks n pin stack flags xenv; do
  cores=$((procs * threads))
  envs="OMP_NUM_THREADS=$threads,MKL_NUM_THREADS=$threads"
  extra=""
  if [ "$pin" = 1 ]; then envs="$envs,OMP_PROC_BIND=close,OMP_PLACES=cores"; extra="--pin"; fi
  case "${stack:-stock}" in
    t25)  envs="$envs,PYTHONPATH=$SLURM_TMPDIR/t25" ;;
    ipex) envs="$envs,PYTHONPATH=$SLURM_TMPDIR/ipex:$SLURM_TMPDIR/t25"; extra="$extra --ipex" ;;
  esac
  nosave=--no-save
  case ",${flags:-}," in *,save,*) nosave=""; flags=${flags//save/} ;; esac
  case ",${flags:-}," in *,slim,*) extra="$extra --model $SLURM_TMPDIR/esm/esm1b_t33_650M_UR50S.pt"; flags=${flags//slim/} ;; esac
  for f in ${flags//,/ }; do [ "$f" != - ] && extra="$extra $f"; done
  [ -n "${xenv:-}" ] && [ "$xenv" != - ] && envs="$envs,$xenv"
  echo "### $name $(date +%T)"
  apptainer exec --cleanenv --env "$envs" -B $B,/project/6004975/phyberos/cyanoverse,$SLURM_TMPDIR $IMG \
    python $B/embed.py --fasta $MG --n $n --mode $mode --procs $procs --threads $threads --toks $toks \
    --cpuset 0-$((cores - 1)) $extra $nosave --out $B/out/scale/$name || echo "### FAILED $name"
  if [ -z "$nosave" ] && [ -f $B/out/scale/$name.npy ]; then
    apptainer exec --cleanenv -B $B,$SLURM_TMPDIR $IMG python $B/calls.py --prefix $B/out/scale/$name \
      --work $SLURM_TMPDIR/ws_$name --threads 8 --skip-stock || echo "### FAILED calls $name"
  fi
done
echo "### done $(date +%T)"
