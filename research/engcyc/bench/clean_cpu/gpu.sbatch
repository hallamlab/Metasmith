#!/bin/bash
#SBATCH --account=def-shallam_gpu
#SBATCH --job-name=clean_gpu
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=03:00:00
#SBATCH --output=/scratch/phyberos/engcyc_pipeline/bench/clean_cpu/logs/%x_%j.log
# ESM-1b on one GPU with the image's own torch, on the same 20,000-ORF sample and DH10B set as the CPU
# runs. Usage: sbatch --gpus=<gres>:1 gpu.sbatch <tag>
set -euo pipefail
B=/scratch/phyberos/engcyc_pipeline/bench/clean_cpu
IMG=/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_external_clean..2026.06.14.sif
MG=/project/6004975/phyberos/cyanoverse/data/orfs_and_metabuli/sequences-open_reading_frames/SRR27911466.faa
TAG=$1
module load apptainer
hostname; nvidia-smi -L; echo "CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-unset}"
# A MIG handle must reach the container or CUDA sees no device; a whole card under cgroup isolation is
# renumbered from 0 inside, so its host index must not be forwarded.
envs="OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK,TORCH_HOME=/opt/torch_cache"
case "${CUDA_VISIBLE_DEVICES:-}" in MIG-*) envs="$envs,CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES" ;; esac
run() { apptainer exec --nv --cleanenv --env "$envs" -B $B,/project/6004975/phyberos/cyanoverse,$SLURM_TMPDIR $IMG "$@"; }
( while true; do date +%T; nvidia-smi --query-compute-apps=pid,used_memory --format=csv,noheader; sleep 30; done ) \
  > $B/logs/gpu_smi_${TAG}_$SLURM_JOB_ID.log 2>&1 &
SMI=$!
mkdir -p $B/out/gpu
for spec in "fp32 4096" "fp32 8192" "fp32 16384" "bf16 4096"; do
  set -- $spec
  echo "### ${TAG}_mg20k_$1_t$2 $(date +%T)"
  run python $B/embed.py --device cuda --fasta $MG --n 20000 --mode $1 --procs 1 --threads $SLURM_CPUS_PER_TASK \
    --toks $2 --no-save --out $B/out/gpu/${TAG}_mg20k_$1_t$2 || echo "### FAILED $spec"
done
echo "### ${TAG}_dh_fp32_t4096 $(date +%T)"
run python $B/embed.py --device cuda --fasta $B/inputs/dh10b.faa --mode fp32 --procs 1 --threads $SLURM_CPUS_PER_TASK \
  --toks 4096 --out $B/out/gpu/${TAG}_dh_fp32_t4096
run python $B/calls.py --prefix $B/out/gpu/${TAG}_dh_fp32_t4096 --work $SLURM_TMPDIR/ws_dh --threads 8 --skip-stock
# The transform's own route, end to end: extract.py (one .pt per ORF) then CLEAN's stock maxsep.
W=$SLURM_TMPDIR/clean_ws
mkdir -p $W/data/inputs $W/data/esm_data $W/results/inputs
ln -s /app/data/pretrained $W/data/pretrained
ln -s /app/data/split100.csv $W/data/split100.csv
ln -s /app/esm $W/esm
run python $B/sample_fasta.py $MG 20000 7 $W/data/inputs/stock20k.fasta
echo "### ${TAG}_stock20k $(date +%T)"
t0=$(date +%s.%N)
apptainer exec --nv --cleanenv --env "$envs" -B $B,$W:/clean_ws $IMG \
  bash -c "cd /clean_ws && python /app/CLEAN_infer_fasta.py --fasta_data stock20k" > $SLURM_TMPDIR/stock.out 2>&1 || echo "### FAILED stock"
t1=$(date +%s.%N)
grep -v "it/s" $SLURM_TMPDIR/stock.out | grep -i -E "transferred|warn|error|compatible" | head -5
echo "{\"name\": \"${TAG}_stock20k\", \"orfs\": 20000, \"wall_s\": $(echo "$t1 - $t0" | bc)}" | tee $B/out/gpu/${TAG}_stock20k.wall.json
kill $SMI
echo "### done $(date +%T)"
