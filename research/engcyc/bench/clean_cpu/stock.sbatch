#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=clean_stock
#SBATCH --cpus-per-task=48
#SBATCH --mem=64G
#SBATCH --time=03:00:00
#SBATCH --output=/scratch/phyberos/engcyc_pipeline/bench/clean_cpu/logs/%x_%j.log
# The transform's own path on a CPU node: CLEAN_infer_fasta.py (fair-esm extract.py, fp32, one process,
# OMP_NUM_THREADS=cpus as metasmith's apptainer launch sets it, one .pt per ORF), on the same 600-ORF sample.
set -euo pipefail
B=/scratch/phyberos/engcyc_pipeline/bench/clean_cpu
IMG=/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_external_clean..2026.06.14.sif
MG=/project/6004975/phyberos/cyanoverse/data/orfs_and_metabuli/sequences-open_reading_frames/SRR27911466.faa
module load apptainer
hostname
W=$SLURM_TMPDIR/clean_ws
mkdir -p $W/data/inputs $W/data/esm_data $W/results/inputs
ln -s /app/data/pretrained $W/data/pretrained
ln -s /app/data/split100.csv $W/data/split100.csv
ln -s /app/esm $W/esm
run() { apptainer exec --cleanenv --env OMP_NUM_THREADS=48,OPENBLAS_NUM_THREADS=48,TORCH_HOME=/opt/torch_cache \
        -B $B,/project/6004975/phyberos/cyanoverse,$W:/clean_ws $IMG "$@"; }
run python $B/sample_fasta.py $MG 600 7 /clean_ws/data/inputs/stock600.fasta
t0=$(date +%s.%N)
run bash -c "cd /clean_ws && python /app/CLEAN_infer_fasta.py --fasta_data stock600" > $SLURM_TMPDIR/stock.out 2>&1
t1=$(date +%s.%N)
grep -v "it/s" $SLURM_TMPDIR/stock.out | tail -5
cp $W/results/inputs/stock600_maxsep.csv $B/out/stock600_maxsep.csv
echo "{\"name\": \"stock600\", \"orfs\": 600, \"cores\": 48, \"wall_s\": $(echo "$t1 - $t0" | bc)}" | tee $B/out/stock600.wall.json
