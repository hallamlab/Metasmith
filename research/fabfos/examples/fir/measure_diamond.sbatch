#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=cv_dmnd
#SBATCH --array=0-1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=11:50:00
#SBATCH --output=/project/rpp-shallam/phyberos/cyanoverse/gpr/audit/logs/dmnd_%A_%a.out
# THE MEASUREMENT THE SHARD SIZE DEPENDS ON.
#
# `diamond_uniref50.py` declares 24 h and runs `--sensitive` at 8 threads with
# --block-size (mem-4)/6 = 10. Nothing in this tree has ever timed that shape:
# the one prior GPR run recorded the database BUILD (152 s) and no lane
# wall-clock at all. The campaign guardrail is that no step may declare more
# than 3 h -- slurm.nf doubles the ask on retry and a 6 h retry never clears
# fir's 08:00 ALL_NODES window -- so if 100,000 queries take longer than 3 h,
# the shard size is wrong, not the guardrail.
#
# Task 0 runs a 10,000-query subsample for an answer in minutes; task 1 runs the
# real 100,000-query shard. Both use the transform's own flags verbatim, because
# a measurement of some other invocation settles nothing.
set -euo pipefail
module load apptainer

GPR=/project/rpp-shallam/phyberos/cyanoverse/gpr
DB=/scratch/phyberos/fabfos_refs/processed/uniref50_dmnd/uniref50.dmnd
SIF=/scratch/phyberos/cache/apptainer/docker..quay.io_biocontainers_diamond..2.1.8--h43eeafb_0.sif
WORK=${SLURM_TMPDIR:-$GPR/work}/dmnd_$SLURM_ARRAY_TASK_ID
mkdir -p "$WORK"

if [ "$SLURM_ARRAY_TASK_ID" -eq 0 ]; then
    N=10000
    awk -v n=$N '/^>/{c++} c>n{exit} {print}' "$GPR/shards/shard_0000.faa" > "$WORK/q.faa"
else
    N=100000
    cp "$GPR/shards/shard_0000.faa" "$WORK/q.faa"
fi
echo "queries: $(grep -c '^>' "$WORK/q.faa") (target $N)"

START=$(date +%s)
apptainer exec -B /project -B /scratch -B "$WORK" "$SIF" \
    diamond blastp \
        --query "$WORK/q.faa" \
        --db "$DB" \
        --out "$WORK/hits.tsv" \
        --threads 8 \
        --block-size 10 \
        --outfmt 6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore stitle \
        --max-target-seqs 1 \
        --evalue 1e-5 \
        --sensitive
END=$(date +%s)

echo "RESULT queries=$N seconds=$((END-START)) hits=$(wc -l < "$WORK/hits.tsv")"
cp "$WORK/hits.tsv" "$GPR/audit/dmnd_${N}_hits.tsv"
