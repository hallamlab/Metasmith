#!/bin/bash
# Usage: mp_analysis_one.sh SAMPLE THREADS   (inside a Slurm allocation)
set -euo pipefail
SAMPLE=$1 THREADS=$2
B=/scratch/phyberos/beth_mp
JUAN=/scratch/jcsm2010/metapathways_dbs
LIC=$B/images/licensed
SIF=$B/images/metapathways_4.0.3.sif
PT_SIF=$LIC/pathway-tools-27.0-3d267039644e-2fb1f8b8-3aa96f5584c1.sif
OUT=$B/mp4/runs/$SAMPLE
MEM_GB=$((SLURM_MEM_PER_NODE / 1024 - 16))
mkdir -p "$OUT"

echo "$(date -Is) $SAMPLE -> $OUT on $(hostname), ${SLURM_CPUS_PER_TASK} cpus, ${MEM_GB} GB, threads $THREADS, SLURM_TMPDIR=$SLURM_TMPDIR ($(df -h --output=avail $SLURM_TMPDIR | tail -1))"
export APPTAINERENV_METAPATHWAYS_FAST_SCRATCH=$SLURM_TMPDIR APPTAINERENV_TMPDIR=$SLURM_TMPDIR
export APPTAINERENV_NXF_OPTS="-Xms512m -Xmx8g" APPTAINERENV_NXF_HOME=$B/nxf_home APPTAINERENV_NXF_OFFLINE=true APPTAINER_TMPDIR=$SLURM_TMPDIR
export APPTAINERENV_APPTAINER_TMPDIR=$SLURM_TMPDIR
apptainer exec --cleanenv -B $B,$JUAN,$SLURM_TMPDIR,$LIC --pwd $B $SIF \
    metapathways analysis_wf --manifest $B/mp4/manifest.tsv -o "$OUT" -d $B/MPDB --samples "$SAMPLE" \
    --annotation_dbs swissprot uniref50 uniref90 cazy eggnog metacyc \
    --threads "$THREADS" --max_cpus $SLURM_CPUS_PER_TASK --memory "16 GB" --ptools_memory "8 GB" \
    --max_memory "${MEM_GB} GB" --executor local --fast_local_db --keep_work --image $PT_SIF
echo "$(date -Is) done $SAMPLE"
