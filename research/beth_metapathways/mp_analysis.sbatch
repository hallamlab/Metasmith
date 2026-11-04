#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=beth_mp4
#SBATCH --nodes=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=160G
#SBATCH --time=48:00:00
#SBATCH --output=/scratch/phyberos/beth_mp/logs/mp_analysis_%j.log
# Usage: sbatch mp_analysis.sbatch SAMPLE [SAMPLE ...]
set -euo pipefail
B=/scratch/phyberos/beth_mp
JUAN=/scratch/jcsm2010/metapathways_dbs
LIC=$B/images/licensed
SIF=$B/images/metapathways_4.0.3.sif
PT_SIF=$LIC/pathway-tools-27.0-3d267039644e-2fb1f8b8-3aa96f5584c1.sif
MEM_GB=$((SLURM_MEM_PER_NODE / 1024 - 16))

echo "$(date -Is) $* on $(hostname), ${SLURM_CPUS_PER_TASK} cpus, ${MEM_GB} GB, SLURM_TMPDIR=$SLURM_TMPDIR ($(df -h --output=avail $SLURM_TMPDIR | tail -1))"
export APPTAINERENV_METAPATHWAYS_FAST_SCRATCH=$SLURM_TMPDIR APPTAINERENV_TMPDIR=$SLURM_TMPDIR
export APPTAINERENV_NXF_OPTS="-Xms512m -Xmx8g" APPTAINERENV_NXF_HOME=$B/nxf_home APPTAINERENV_NXF_OFFLINE=true APPTAINER_TMPDIR=$SLURM_TMPDIR
export APPTAINERENV_APPTAINER_TMPDIR=$SLURM_TMPDIR
apptainer exec --cleanenv -B $B,$JUAN,$SLURM_TMPDIR,$LIC --pwd $B $SIF \
    metapathways analysis_wf --manifest $B/mp4/manifest.tsv -o $B/mp4/out -d $B/MPDB --samples "$@" \
    --annotation_dbs swissprot uniref50 uniref90 cazy eggnog metacyc \
    --threads 16 --max_cpus $SLURM_CPUS_PER_TASK --memory "16 GB" --ptools_memory "8 GB" \
    --max_memory "${MEM_GB} GB" --executor local --fast_local_db --keep_work --image $PT_SIF
echo "$(date -Is) done"
