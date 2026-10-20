#!/bin/bash
#SBATCH -A rrg-shallam-ab
#SBATCH -c 8
#SBATCH --mem=32G
#SBATCH --time=2:00:00
#SBATCH -o /scratch/phyberos/mp4/logs/build_mpdb.%x.%j.log
# Index a prefetched MPDB (fetch_mpdb.sh) offline.
# usage: sbatch -J <db> build_mpdb.sbatch <mpdb_root> <db>...
set -euo pipefail
root=$1; shift
module load apptainer/1.4.5
export APPTAINER_TMPDIR=$SLURM_TMPDIR NXF_HOME=$SLURM_TMPDIR/nxf
/usr/bin/time -v apptainer exec --cleanenv --bind /scratch/phyberos:/scratch/phyberos,$SLURM_TMPDIR:$SLURM_TMPDIR \
    --env NXF_HOME=$NXF_HOME \
    /scratch/phyberos/mp4/metapathways_4.0.1.sif \
    metapathways build_db -d "$root" --func "$@" -a fast \
        -t $SLURM_CPUS_PER_TASK --max_cpus $SLURM_CPUS_PER_TASK --memory "${MPDB_TASK_MEM:-16 GB}" \
        --max_memory "$((SLURM_MEM_PER_NODE / 1024)) GB" --work_dir $SLURM_TMPDIR/work
du -sh "$root"/functional/formatted
ls -la "$root"/functional/formatted "$root"/functional_categories
