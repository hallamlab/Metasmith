#!/bin/bash
#SBATCH --account=rpp-shallam
#SBATCH --job-name=e2_pack
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=1:00:00
#SBATCH --output=/scratch/phyberos/bench/archive/e2/logs/pack.%A_%a.log
# Packs E2 samples out of the cami task cache. Reads the cache; writes only under archive/e2.
# Array index = position in the manifest's sorted sample list; with no array, pass --sample args.
set -uo pipefail
cd /scratch/phyberos/bench/archive/e2
sel=${SLURM_ARRAY_TASK_ID:+--index $SLURM_ARRAY_TASK_ID}
~/bench_client_env/bin/python -u e2_archive.py pack --manifest archive_manifest.tsv \
    --cache /scratch/phyberos/cami/metasmith/task_cache --dest /scratch/phyberos/bench/archive/e2 ${sel:-"$@"}
rc=$?
echo "EXIT $rc"
exit $rc
