#!/bin/bash
#SBATCH --account=rpp-shallam
#SBATCH --job-name=e2_archive_census
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=3:00:00
#SBATCH --output=/scratch/phyberos/bench/archive/e2/census.%j.log
# Read-only census of E2 products in the cami task cache.
set -uo pipefail
cd /scratch/phyberos/bench/archive/e2
echo "START $(date -Is) $(hostname)"
~/bench_client_env/bin/python -u e2_archive.py census --cache /scratch/phyberos/cami/metasmith/task_cache --out archive_manifest.tsv
echo "EXIT $? $(date -Is)"
