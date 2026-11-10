#!/bin/bash
#SBATCH --account=def-shallam
#SBATCH --job-name=beth_publish
#SBATCH --cpus-per-task=2
#SBATCH --output=/scratch/phyberos/beth_mp/logs/publish_%j.log
# Usage: sbatch --mem=.. --time=.. publish.sbatch PART
set -euo pipefail
module load apptainer
B=/scratch/phyberos/beth_mp
SIF=$B/port/metasmith/container_images/docker..quay.io_hallamlab_metasmith..0.24.2.sif
apptainer exec --cleanenv -B $B $SIF python3 $B/jobs/publish/publish.py --out $B/publish/copper_enrichments --part "$1"
echo "$(date -Is) done $1"
