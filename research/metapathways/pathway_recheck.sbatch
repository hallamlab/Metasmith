#!/bin/bash
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=pw_recheck
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=1:00:00
#SBATCH --output=/scratch/phyberos/metapathways_mp/pffix/logs/recheck_%j.log
# Usage: sbatch pathway_recheck.sbatch SAMPLE PORT_RUN_KEY [LABEL]
set -euo pipefail
B=/scratch/phyberos/metapathways_mp
J=$B/pffix/scripts
SIF=$B/port/metasmith/container_images/docker..quay.io_hallamlab_metasmith..0.24.2.sif
S=$1 R=$B/port/metasmith/runs/$2 O=$B/pffix/${3:-names}/$1
py() { apptainer exec --cleanenv -B $B $SIF python3 "$@"; }
py $J/export_pgdb.py --sample $S --bins $B/mp4/mag_maps/$S.tsv --port-run $R --out-dir $O/pgdb_port
py $J/pathway_recheck.py $S $O/pgdb_port | tee $O/pathways.tsv
