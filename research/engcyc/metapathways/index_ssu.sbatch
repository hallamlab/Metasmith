#!/bin/bash
#SBATCH -A rrg-shallam-ab
#SBATCH -c 2
#SBATCH --mem=8G
#SBATCH --time=1:00:00
#SBATCH -o /scratch/phyberos/engcyc/mp_pilot/logs/index_ssu.%j.log
# The BLAST nucleotide index of fetch_refs.sh's ssu.fna, with the blast.env image.
# usage: sbatch index_ssu.sbatch <refs_root>/gtdb_rrna
set -euo pipefail
dir=$(realpath "$1")
module load apptainer
apptainer exec -B "$dir:/ref" docker://quay.io/biocontainers/blast:2.16.0--hc155240_2 \
    makeblastdb -in /ref/ssu.fna -dbtype nucl -out /ref/ssu
ls -la "$dir"
