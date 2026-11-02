#!/bin/bash
# Tabulate MEMOTE's per-model results for metaGEM and both E4 lanes (e4_quality.py).
#
#   sbatch e4_quality.sbatch <drivers dir> <out dir>
#SBATCH --account=rpp-shallam
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=4:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/e4_quality.%j.out
set -euo pipefail

DRIVERS="$1"
OUT="$2"
mkdir -p "$OUT"
cd "$DRIVERS"

for source in metagem repro modern; do
    python3 e4_quality.py --sources "$source" --out "$OUT" 2> "$OUT/quality_$source.log" &
done
wait
