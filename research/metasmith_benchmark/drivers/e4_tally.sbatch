#!/bin/bash
# Count every E4 lane's archived GEMs against metaGEM's, then compare each model with its
# published GEM. Writes into <out dir>: tally.tsv and tally.txt (e4_tally.py), and per lane
# parity_<lane>.tsv with its medians in parity_<lane>.txt (e4_parity.py).
#
#   sbatch e4_tally.sbatch <checkout> <out dir>
#
# The extracted models go under <out dir>/models, ~28K files. Delete them once parity is read.
#SBATCH --account=rpp-shallam
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=8:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/e4_tally.%j.out
set -euo pipefail

DRIVERS="$1/research/metasmith_benchmark/drivers"
OUT="$2"
mkdir -p "$OUT"

python3 "$DRIVERS/e4_tally.py" --out "$OUT/tally.tsv" --extract "$OUT/models" > "$OUT/tally.txt"

for lane in repro modern; do
    for model in "$OUT/models/$lane"/*.xml.gz; do
        printf '%s\t%s\n' "$(basename "$model" .xml.gz)" "$model"
    done > "$OUT/pairs_$lane.tsv"
    python3 "$DRIVERS/e4_parity.py" "$OUT/pairs_$lane.tsv" > "$OUT/parity_$lane.tsv" 2> "$OUT/parity_$lane.txt" &
done
wait
