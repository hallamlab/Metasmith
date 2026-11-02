#!/bin/bash
# Concatenate every sample's compare_arms_sample.py outputs into one gzipped table per kind.
set -euo pipefail
D=${1:-/scratch/phyberos/bench/compare_arms}
mkdir -p $D/raw
for k in assembly contig_matches mags skani_pairs; do
    files=($D/*/$k.tsv)
    { head -1 ${files[0]}; for f in "${files[@]}"; do tail -n +2 $f; done; } | gzip -n > $D/raw/$k.tsv.gz
    echo "$k: ${#files[@]} samples, $(zcat $D/raw/$k.tsv.gz | tail -n +2 | wc -l) rows"
done
