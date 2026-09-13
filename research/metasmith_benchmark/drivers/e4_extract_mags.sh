#!/bin/bash
# Unpack metaGEM's published MAG archives into one plain FASTA per MAG, where e4_metagem.py registers them.
# Run on fir: sbatch e4_extract_mags.sh [study ...]
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=e4_extract_mags
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=6:00:00
#SBATCH --output=e4_extract_mags.%j.log
set -euo pipefail

ROOT=${METAGEM_PUBLISHED:-/scratch/phyberos/metagem/published}
STUDIES=("$@")
[ ${#STUDIES[@]} -gt 0 ] || STUDIES=(li2019 korem2015 bissett_base karlsson2013 sunagawa2015)

for study in "${STUDIES[@]}"; do
    archive=$ROOT/$study/MAGs.tar.gz
    [ "$study" = sunagawa2015 ] && archive=$ROOT/$study/tara_mags.tar.gz
    out=$ROOT/$study/mags
    tmp=$ROOT/$study/.mags_unpack
    rm -rf "$tmp"
    mkdir -p "$tmp" "$out"
    tar xzf "$archive" -C "$tmp" --exclude='._*' --exclude='.DS_Store'
    find "$tmp" -type f \( -name '*.fa' -o -name '*.fa.gz' \) | while read -r f; do
        name=$(basename "$f")
        name=${name%.gz}
        if [[ $f == *.gz ]]; then gunzip -c "$f" > "$out/$name"; else mv "$f" "$out/$name"; fi
    done
    rm -rf "$tmp"
    echo "$study: $(find "$out" -name '*.fa' | wc -l) MAGs in $out"
done
