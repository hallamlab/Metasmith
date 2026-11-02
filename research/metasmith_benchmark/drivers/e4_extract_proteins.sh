#!/bin/bash
# Unpack metaGEM's published protein bins into one plain FASTA per bin, where e4_gems.py registers them.
# Run on fir from the drivers directory: sbatch e4_extract_proteins.sh [bin ...]   (no bins = all 14,105)
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=e4_extract_proteins
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=6:00:00
#SBATCH --output=e4_extract_proteins.%j.log
set -euo pipefail

ROOT=${METAGEM_PUBLISHED:-/scratch/phyberos/metagem/published}
MANIFEST=${SLURM_SUBMIT_DIR:-$(dirname "$0")}/e4_published_proteins.tsv
BINS=" $* "

for study in li2019 korem2015 karlsson2013 bissett_base sunagawa2015; do
    archive=$ROOT/$study/MAGs_protein.tar.gz
    [ "$study" = sunagawa2015 ] && archive=$ROOT/$study/tara_protein.gz.tar
    out=$ROOT/$study/proteins
    tmp=$ROOT/$study/.proteins_unpack
    rows=$(awk -F'\t' -v s="$study" -v bins="$BINS" \
        'NR > 1 && $1 == s && (bins ~ /^ *$/ || index(bins, " " $3 " ")) {print $2 "\t" $3}' "$MANIFEST")
    [ -n "$rows" ] || continue
    rm -rf "$tmp"
    mkdir -p "$tmp" "$out"
    cut -f1 <<< "$rows" > "$tmp.members"
    tar xf "$archive" -C "$tmp" -T "$tmp.members"
    while IFS=$'\t' read -r member bin; do
        if [[ $member == *.gz ]]; then gunzip -c "$tmp/$member" > "$out/$bin.faa"; else mv "$tmp/$member" "$out/$bin.faa"; fi
    done <<< "$rows"
    rm -rf "$tmp" "$tmp.members"
    echo "$study: $(wc -l <<< "$rows") bins unpacked, $(find "$out" -name '*.faa' | wc -l) in $out"
done
