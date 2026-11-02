#!/bin/bash
# Map each step-1 interleaved product back to the library it came from.
#
# The product filename is a metasmith content hash and carries no sample label,
# so the mapping has to come from the task that produced it: the nextflow work
# dir holds exactly one .fq.gz alongside a .command.sh naming the source reads.
# We resolve the product to its results/ copy, because that is the inode the
# rest of the DAG consumes (C4: each product exists three times on disk).

set -uo pipefail

RUN=${1:?usage: build_product_map.sh <run_dir>}
TRUTH=/scratch/phyberos/gmcf3495/truth
OUT="$TRUTH/product_map.tsv"

mkdir -p "$TRUTH"
: > "$OUT.tmp"

for d in "$RUN"/nxf_work/*/*; do
    [ -f "$d/.exitcode" ] || continue
    [ "$(cat "$d/.exitcode")" = "0" ] || continue
    sample=$(grep -o '/scratch/phyberos/gmcf3495/reads/[A-Za-z0-9_]*_R1\.fastq\.gz' "$d/.command.sh" 2>/dev/null \
             | head -1 | xargs -r basename | sed 's/_R1\.fastq\.gz$//')
    [ -n "$sample" ] || continue          # not a step-1 task
    prod=$(ls "$d"/*.fq.gz 2>/dev/null | head -1)
    [ -n "$prod" ] || continue
    base=$(basename "$prod")
    resolved="$RUN/results/1_sequences-short_reads/$base"
    if [ ! -f "$resolved" ]; then
        echo "WARN no results/ copy for $sample ($base); falling back to work dir" >&2
        resolved="$prod"
    fi
    printf '%s\t%s\n' "$sample" "$resolved" >> "$OUT.tmp"
done

sort -u "$OUT.tmp" > "$OUT"
rm -f "$OUT.tmp"

n=$(wc -l < "$OUT")
dupes=$(cut -f1 "$OUT" | sort | uniq -d)
echo "product_map.tsv rows=$n"
[ -n "$dupes" ] && { echo "DUPLICATE SAMPLES: $dupes"; exit 1; }
exit 0
