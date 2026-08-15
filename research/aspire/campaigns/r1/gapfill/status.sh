#!/bin/bash
set -eu
I=/scratch/phyberos/gmcf3495/inv
G=/scratch/phyberos/gmcf3495/gapfill
RES=/scratch/phyberos/gmcf3495/metasmith/runs/QkqCNJOo/results

# usable contigs (>=1000bp) per assembly, cached
UC=$I/usable_contigs.tsv
if [ ! -s "$UC" ]; then
  : > $UC
  while IFS=$'\t' read -r s asm bam; do
    n=$(awk '/^>/{if(l>=1000)n++; l=0; next}{l+=length($0)}END{if(l>=1000)n++; print n+0}' "$RES/sequences-megahit_assembly/$asm")
    t=$(grep -c '^>' "$RES/sequences-megahit_assembly/$asm")
    printf '%s\t%s\t%s\n' "$s" "$t" "$n" >> $UC
  done < $I/sample_asm_bam.tsv
fi

printf 'sample\ttotal_contigs\tusable_contigs\tmetabat2\tsemibin2\tcomebin\tcb_class\n'
while IFS=$'\t' read -r s asm bam; do
  read -r _ tot use < <(awk -F'\t' -v s="$s" '$1==s' $UC)
  out=""
  for b in metabat2 semibin2 comebin; do
    st=$(awk -F'\t' -v a="$asm" -v b="$b" '$1==a && $2==b {print $3}' $I/binners_all.tsv | sort -u | tr '\n' ',' )
    [ -z "$st" ] && st="NEVER_RUN"
    out="$out\t${st%,}"
  done
  cb=$(awk -F'\t' -v a="$asm" '$1==a{print $2}' $I/comebin_class.tsv | grep -v '^OTHER:$' | head -1)
  [ -z "$cb" ] && cb="-"
  printf "%s\t%s\t%s$out\t%s\n" "$s" "$tot" "$use" "$cb"
done < $I/sample_asm_bam.tsv
