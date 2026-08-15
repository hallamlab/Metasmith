#!/bin/bash
# Normalize each binner's differently-shaped output into
#   gapfill/bins/<sample>/<binner>/<sample>__<binner>__<n>.fna
# Sample attribution is in the filename by construction here, unlike the
# metasmith products which are content-addressed and need the work-dir join.
#
# metabat2fix / metabat2sub are variants of metabat2, not separate binners:
# they exist because jgi_summarize_bam_contig_depths writes a handful of garbage
# rows on some assemblies. Whichever variant yielded the most bins wins -- on S29
# that is the plain run (41 vs 27), because metabat2 only *warns* on the bad rows
# and drops those contigs, while the sanitising variant also raises the minimum
# contig length. Collapse them here so the catalogue never invents a fourth binner.
set -u
G=/scratch/phyberos/gmcf3495/gapfill
count_dir() {
  ls -1 "$1"/bins/*.fa "$1"/output_bins/*.fa "$1"/output_bins/*.fa.gz \
        "$1"/comebin_res/comebin_res_bins/*.fa 2>/dev/null | wc -l
}
total=0
for d in $G/out/*/; do
  S=$(basename "$d")
  declare -A best=() bestn=()
  for bd in "$d"*/; do
    B=$(basename "$bd")
    [ "$B" = megahit ] && continue
    case $B in metabat2fix|metabat2sub) BN=metabat2;; *) BN=$B;; esac
    n=$(count_dir "$bd")
    if [ -z "${bestn[$BN]:-}" ] || [ "$n" -gt "${bestn[$BN]}" ]; then
      bestn[$BN]=$n; best[$BN]=$bd
    fi
  done
  for BN in "${!best[@]}"; do
    dest=$G/bins/$S/$BN
    have=$(ls -1 "$dest"/*.fna 2>/dev/null | wc -l)
    # `bins/` is the catalogue's source of truth -- its paths are what
    # catalogue.tsv stores -- while `out/` is a scratch work dir that
    # submit_bin.sh destroys on every submission. Never let an empty or missing
    # work dir delete bins that were already collected: re-submitting a binner
    # for a sample that already has bins would otherwise erase them here, one
    # step removed from the `rm -rf` that actually did it.
    if [ "${bestn[$BN]}" -eq 0 ] && [ "$have" -gt 0 ]; then
      echo -e "${S}\t${BN}\t${have}\tKEPT (work dir empty; collected bins preserved)"
      total=$((total+have))
      continue
    fi
    rm -rf "$dest"; mkdir -p "$dest"
    n=0
    for f in "${best[$BN]}"bins/*.fa "${best[$BN]}"output_bins/*.fa \
             "${best[$BN]}"output_bins/*.fa.gz \
             "${best[$BN]}"comebin_res/comebin_res_bins/*.fa; do
      [ -e "$f" ] || continue
      n=$((n+1))
      if [[ "$f" == *.gz ]]; then gunzip -c "$f" > "$dest/${S}__${BN}__${n}.fna"
      else cp "$f" "$dest/${S}__${BN}__${n}.fna"; fi
    done
    if [ "$n" -gt 0 ]; then
      echo -e "${S}\t${BN}\t${n}\t$(basename ${best[$BN]%/})"
      total=$((total+n))
    else
      rmdir "$dest" 2>/dev/null
    fi
  done
  unset best bestn
done
echo "total gap-filled bins: $total"
