#!/bin/bash
#SBATCH --account=rpp-shallam
#SBATCH --job-name=e1_close_extract
#SBATCH --time=02:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1
#SBATCH --exclude=fc30557,fc30564
#SBATCH --output=/scratch/phyberos/bench/e1_close/logs/extract.%j.log
set -euo pipefail
A=/scratch/phyberos/bench/archive
W=/scratch/phyberos/bench/e1_close
E=$W/e1_extract
echo "START $(date -Is)"
mkdir -p $E
tar -xf $A/e1_short_out.tar -C $E \
    out/GenomeBinning/QC/checkm2_summary.tsv \
    out/GenomeBinning/contig_to_bin/contig_to_bin_map.tsv
tar -xf $A/e1_long.tar -C $E \
    long/out/GenomeBinning/QC/checkm2_summary.tsv \
    long/out/GenomeBinning/contig_to_bin/contig_to_bin_map.tsv \
    long/b19/contig_to_bin_map.tsv long/b19_rerun.sbatch long/b19_sheet.csv
find $E -type f -printf "%p %s\n"

python3 $W/e1_close.py sheet --cami-samples $W/cami_samples.tsv --e2-manifest $A/e2/archive_manifest.tsv \
    --cache /scratch/phyberos/cami/metasmith/task_cache --short-assemblies $A/e1_amber_inputs/short_assembly \
    --kept-inputs /scratch/phyberos/bench/e1/long/kept_inputs --work $W --out $W/sheet.tsv
python3 $W/e1_close.py split --sheet $W/sheet.tsv --outdir $W/maps \
    --maps $E/out/GenomeBinning/contig_to_bin/contig_to_bin_map.tsv $E/long/b19/contig_to_bin_map.tsv
awk -F'\t' 'NR>1{split($1,a,"_sample"); n[a[1]" "$2]+=$3; z[a[1]" "$2]+=($3==0)} END{for(k in n) print k, n[k], "zero:", z[k]}' $W/maps/bin_counts.tsv | sort
echo "EXIT 0 $(date -Is)"
