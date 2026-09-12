#!/bin/bash
# usage: run_skani.sh <sample>
# Stages that sample's quality MAGs (from catalogue.tsv) and runs skani triangle.
# Clustering itself is done by cluster.py, which reproduces the transform's
# union-find at 95/99% ANI with a medoid centroid per cluster.
set -eu
G=/scratch/phyberos/gmcf3495/gapfill
. $G/env.sh
S=$1; T=${2:-8}
WD=$G/skani/$S; rm -rf "$WD"; mkdir -p "$WD/staged"
awk -F'\t' -v s="$S" 'NR>1 && $2==s && $7==1 {print $1"\t"$8}' $G/catalogue.tsv > $WD/bins.tsv
n=$(wc -l < $WD/bins.tsv)
[ "$n" -gt 0 ] || { echo -e "$S\t0\tno quality bins"; exit 0; }
: > $WD/bins.list
while IFS=$'\t' read -r uid path; do
  cp "$path" "$WD/staged/$uid.fna"
  echo "$WD/staged/$uid.fna" >> $WD/bins.list
done < $WD/bins.tsv
apptainer exec $BIND "$SIF_SKANI" skani triangle -l $WD/bins.list --sparse -o $WD/skani_ani.tsv -t $T
echo -e "$S\t$n\tok"
