#!/bin/bash
# The mechanical tail of a gap-fill pass, in the one order that works:
#
#   collect -> score anything unscored -> catalogue -> census -> skani -> cluster
#
# Idempotent and re-runnable. It does NOT block on CheckM2: if any (sample,
# binner) still needs scoring it submits those jobs, says so, and stops --
# because the catalogue would otherwise be rebuilt over bins with no quality
# numbers and quietly report fewer MAGs than exist. Run it again once the
# checkm jobs land and it proceeds to the clustering.
#
#   bash finish.sh [run_key ...]     # run keys are passed through to build_catalogue.py
set -u
G=/scratch/phyberos/gmcf3495/gapfill
cd $G

echo "== 0. sweep COMEBin intermediates out of the published assembly tree"
# COMEBin writes FragGeneScan and hmmsearch intermediates NEXT TO its input
# FASTA, so any job that was handed a published path scatters them through
# results/sequences-megahit_assembly/. submit_bin.sh stages the assembly now,
# but a job submitted before that fix re-polluted the directory two hours after
# it was cleaned by hand -- the clean has to outlive the jobs, which is why it
# lives here rather than in a one-off command. `<assembly>.fna.<anything>` is
# never a product: the published names end at .fna.
for R in /scratch/phyberos/gmcf3495/metasmith/runs/*/results/*megahit_assembly; do
  [ -d "$R" ] || continue
  n=$(find "$R" -type f -name "*.fna.*" -print -delete | wc -l)
  [ "$n" -gt 0 ] && echo "  removed $n stray file(s) from $R"
done
echo "  swept"

echo
echo "== 1. normalise binner outputs into bins/<sample>/<binner>/"
bash collect_bins.sh

echo
echo "== 2. score any (sample, binner) that has bins but no CheckM2 report"
INFLIGHT=$(squeue -u "$USER" -h -o '%j' || true)
pending=0
for d in $G/bins/*/*/; do
  [ -d "$d" ] || continue
  B=$(basename "$d"); S=$(basename "$(dirname "$d")")
  [ -s "$G/checkm/$S/$B/checkm2_out/quality_report.tsv" ] && continue
  if echo "$INFLIGHT" | grep -qx "gf_checkm_${S}_${B}"; then
    echo "  in flight: $S $B"; pending=$((pending+1)); continue
  fi
  bash submit_checkm.sh "$S" "$B" 16 64G 4:00:00 && pending=$((pending+1))
done
if [ "$pending" -gt 0 ]; then
  echo
  echo "$pending checkm job(s) outstanding -- re-run finish.sh when they land."
  echo "Stopping here: a catalogue built now would score those bins as absent."
  exit 0
fi
echo "  all bins scored"

echo
echo "== 3. rebuild the catalogue"
REFRESH=1 python3 build_catalogue.py "$@"

echo
echo "== 4. rebuild the 34x3 census"
python3 binner_status.py | head -3

echo
echo "== 5. skani over every sample with quality MAGs"
# Per sample, not pooled: the transform this reproduces is group_by=assembly, so
# clustering across samples would answer a different question than the DAG did.
for S in $(awk -F'\t' 'NR>1 && $7==1 {print $2}' catalogue.tsv | sort -u); do
  bash run_skani.sh "$S" 8
done

echo
echo "== 6. cluster"
python3 cluster.py

echo
echo "== 7. the census and the catalogue must name the same (sample, binner) pairs"
# They are built from different sources on purpose -- the census walks work dirs
# and logs so it can distinguish a genuine zero from a job that never ran, the
# catalogue walks collected bins and CheckM2 reports. That independence is only
# worth having if a disagreement is loud. It has already been silent once: a
# cleared work dir made the census read ZERO for a (sample, binner) whose bins
# the catalogue was still publishing.
python3 - <<'PY'
import csv, sys
G = "/scratch/phyberos/gmcf3495/gapfill"
cat = {(r[1], r[2]) for r in csv.reader(open(f"{G}/catalogue.tsv"), delimiter="\t")
       if r[0] != "uid"}
cen = {(r[0], r[3]) for r in csv.reader(open(f"{G}/binner_status.tsv"), delimiter="\t")
       if r[6] == "BINS"}
only_cat, only_cen = sorted(cat - cen), sorted(cen - cat)
if only_cat or only_cen:
    print(f"  MISMATCH  catalogue-only={only_cat}  census-only={only_cen}")
    sys.exit(1)
print(f"  agree: {len(cat)} (sample, binner) pairs with bins")
PY
