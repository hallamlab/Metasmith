#!/bin/bash
# Inventory fir's /scratch/phyberos/e4_ablation before it is archived to chinook.
#
#   sbatch e4_ablation_inventory.sbatch <out dir outside the tree>
#
# Globus copies neither symlinks, empty directories nor dot-entries, so each gets its own list.
#SBATCH --account=rrg-shallam-ab
#SBATCH --job-name=e4abl_inventory
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=3:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/e4abl_inventory.%j.out
set -euo pipefail

TARGET=/scratch/phyberos/e4_ablation
OUT="${1:?usage: $0 <out dir>}"
[[ "$OUT" != "$TARGET" && "$OUT" != "$TARGET"/* ]] || { echo "ABORT: $OUT lies inside $TARGET"; exit 1; }
mkdir -p "$OUT"
cd "$TARGET"

date -u +%Y-%m-%dT%H:%M:%SZ > "$OUT/STARTED_AT"
find . -mindepth 1 -type f -printf '%P\t%s\t%T@\n' | LC_ALL=C sort > "$OUT/manifest.tsv"
find . -mindepth 1 -type l -printf '%P\t%l\n' | LC_ALL=C sort > "$OUT/symlinks.tsv"
find . -mindepth 1 -type d -empty -printf '%P\n' | LC_ALL=C sort > "$OUT/empty_dirs.txt"
find . -mindepth 1 ! -type f ! -type d ! -type l -printf '%P\t%y\n' | LC_ALL=C sort > "$OUT/other_types.tsv"
find . -mindepth 1 -name '.*' -printf '%P\t%y\n' | LC_ALL=C sort > "$OUT/dot_entries.tsv"
find . -mindepth 1 -type d -printf '%P\n' | wc -l > "$OUT/dir_count"
awk -F'\t' '{split($1,p,"/"); top=(index($1,"/")?p[1]:"."); n[top]++; b[top]+=$2}
            END {for (t in n) printf "%s\t%d\t%d\n", t, n[t], b[t]}' "$OUT/manifest.tsv" \
    | LC_ALL=C sort > "$OUT/per_top_level.tsv"
newest=$(sort -t$'\t' -k3,3g "$OUT/manifest.tsv" | tail -1 | cut -f3)
echo "newest_mtime=$(date -u -d "@${newest%.*}" +%Y-%m-%dT%H:%M:%SZ)" > "$OUT/summary.txt"
{
    echo "files=$(wc -l < "$OUT/manifest.tsv")"
    echo "bytes=$(awk -F'\t' '{s+=$2} END {printf "%d", s}' "$OUT/manifest.tsv")"
    echo "symlinks=$(wc -l < "$OUT/symlinks.tsv")"
    echo "empty_dirs=$(wc -l < "$OUT/empty_dirs.txt")"
    echo "dirs=$(cat "$OUT/dir_count")"
    echo "other_types=$(wc -l < "$OUT/other_types.tsv")"
    echo "dot_entries=$(wc -l < "$OUT/dot_entries.tsv")"
} >> "$OUT/summary.txt"
cat "$OUT/summary.txt"
cat "$OUT/per_top_level.tsv"
