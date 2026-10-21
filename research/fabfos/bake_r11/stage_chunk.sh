#!/usr/bin/env bash
# Lay the assembled parts out in the shape a chunk has, so the checks can read it.
#
# WHAT IS CARRIED FORWARD UNCHANGED, and why each has to be:
#   aam_cache/  the mapped SMILES. Nothing re-mapped, so these are the same bytes, and
#               they are what the NEXT generation resumes from.
#   logs/       the mapper's own per-reaction records. `aam_forecast` reads this directory
#               as `fabfos_data::prior_bake_logs` to decide its offer set. Rewriting it
#               from a run that ran no mapper would describe silences that did not happen.
#   seams/      the lanes upstream of the mappers -- rescue, algebra, partial, worklist,
#               forecast, recount, and the direction members. None of them moved.
# The seams that DID move are overwritten from the assembly below.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SRC="${OUT:-$REPO/data/fabfos/scratch/r11}"
DEPLOYED="$REPO/data/fabfos/processed/metabolism_bake"
CHUNK="${CHUNK:-$REPO/data/fabfos/scratch/r11_chunk}"

for f in atom_pairs.parquet vocab.parquet direction.parquet ref/ledger.parquet \
         aam_stack.parquet injectivity.tsv; do
    [ -s "$SRC/$f" ] || { echo "missing $SRC/$f -- assemble.sh did not finish" >&2; exit 1; }
done

rm -rf "$CHUNK"
mkdir -p "$CHUNK"

# `cp -a`: the deployed files are read-only hardlinks into a DVC cache shared with a dozen
# sibling worktrees, and the chunk carries internal hardlink pairs of its own.
cp -a "$DEPLOYED/aam_cache" "$CHUNK/aam_cache"
cp -a "$DEPLOYED/logs"      "$CHUNK/logs"
cp -a "$DEPLOYED/seams"     "$CHUNK/seams"
chmod -R u+w "$CHUNK/seams"

cp "$SRC/atom_pairs.parquet"  "$CHUNK/atom_pairs.parquet"
cp "$SRC/vocab.parquet"       "$CHUNK/vocab.parquet"
cp "$SRC/direction.parquet"   "$CHUNK/direction.parquet"
cp "$SRC/ref/ledger.parquet"  "$CHUNK/ledger.parquet"

# the seams this generation rewrote
cp "$SRC/aam_stack.parquet"                     "$CHUNK/seams/aam_stack.parquet"
cp "$SRC/members/aam_member_indigo.parquet"     "$CHUNK/seams/aam_member_indigo.parquet"
cp "$SRC/members/aam_member_rxnmapper.parquet"  "$CHUNK/seams/aam_member_rxnmapper.parquet"
cp "$SRC/members/aam_member_localmapper.parquet" "$CHUNK/seams/aam_member_localmapper.parquet"
mkdir -p "$CHUNK/seams/aam_pairs"
cp "$SRC"/redox/*                               "$CHUNK/seams/aam_pairs/"
cp "$SRC/injectivity.tsv"                       "$CHUNK/seams/injectivity.tsv"
cp "$SRC/ref/ledger_summary.tsv"                "$CHUNK/seams/ledger_summary.tsv"

echo "staged -> $CHUNK"
du -sh "$CHUNK"
