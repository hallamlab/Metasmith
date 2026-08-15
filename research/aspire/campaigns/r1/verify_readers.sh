#!/bin/bash
# Cross-check the read-splitting bbtools readers against the independent ground
# truth, for free, using reports the DAG produces anyway.
#
# F1 is a bug in bbmap's BGZF reader, and `unbgzip=f` was applied at four call
# sites. The interleave site is checked directly by verify_products.sh. The
# other sites are inside kraken2/centrifuger transforms, which run their own
# reformat.sh split before classifying -- so their reports' total read count is
# a number that has passed through a patched reader, and it is free evidence.
#
# Both report formats are kraken-style: col2 is CUMULATIVE reads at a clade, so
# unclassified + root is the total handed to the classifier. kraken2 marks root
# with rank "R"; centrifuger leaves rank as "-" and only taxid 1 identifies it,
# hence the `$4=="U" || $5==1` predicate rather than a rank test alone.
#
# The check is stronger than "26 numbers were equal": each total must land on
# some truth sample's read_pairs, all must be DISTINCT, and none may be
# unmatched. A reader dropping a constant fraction would still produce plausible
# numbers -- but not numbers that are each exactly some other library's count.

set -uo pipefail

RUN=${1:?usage: verify_readers.sh <run_dir>}
TRUTH=${2:-/scratch/phyberos/gmcf3495/truth}
D="$RUN/results"

# This script only means anything on fir. Run it anywhere else and every glob
# below misses, every check reports "(no reports yet)", and it exits 0 -- a pass
# indistinguishable from a run that has not started. Refuse instead.
[ -d "$RUN" ] || { echo "no such run dir: $RUN (this script runs ON fir)"; exit 2; }

declare -A P
for f in "$TRUTH"/*.truth; do P[$(cut -f2 "$f")]=$(cut -f1 "$f"); done
[ "${#P[@]}" -gt 0 ] || { echo "no truth files under $TRUTH"; exit 1; }

rc=0

check () {   # $1=label  $2=report dir
    local label="$1" dir="$2"
    [ -d "$dir" ] || { echo "$label: (no reports yet)"; return 0; }
    local m=0 u=0 dups=0 tot s
    declare -A SEEN=()
    for r in "$dir"/*; do
        [ -f "$r" ] || continue
        tot=$(awk -F'\t' '$4=="U" || $5==1 {s+=$2} END{print s+0}' "$r")
        s=${P[$tot]:-}
        if [ -n "$s" ]; then
            m=$((m+1)); SEEN[$s]=$(( ${SEEN[$s]:-0} + 1 ))
        else
            u=$((u+1)); echo "  UNMATCHED $(basename "$r") total=$tot"
        fi
    done
    for s in "${!SEEN[@]}"; do
        [ "${SEEN[$s]}" != "1" ] && { echo "  DUP $s x${SEEN[$s]}"; dups=$((dups+1)); }
    done
    echo "$label: reports=$(ls "$dir" 2>/dev/null | wc -l) matched=$m unmatched=$u distinct=${#SEEN[@]} dups=$dups"
    [ "$u" -eq 0 ] && [ "$dups" -eq 0 ] || rc=1
}

check "kraken2    " "$D/taxonomy-kraken2_report"
check "centrifuger" "$D/taxonomy-centrifuger_kreport"

# seqkit is not a bbtools reader at all -- it is a separate Go implementation
# with its own gzip decoder -- and the DAG already runs it over every step-1
# product. Its `reads` field must equal the FULL interleaved count (r1+r2), not
# read_pairs, so this matches on column 4 of the truth table rather than 2.
# Together with verify_products.sh (pigz) that is two independent decompressors
# agreeing on all 34 products.
seqkit_check () {
    local dir="$D/sequences-read_qc_stats"
    [ -d "$dir" ] || { echo "seqkit     : (no stats yet)"; return 0; }
    local m=0 u=0 dups=0 n s
    declare -A EXP=() SEEN=()
    for f in "$TRUTH"/*.truth; do EXP[$(cut -f4 "$f")]=$(cut -f1 "$f"); done
    for f in "$dir"/*; do
        [ -f "$f" ] || continue
        n=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['reads'])" "$f")
        s=${EXP[$n]:-}
        if [ -n "$s" ]; then
            m=$((m+1)); SEEN[$s]=$(( ${SEEN[$s]:-0} + 1 ))
        else
            u=$((u+1)); echo "  UNMATCHED $(basename "$f") reads=$n"
        fi
    done
    for s in "${!SEEN[@]}"; do
        [ "${SEEN[$s]}" != "1" ] && { echo "  DUP $s x${SEEN[$s]}"; dups=$((dups+1)); }
    done
    echo "seqkit     : files=$(ls "$dir" 2>/dev/null | wc -l) matched=$m unmatched=$u distinct=${#SEEN[@]} dups=$dups"
    [ "$u" -eq 0 ] && [ "$dups" -eq 0 ] || rc=1
}
seqkit_check

# Completeness is a separate question from reader correctness, and conflating
# them is how a gap hides: the checks above verify that the reports which EXIST
# carry believable read totals, and say nothing about reports that are missing.
#
# Bracken appears here and deliberately NOT above. It never reads a fastq -- it
# re-estimates abundance from kraken2's report -- so its totals are derived
# rather than independent reader evidence, and feeding them to `check` would
# flag every sample UNMATCHED. It still has to be present for all 34.
completeness () {
    local n short=0
    echo "-- completeness (expect $EXPECT per product) --"
    for d in "$D"/*/; do
        n=$(ls "$d" 2>/dev/null | wc -l)
        [ "$n" -eq "$EXPECT" ] && continue
        printf "  SHORT %-45s %3d/%d\n" "$(basename "$d")" "$n" "$EXPECT"
        short=$((short+1))
    done
    # Deliberately a DIFFERENT exit code from a reader failure. Mid-run every
    # product is short and that is normal; a reader mismatch never is. Folding
    # both into rc=1 would train the wake check to ignore the one that matters.
    #   0 = readers agree and everything is present
    #   1 = a reader disagreed with ground truth  -> investigate now
    #   3 = readers fine, run simply not finished -> expected while it runs
    [ "$short" -eq 0 ] && echo "  all products complete" || { [ "$rc" -eq 0 ] && rc=3; }
}
# Count truth FILES, not P's keys: P is keyed by read count, so two libraries
# that happened to share a total would silently lower the bar to 33.
EXPECT=${EXPECT:-$(ls "$TRUTH"/*.truth 2>/dev/null | wc -l)}
completeness

exit $rc
