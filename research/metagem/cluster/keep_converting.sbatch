#!/bin/bash
#SBATCH --job-name=metagem_convkeeper
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=0:20:00
#SBATCH --output=/scratch/phyberos/metagem/_logs/%x.%j.out
# Re-arms the conversion until one study's manifest rows are all on disk.
#
# ONE STUDY PER CHAIN, and every file it touches is named after the study -- worklist, array
# job name, keeper job name, verify tag. Four chains sharing one worklist would each rebuild
# it from disk under the other three, so an array would launch against a worklist describing
# a different study. It is also the only shape that reports a rate per study, and both halves
# of this transfer measured per study: S3 gives 32-36 MB/s per stream on karlsson, bissett
# and li2019, and pigz is the whole cost on bissett, whose runs are 8 GB of .sra each.
#
# The keeper exits without arming anything when the study's .sra objects are not staged yet;
# that is a login-node job (fetch_sra.sh) which no compute node can do. It re-arms on a timer
# in that case rather than spinning, so a study can be armed before its bytes have landed.
set -euo pipefail

ROOT=${METAGEM_ROOT:-/scratch/phyberos/metagem}
DS=${DS:?set DS to one dataset}
MAX_ROUNDS=${MAX_ROUNDS:-40}
ROUND=${ROUND:-1}
WIDTH=${WIDTH:-8}
WAIT_MIN=${WAIT_MIN:-20}
PREV_N=${PREV_N:--1}
WORKLIST=$ROOT/_convert.$DS.tsv

cd "$ROOT"

# Headroom, checked with diskusage_report rather than df -- df on this Lustre filesystem
# reports the whole cluster, not this user's allocation, and has misread the picture before.
# The raw CAMI long-read + CAMI III archives are the cheapest thing on scratch to sacrifice: a
# Globus re-fetch from chinook lands in ~20 minutes. Reclaim only them, and only if tight.
report=$(diskusage_report 2>&1 || true)
line=$(echo "$report" | grep '/scratch (user' || true)
echo "[$DS] round $ROUND headroom: $line"
used_gib=$(echo "$line" | grep -oE '[0-9]+GiB' | head -1 | tr -d 'GiB' || true)
total_tib=$(echo "$line" | grep -oE '[0-9]+TiB' | head -1 | tr -d 'TiB' || true)
used_files_k=$(echo "$line" | grep -oE '[0-9]+K/' | head -1 | tr -d 'K/' || true)
total_files_k=$(echo "$line" | grep -oE '/[0-9]+K' | head -1 | tr -d '/K' || true)
if [ -n "$used_gib" ] && [ -n "$total_tib" ] && [ -n "$used_files_k" ] && [ -n "$total_files_k" ]; then
    free_gib=$(( total_tib * 1024 - used_gib ))
    free_files_k=$(( total_files_k - used_files_k ))
    echo "[$DS] round $ROUND free: ${free_gib} GiB, ${free_files_k}K files"
    if [ "$free_gib" -lt 1500 ] || [ "$free_files_k" -lt 50 ]; then
        if [ -d /scratch/phyberos/cami/cami2_challenge ] || [ -d /scratch/phyberos/cami/cami3_toy_humangut ]; then
            echo "[$DS] round $ROUND headroom tight -- reclaiming raw CAMI long-read + CAMI III archives"
            rm -rf /scratch/phyberos/cami/cami2_challenge/marine/long_read
            rm -rf /scratch/phyberos/cami/cami2_challenge/strain/long_read
            rm -rf /scratch/phyberos/cami/cami2_challenge/plant_associated/long_read_nano
            rm -rf /scratch/phyberos/cami/cami2_challenge/plant_associated/long_read_pacbio
            rm -rf /scratch/phyberos/cami/cami3_toy_humangut/long
            rm -rf /scratch/phyberos/cami/cami3_toy_humangut/short
        else
            echo "[$DS] round $ROUND headroom tight but raw CAMI archives already gone -- nothing left to reclaim"
        fi
    fi
fi

python3 build_convert_list.py --table "runs.$DS.tsv" --root . --dataset "$DS" --out "$WORKLIST"
N=$(wc -l < "$WORKLIST")
DONE=$(python3 - "$DS" <<'PY'
import csv, sys
from pathlib import Path
ds = sys.argv[1]
rows = [r for r in csv.DictReader(open("manifest.tsv"), delimiter="\t") if r["dataset"] == ds]
have = sum(1 for r in rows if (Path(ds)/r["relpath"]).is_file())
print(f"{have}/{len(rows)}")
PY
)
echo "[$DS] round $ROUND: $N runs convertible, files on disk $DONE"

# This round only follows an array submission when the caller passed PREV_N (the outstanding
# count right before that array was launched). A "waiting for the login-node fetch to stage
# more .sra" round never runs an array and must not be judged as one -- it always shows
# unchanged counts by design, and that is not a stall.
if [ "$PREV_N" != "-1" ] && [ "$N" -eq "$PREV_N" ] && [ "$N" -gt 0 ]; then
    echo "[$DS] STALL: the last array converted zero of $N outstanding runs -- stopping rather than retrying identically"
    exit 1
fi

if [ "$N" -eq 0 ]; then
    case "$DONE" in
        */*) have=${DONE%%/*}; want=${DONE##*/} ;;
    esac
    if [ "$have" = "$want" ]; then
        echo "[$DS] ALL CONVERTED $(date -Is)"
        DS="$DS" sbatch --job-name="metagem_verify_$DS" verify_sra.sbatch
        exit 0
    fi
    if [ "$ROUND" -ge "$MAX_ROUNDS" ]; then
        echo "[$DS] STOPPING after $ROUND rounds, $DONE on disk and no .sra left to convert"
        exit 1
    fi
    echo "[$DS] waiting ${WAIT_MIN}m for the login-node fetch to stage more .sra"
    ROUND=$((ROUND + 1)) DS="$DS" MAX_ROUNDS="$MAX_ROUNDS" WIDTH="$WIDTH" WAIT_MIN="$WAIT_MIN" PREV_N="-1" \
        sbatch --job-name="metagem_convkeeper_$DS" --begin="now+${WAIT_MIN}minutes" \
        keep_converting.sbatch
    exit 0
fi
if [ "$ROUND" -ge "$MAX_ROUNDS" ]; then
    echo "[$DS] STOPPING after $ROUND rounds with $N outstanding"
    exit 1
fi

A=$(DS="$DS" WORKLIST="$WORKLIST" sbatch --parsable --job-name="metagem_convert_$DS" \
        --array=0-$((N - 1))%${WIDTH} convert_array.sbatch)
echo "[$DS] round $ROUND submitted array $A width $WIDTH, watching for $N to shrink"
ROUND=$((ROUND + 1)) DS="$DS" MAX_ROUNDS="$MAX_ROUNDS" WIDTH="$WIDTH" WAIT_MIN="$WAIT_MIN" PREV_N="$N" \
    sbatch --job-name="metagem_convkeeper_$DS" --dependency=afterany:$A keep_converting.sbatch
