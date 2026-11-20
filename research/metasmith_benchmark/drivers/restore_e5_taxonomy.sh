#!/bin/bash
# Copy E5's assemblies and DAS Tool MAGs from chinook to fir, where e5.py's taxonomy plans read them.
set -euo pipefail

GLOBUS="${GLOBUS_BIN:-/home/tony/lib/miniforge3/envs/globus/bin/globus}"
CHINOOK=2602486c-1e0f-47a0-be15-eec1b0ff0f96
FIR=8dec4129-9ab4-451d-a45f-5b4b8471f7a3
ARCHIVE=/Workspace_backups/Tony_Liu/fir_bench_e5/e5_home/task_cache
DEST=/scratch/phyberos/e5_tax/inputs
INPUTS="$(dirname "$0")/../results/e5/taxonomy_inputs.tsv"

usage() { echo "usage: $0 {transfer|verify} [STUDY...]" >&2; exit 2; }
[ $# -ge 1 ] || usage
cmd=$1; shift

batch() {
    awk -F'\t' -v studies="$*" -v src="$ARCHIVE" -v dst="$DEST" '
        BEGIN { n = split(studies, s, " "); for (i = 1; i <= n; i++) want[s[i]] = 1 }
        NR > 1 && (n == 0 || $2 in want) { print src "/" $6 "/out/" $5, dst "/" $6 "/out/" $5 }' "$INPUTS"
}

case $cmd in
transfer)
    b=$(mktemp)
    batch "$@" > "$b"
    echo "$(wc -l < "$b") files"
    "$GLOBUS" transfer --batch "$b" "$CHINOOK" "$FIR" \
        --verify-checksum --preserve-timestamp --sync-level checksum --notify off \
        --label "e5 taxonomy inputs $(date +%F)"
    rm -f "$b"
    ;;
verify)
    batch "$@" | awk '{print $2}' | ssh fir 'n=0; m=0; while read -r p; do n=$((n+1)); [ -s "$p" ] || { m=$((m+1)); echo "MISSING $p"; }; done; echo "$n files, $m missing"'
    ;;
*) usage ;;
esac
