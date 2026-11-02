#!/bin/bash
set -uo pipefail

HOME_PATH=/scratch/phyberos/metagem
WORK=/scratch/phyberos/bench/metagem_archive
QUOTA_UID=83115734

quota() { { lfs quota -p "$QUOTA_UID" /scratch | tail -1; } || true; }

usage() { echo "usage: $0 {preflight|inventory}" >&2; exit 2; }
[ $# -ge 1 ] || usage

preflight() {
    echo "== jobs naming metagem =="
    local hits
    hits=$(squeue -u "$USER" -h -o '%i|%j|%o|%Z' | grep -i metagem || true)
    if [ -n "$hits" ]; then
        echo "ABORT: jobs reference metagem:" >&2
        echo "$hits" >&2
        exit 1
    fi
    echo "  none"

    echo "== PID.lock under $HOME_PATH =="
    local lock
    lock=$(find "$HOME_PATH" -iname 'PID.lock' 2>/dev/null)
    if [ -n "$lock" ]; then
        echo "ABORT: PID.lock present:" >&2
        echo "$lock" >&2
        exit 1
    fi
    echo "  none"

    echo "== other metasmith homes/pools referencing $HOME_PATH =="
    local f found=0
    while IFS= read -r f; do
        if grep -q "$HOME_PATH" "$f" 2>/dev/null; then
            echo "  HIT: $f"
            found=1
        fi
    done < <(find /scratch/phyberos -maxdepth 8 \
        \( -path "$HOME_PATH" -o -path /scratch/phyberos/refs -o -path /scratch/phyberos/viromics_refs \
           -o -path /scratch/phyberos/databases -o -path /scratch/phyberos/apptainer_cache \
           -o -path /scratch/phyberos/cache \) -prune -o \
        \( -iname 'task.yml' -o -iname 'agent.yml' -o -iname 'workflow.*' -o -iname 'pool.yml' -o -iname '*.pool' \) -print 2>/dev/null)
    [ "$found" -eq 0 ] && echo "  none"

    echo "== quota =="
    quota
}

inventory() {
    mkdir -p "$WORK"
    cd "$HOME_PATH" || exit 1

    find . -printf '%y\t%s\t%T@\t%d\t%P\t%l\n' > "$WORK/raw_listing.tsv"

    awk -F'\t' '$1=="f"{print $5"\t"$2"\t"$3}' "$WORK/raw_listing.tsv" > "$WORK/manifest_full.tsv"
    awk -F'\t' '$1=="l"{print $5"\t"$6}' "$WORK/raw_listing.tsv" > "$WORK/symlinks.tsv"
    find . -type d -empty -printf '%P\n' > "$WORK/empty_dirs.txt"

    awk -F'\t' '{split($1,a,"/"); n[a[1]]++; b[a[1]]+=$2} END{for(t in n) printf "%s\t%d\t%d\n", t, n[t], b[t]}' \
        "$WORK/manifest_full.tsv" | sort > "$WORK/per_top_level.tsv"

    sha256sum "$HOME_PATH/metasmith/task_cache/cache.sqlite" > "$WORK/cache_sqlite.sha256"

    awk -F'\t' '{if($4>m)m=$4} END{print m}' "$WORK/raw_listing.tsv" > "$WORK/max_depth.txt"

    echo "== counts =="
    wc -l "$WORK/manifest_full.tsv" "$WORK/symlinks.tsv" "$WORK/empty_dirs.txt"
    echo "== per top level =="
    cat "$WORK/per_top_level.tsv"
    echo "== max depth =="
    cat "$WORK/max_depth.txt"
    echo "== cache.sqlite =="
    cat "$WORK/cache_sqlite.sha256"
    echo "inventory in $WORK"
}

case "$1" in
    preflight) preflight ;;
    inventory) inventory ;;
    *) usage ;;
esac
