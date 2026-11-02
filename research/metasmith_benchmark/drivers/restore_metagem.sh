#!/bin/bash
set -euo pipefail

GLOBUS="${GLOBUS_BIN:-/home/tony/lib/miniforge3/envs/globus/bin/globus}"
CHINOOK=2602486c-1e0f-47a0-be15-eec1b0ff0f96
FIR=8dec4129-9ab4-451d-a45f-5b4b8471f7a3
ARCHIVE=/Workspace_backups/Tony_Liu/fir_metagem
HOME_PATH=/scratch/phyberos/metagem

CACHE_SHA="ebfb6fc500ead4c54bbb0ea915516190b13f3b4d6c29200cf62de4e133101430"
CACHE_ENTRIES=23400
CACHE_LINEAGE=4262
CACHE_IMPORTED=19138

usage() { echo "usage: $0 {preflight|transfer|verify|recreate-symlinks <symlinks.tsv>|recreate-empty-dirs <empty_dirs.txt>}" >&2; exit 2; }
[ $# -ge 1 ] || usage

preflight() {
    echo "== fir has room =="
    ssh fir 'lfs quota -p 83115734 /scratch' || true
    echo "the archive needs about 5.5 TB and 140,000 inodes"

    echo "== the destination must be absent or empty =="
    ssh fir "test -e '$HOME_PATH' && echo 'EXISTS -- resolve before restoring' || echo 'absent, good'"

    echo "== no external dependencies =="
    echo "every entries.output_root in cache.sqlite is relative to the tree; nothing outside"
    echo "$HOME_PATH is required (verified 2026-09-25 against the pre-archive cache)"
}

transfer() {
    preflight
    echo "== submitting =="
    local batch; batch=$(mktemp)
    echo "--recursive $ARCHIVE/metagem $HOME_PATH" > "$batch"
    cat "$batch"
    "$GLOBUS" transfer --batch "$batch" "$CHINOOK" "$FIR" \
        --verify-checksum --preserve-timestamp --sync-level checksum --notify off \
        --label "metagem restore $(date +%F)"
    rm -f "$batch"
    echo
    echo "poll with: $GLOBUS task show <task id>"
    echo "then recreate symlinks and empty directories from the inventory manifests, then run: $0 verify"
}

verify() {
    echo "== cache index =="
    ssh fir "sha256sum '$HOME_PATH/metasmith/task_cache/cache.sqlite'"
    echo "expected: $CACHE_SHA"
    ssh fir "sqlite3 'file:$HOME_PATH/metasmith/task_cache/cache.sqlite?immutable=1' \
        \"SELECT 'entries', COUNT(*) FROM entries
          UNION ALL SELECT 'lineage', COUNT(*) FROM entries WHERE origin='lineage'
          UNION ALL SELECT 'imported', COUNT(*) FROM entries WHERE origin='imported'
          UNION ALL SELECT 'tombstoned', COUNT(*) FROM entries WHERE tombstoned_at IS NOT NULL;\""
    echo "expected: $CACHE_ENTRIES / $CACHE_LINEAGE / $CACHE_IMPORTED / 0"

    echo "== file count against the inventory manifest =="
    ssh fir "find '$HOME_PATH' | wc -l"
    ssh fir "du -sb '$HOME_PATH'"
}

recreate_symlinks() {
    ssh fir "cd '$HOME_PATH' && while IFS=\$'\\t' read -r rel target; do
        [ -n \"\$rel\" ] || continue
        mkdir -p \"\$(dirname \"\$rel\")\" && ln -sfn \"\$target\" \"\$rel\"
    done && echo symlinks recreated" < "$1"
}

recreate_empty_dirs() {
    ssh fir "cd '$HOME_PATH' && xargs -r -d '\\n' mkdir -p -- && echo empty directories recreated" < "$1"
}

case "$1" in
    preflight)          preflight ;;
    transfer)           transfer ;;
    verify)             verify ;;
    recreate-symlinks)  recreate_symlinks "$2" ;;
    recreate-empty-dirs) recreate_empty_dirs "$2" ;;
    *)                  usage ;;
esac
