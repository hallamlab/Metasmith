#!/bin/bash
# Restore E3's agent home from the chinook archive back onto fir, then verify it.
#
# Run this from the workstation that holds the Globus CLI, not from fir. It submits one
# Globus task and prints the verification commands to run once that task succeeds.
#
# WARNING The destination path is not a choice. `given_name()` folds the absolute path string
# of every declared input into the pool entry's name, so a home restored anywhere other than
# /scratch/phyberos/pratama2026 renames every pool entry and silently cools the whole cache.
# Nothing raises. The run simply recomputes 2 TB of products. The path is hard-coded below for
# that reason -- do not parameterise it.
#
# CAUTION Six external trees are NOT in this archive and must already exist on fir. The script
# checks them and refuses to start when one is missing, because a restore that lands on a
# missing reference database fails at the far end of a multi-hour transfer.
set -euo pipefail

GLOBUS="${GLOBUS_BIN:-/home/tony/lib/miniforge3/envs/globus/bin/globus}"
CHINOOK=2602486c-1e0f-47a0-be15-eec1b0ff0f96
FIR=8dec4129-9ab4-451d-a45f-5b4b8471f7a3
ARCHIVE=/Workspace_backups/Tony_Liu/fir_bench_e3
HOME_PATH=/scratch/phyberos/pratama2026

# Taken 2026-09-22 after driver 60837426 exited COMPLETED 0:0. A restored index that does not
# match these is not the archived index.
CACHE_SHA=eaee257a460bf743cd4631fb1dd66853fa680129caaaa0ade316f8e79598bc0a
CACHE_ENTRIES=11326
CACHE_LINEAGE=11083
CACHE_IMPORTED=243

# Every absolute path any cache payload names, extracted from the 243 imported rows.
EXTERNAL=(
    /project/6004975/phyberos/cplex/cplex_runtime
    /project/6004975/phyberos/lib/diamond/uniref50.dmnd
    /project/6004975/phyberos/lib/kofamscan/ko_list.tsv
    /project/6004975/phyberos/lib/kofamscan/profiles
    /scratch/phyberos/databases/genomad
    /scratch/phyberos/refs/checkv_db
    /scratch/phyberos/refs/dram_1.5.0
    /scratch/phyberos/refs/metapop_0.0.60_env.sqfs
    /scratch/phyberos/refs/vcontact3_v230
    /scratch/phyberos/refs/vibrant_1.2.1
    /scratch/phyberos/refs/virsorter2_2.2.4
    /scratch/phyberos/staging/gtdb/release232
    /scratch/phyberos/staging/gtdb/release232_skani_genomes.sqfs
    /scratch/phyberos/viromics_refs/iphop_db/Aug_2023_pub_rw
    /scratch/phyberos/wave2_b3_nfcore/checkm2_db/CheckM2_database/uniref100.KO.1.dmnd
)

usage() { echo "usage: $0 {preflight|transfer|verify <after transfer succeeds>}" >&2; exit 2; }
[ $# -ge 1 ] || usage

preflight() {
    echo "== external dependencies on fir =="
    local missing=0
    for p in "${EXTERNAL[@]}"; do
        if ssh fir "test -e '$p'" 2>/dev/null; then
            echo "  ok      $p"
        else
            echo "  MISSING $p"
            missing=$((missing + 1))
        fi
    done
    echo "missing: $missing of ${#EXTERNAL[@]}"

    echo "== fir has room =="
    ssh fir 'lfs quota -p 83115734 /scratch' || true
    echo "the archive needs about 3.83 TB and 145,220 inodes"

    echo "== the destination must be absent or empty =="
    ssh fir "test -e '$HOME_PATH' && echo 'EXISTS -- resolve before restoring' || echo 'absent, good'"

    [ "$missing" -eq 0 ] || { echo "refusing: $missing external dependencies are missing" >&2; exit 1; }
}

transfer() {
    preflight
    echo "== submitting =="
    "$GLOBUS" transfer \
        "$CHINOOK:$ARCHIVE/pratama2026" "$FIR:$HOME_PATH" \
        --recursive --verify-checksum --preserve-timestamp --sync-level checksum --notify off \
        --label "E3 restore pratama2026 $(date +%F)"
    echo
    echo "poll with: $GLOBUS task show <task id>"
    echo "then run:  $0 verify"
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

    echo "== file count against the archived manifest =="
    echo "manifest lives at $ARCHIVE/meta/manifest_full.tsv.gz -- 145,220 entries,"
    echo "110,780 files, 30,929 directories, 3,511 symlinks, 3,832,855,343,605 bytes."
    ssh fir "find '$HOME_PATH' -path '$HOME_PATH/metasmith/runs/Qt0rbV1R/nxf_work' -prune -o \
             -path '$HOME_PATH/metasmith/runs/Qt0rbV1R/results' -prune -o -print | wc -l"

    echo "== shard count against the index =="
    ssh fir "ls -1 '$HOME_PATH/metasmith/task_cache/1e' | wc -l"
    echo "expected 11583 shard directories, of which 11083 carry an index row"
}

case "$1" in
    preflight) preflight ;;
    transfer)  transfer ;;
    verify)    verify ;;
    *)         usage ;;
esac
