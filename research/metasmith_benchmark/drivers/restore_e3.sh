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
# CAUTION Fifteen external paths are NOT in this archive and must already exist on fir. The script
# checks them and refuses to start when one is missing, because a restore that lands on a
# missing reference database fails at the far end of a multi-hour transfer.
set -euo pipefail

GLOBUS="${GLOBUS_BIN:-/home/tony/lib/miniforge3/envs/globus/bin/globus}"
CHINOOK=2602486c-1e0f-47a0-be15-eec1b0ff0f96
FIR=8dec4129-9ab4-451d-a45f-5b4b8471f7a3
ARCHIVE=/Workspace_backups/Tony_Liu/fir_bench_e3
HOME_PATH=/scratch/phyberos/pratama2026
APPTAINER_CACHE=/scratch/phyberos/cache/apptainer

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
    echo "the archive needs about 3.78 TB and 145,199 inodes"

    echo "== the destination must be absent or empty =="
    ssh fir "test -e '$HOME_PATH' && echo 'EXISTS -- resolve before restoring' || echo 'absent, good'"

    [ "$missing" -eq 0 ] || { echo "refusing: $missing external dependencies are missing" >&2; exit 1; }
}

transfer() {
    preflight
    echo "== submitting =="
    # The image is restored alongside the home rather than into it. `_common.py` names a mutable
    # remote tag, and apptainer resolved it through APPTAINER_CACHEDIR to a path outside the home,
    # so nothing in the home carries the image. Point APPTAINER_CACHEDIR at APPTAINER_CACHE after
    # the restore.
    local batch; batch=$(mktemp)
    {
        echo "--recursive $ARCHIVE/pratama2026 $HOME_PATH"
        echo "--recursive $ARCHIVE/deps $APPTAINER_CACHE"
    } > "$batch"
    cat "$batch"
    "$GLOBUS" transfer --batch "$batch" "$CHINOOK" "$FIR" \
        --verify-checksum --preserve-timestamp --sync-level checksum --notify off \
        --label "E3 restore pratama2026 $(date +%F)"
    rm -f "$batch"
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
    # CAUTION The manifest at $ARCHIVE/meta/manifest_full.tsv.gz is not the archive. It holds
    # 145,220 entries and 3,832,855,343,605 bytes, but the archive drops `.staging` (21 entries,
    # 55,992,517,500 bytes) and every symlink (3,511, because the transfer ran with
    # recursive_symlinks=ignore), and adds results/_metadata (12 entries, 230,472,594 bytes) and
    # relay/msm_relay (1,799,672 bytes). RESUME.md carries the arithmetic.
    echo "expected 141701 paths and 3,777,095,098,371 bytes"
    ssh fir "find '$HOME_PATH' | wc -l"
    ssh fir "du -sb '$HOME_PATH'"

    echo "== the relay executable survived the relay/ exclusion =="
    ssh fir "test -x '$HOME_PATH/metasmith/relay/msm_relay' && echo ok || echo 'MISSING -- re-extract with deploy_from_container'"

    echo "== the image is where APPTAINER_CACHEDIR will look =="
    ssh fir "ls -l '$APPTAINER_CACHE/docker..quay.io_hallamlab_metasmith..0.22.1.sif'"

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
