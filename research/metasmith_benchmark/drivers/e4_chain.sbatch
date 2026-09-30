#!/bin/bash
# Run a range of E4 GEM chunks back to back, each lane of a chunk as submit_driver.sbatch's two
# phases, then archive the lane's products and give its inodes back, stopping the moment anything
# looks wrong.
#
#   sbatch <checkout>/research/metasmith_benchmark/drivers/e4_chain.sbatch <checkout> <lanes> <first> <last>
#
# <lanes> is a comma list run in order within each chunk, e.g. repro,modern.
#
# Thresholds and knobs, overridable at submit time (sbatch --export=ALL,INODE_GATE=...):
#   CHUNK_SIZE    bins per chunk, passed to e4_gems.py --chunk-size (default 2015, 7 chunks of 14,105)
#   INODE_GATE    do not START a lane at or above this inode count (default 900000)
#   INODE_STOP    USR1 the live driver at or above this count (default 970000, the campaign's stop)
#   BYTE_STOP_TIB USR1 the live driver at or above this many TiB (default 17.885, the campaign's 96%)
#   POLL_SECONDS  seconds between polls while a job runs (default 120)
#   IGNORE_TOLERANCE  ignored steps one lane of one chunk may lose before the chain stops
#                 (default 1% of CHUNK_SIZE). CarveMe stops at its 6 h cap, and MEMOTE 0.9.13 at
#                 its 4 h second attempt, on a few hard bins.
#   RENEW         1 to resubmit a successor for the unfinished range before this job's wall (default 1)
#
# RESUMING. Phase 2 writes <archive>/<lane>/chunk<N>.launched. A rerun over a launched lane waits
# on its driver while that is live, then goes to the gate and archive. One whose run died stops.
#
# STAGGERING. Two chains may run disjoint chunk ranges in the one home at once: each run has its
# own stage and PID.lock, and the cache takes concurrent writers. Each chain guards the quota for
# its own driver only, so size INODE_STOP for both lanes' peaks together.
#
# WHY IT ARCHIVES. The two lanes leave 51 inodes per bin in task_cache, ~720K for 14,105 bins,
# against ~330K of room. So each finished lane is tarred with its lineage index, and then its cache
# entries and its stage are removed. A later plan cannot reuse those entries. Tony chose this
# (2026-09-22). The archive is the only copy of the products once a lane is reclaimed.
#
# IT IS ALSO ITS OWN QUOTA GUARD. quota_stop.sbatch takes a fixed list of driver job ids, so it
# cannot guard a chunk that has not launched yet. This script reads the quota on every poll and
# sends the live driver a USR1 on a breach.
#
# CAUTION a driver may ONLY be stopped with `scancel --batch --signal=USR1 <jobid>`. A plain scancel
# skips nextflow's shutdown hook and orphans the grid jobs. This script issues USR1 only.
#
# CAUTION `--materialise`, `--launch` and `--stage-only` are ONE mutually exclusive argparse group.
# Phase 2 is `--launch` with NEITHER `--materialise` NOR `--import`.
#
# CAUTION one importing job per agent home at a time, because concurrent imports lock the pool's
# sqlite. A chain holds its phase 1 until no other e4g_*_prep job is queued, which serialises two
# chains only when their starts are staggered: two checks in the same poll can both pass.
#
# This does NOT sync. Run sync.sh by hand before submitting if the checkout changed, and never into a
# home that holds a live driver.
#SBATCH --account=rpp-shallam
#SBATCH --cpus-per-task=4
#SBATCH --mem=4G
#SBATCH --time=24:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/e4_chain.%j.out
set -euo pipefail

if [ "$#" -ne 4 ]; then
    echo "usage: sbatch e4_chain.sbatch <checkout> <lanes> <first chunk> <last chunk>" >&2
    exit 1
fi
CHECKOUT="$1"
LANES="$2"
FIRST="$3"
LAST="$4"

CHUNK_SIZE="${CHUNK_SIZE:-2015}"
INODE_GATE="${INODE_GATE:-900000}"
INODE_STOP="${INODE_STOP:-970000}"
BYTE_STOP_TIB="${BYTE_STOP_TIB:-17.885}"
POLL_SECONDS="${POLL_SECONDS:-120}"
IGNORE_TOLERANCE="${IGNORE_TOLERANCE:-$(( CHUNK_SIZE / 100 ))}"
RENEW="${RENEW:-1}"
RENEW_AFTER="${RENEW_AFTER:-54000}"   # 15 h of the 24 h wall: one repro lane can take 8 h
ignored_total=0

DRIVERS="$CHECKOUT/research/metasmith_benchmark/drivers"
SUBMIT="$DRIVERS/submit_driver.sbatch"
HOME_DIR="/scratch/phyberos/metagem/metasmith"
CACHE_DB="$HOME_DIR/task_cache/cache.sqlite"
ARCHIVE="/scratch/phyberos/metagem/e4_gems_archive"
LOGS="/scratch/phyberos/bench/logs"

declare -A STEPS=([repro]=4 [modern]=2)
declare -A FINAL=([repro]="modelling-carveme_model_cplex modelling-memote_results"
                  [modern]="modelling-carveme_model modelling-memote_score")

log() { printf '%(%Y-%m-%dT%H:%M:%S)T %s\n' -1 "$*"; }
die() { log "STOPPING: $*"; exit 1; }
alive() { squeue -h -j "$1" >/dev/null 2>&1; }

# One quota reading, or failure on an unparseable one. `lfs` marks an over-quota field with a
# trailing '*' and a transient MDT error exits 5, so a bad read must never pass as "under threshold".
read_quota() {
    local reading kb inodes
    reading="$( { lfs quota -p 83115734 /scratch 2>/dev/null | tail -1; } || true)"
    kb="$(awk '{print $2}' <<<"$reading")" || true
    inodes="$(awk '{print $6}' <<<"$reading")" || true
    [[ "$kb" =~ ^[0-9]+$ && "$inodes" =~ ^[0-9]+$ ]] || return 1
    QUOTA_INODES="$inodes"
    QUOTA_TIB="$(awk -v k="$kb" 'BEGIN{printf "%.3f", k/2^30}')"
}

# sacct is the authority on how a job ended. squeue forgets it the moment it leaves the queue.
final_state() { sacct -X -j "$1" -n -o State 2>/dev/null | head -1 | tr -d ' '; }
exit_code() { sacct -X -j "$1" -n -o ExitCode 2>/dev/null | head -1 | tr -d ' '; }

# Wait for a job to leave the queue, reading the quota on each poll. On a breach, USR1 the
# `guard_target` driver if one is named, then stop the chain.
wait_for() {
    local job="$1" guard_target="${2:-}" label="$3"
    while alive "$job"; do
        if read_quota; then
            log "$label: quota ${QUOTA_TIB} TiB, ${QUOTA_INODES} inodes (stop at ${BYTE_STOP_TIB} TiB / ${INODE_STOP})"
            local breach=""
            if (( QUOTA_INODES >= INODE_STOP )); then
                breach="inodes ${QUOTA_INODES} >= ${INODE_STOP}"
            elif (( $(awk -v a="$QUOTA_TIB" -v b="$BYTE_STOP_TIB" 'BEGIN{print (a>=b)}') )); then
                breach="bytes ${QUOTA_TIB} TiB >= ${BYTE_STOP_TIB} TiB"
            fi
            if [ -n "$breach" ]; then
                log "BREACH: $breach"
                if [ -n "$guard_target" ] && alive "$guard_target"; then
                    scancel --batch --signal=USR1 "$guard_target"
                    log "signalled USR1 to driver $guard_target"
                fi
                die "quota breach -- the chain stops here, nothing further is submitted"
            fi
        else
            log "$label: UNPARSEABLE quota reading -- continuing, will retry next poll"
        fi
        sleep "$POLL_SECONDS"
    done
}

run_job() {
    local label="$1"; shift
    local job
    job="$(sbatch --parsable "$@")" || die "sbatch refused $label"
    log "$label submitted as $job"
    wait_for "$job" "" "$label"
    [ "$(final_state "$job")" = "COMPLETED" ] || die "$label $job ended $(final_state "$job") (exit $(exit_code "$job"))"
    log "$label $job COMPLETED"
}

renew() {
    local next="$1" me
    me="$(scontrol show job "${SLURM_JOB_ID:-}" 2>/dev/null | tr ' ' '\n' | sed -n 's/^Command=//p' | head -1)" || true
    if [ -z "$me" ] || [ ! -f "$me" ]; then
        log "RENEW FAILED: cannot resolve this script's path (got '${me:-}') -- chunks ${next}..${LAST} are NOT queued, resubmit by hand"
        return 1
    fi
    sbatch --export="ALL,CHUNK_SIZE=$CHUNK_SIZE,INODE_GATE=$INODE_GATE,INODE_STOP=$INODE_STOP,BYTE_STOP_TIB=$BYTE_STOP_TIB,POLL_SECONDS=$POLL_SECONDS,IGNORE_TOLERANCE=$IGNORE_TOLERANCE,RENEW=$RENEW" \
        "$me" "$CHECKOUT" "$LANES" "$next" "$LAST" \
        && log "renewed: successor submitted for chunks ${next}..${LAST}" \
        || { log "RENEW FAILED: sbatch refused -- chunks ${next}..${LAST} are NOT queued"; return 1; }
}

run_lane() {
    local lane="$1" chunk="$2"
    local tag="e4g_${lane}_c${chunk}" steps="${STEPS[$lane]}"
    local done_file="$ARCHIVE/$lane/chunk${chunk}.done"
    if [ -f "$done_file" ]; then
        log "$tag already archived ($(cat "$done_file")), skipping"
        return 0
    fi
    log "=== $tag ==="
    mkdir -p "$ARCHIVE/$lane"

    # A launched lane resumes: it waits on its driver if that is still live, then goes to the gate.
    # Re-running phases 1 and 2 would stage a second run beside the first.
    local launched_file="$ARCHIVE/$lane/chunk${chunk}.launched"
    local key nbins p1log p2log p2
    if [ -f "$launched_file" ]; then
        read -r key nbins p1log p2log < "$launched_file"
        p2="$(basename "$p2log" .out)"
        p2="${p2##*.}"
        if alive "$p2"; then
            log "$tag resuming on live driver $p2 (run $key)"
            wait_for "$p2" "$p2" "$tag phase 2"
            [ "$(final_state "$p2")" = "COMPLETED" ] || die "$tag driver $p2 ended $(final_state "$p2") (exit $(exit_code "$p2"))"
        fi
        grep -q 'Execution complete' "$HOME_DIR/runs/$key/_metasmith/logs.latest/nxf.log" 2>/dev/null \
            || die "$tag was launched as $key but its run did not finish -- look before re-running"
        log "$tag resuming at the gate: run $key finished, $nbins bins"
        archive_lane "$lane" "$chunk" "$tag" "$key" "$nbins" "$p1log" "$p2log"
        return 0
    fi

    while squeue -u "$USER" -h -o '%j' | grep -qE '^e4g_[a-z]+_c[0-9]+_prep$'; do
        log "$tag waiting: another lane is importing"
        sleep "$POLL_SECONDS"
    done

    read_quota || die "cannot read the quota, refusing to start $tag"
    log "pre-flight quota: ${QUOTA_TIB} TiB, ${QUOTA_INODES} inodes (gate at ${INODE_GATE})"
    (( QUOTA_INODES < INODE_GATE )) || die "inodes ${QUOTA_INODES} >= INODE_GATE ${INODE_GATE} -- $tag is NOT started"
    (( $(awk -v a="$QUOTA_TIB" -v b="$BYTE_STOP_TIB" 'BEGIN{print (a<b)}') )) || die "bytes ${QUOTA_TIB} TiB >= ${BYTE_STOP_TIB} -- $tag is NOT started"

    # --- phase 1: materialise images and import the chunk's bins into the pool ---
    local p1 staged got skipped
    p1="$(sbatch --parsable -J "${tag}_prep" "$SUBMIT" "$CHECKOUT" e4_gems.py \
            run --lane "$lane" --chunk "$chunk" --chunk-size "$CHUNK_SIZE" --materialise --import --tag "$tag")" \
        || die "sbatch refused phase 1 for $tag"
    log "phase 1 submitted as $p1"
    wait_for "$p1" "" "$tag phase 1"
    [ "$(final_state "$p1")" = "COMPLETED" ] || die "$tag phase 1 ended $(final_state "$p1") (exit $(exit_code "$p1"))"

    # The plan key is read from the log, never assumed: the given set is part of the digest.
    p1log="$LOGS/${tag}_prep.${p1}.out"
    [ -f "$p1log" ] || die "$tag phase 1 log missing at $p1log"
    key="$(grep -oE "Plan OK -- ${steps} steps, key=[A-Za-z0-9]+" "$p1log" | sed 's/.*key=//' | sort -u)" || true
    [ "$(printf '%s\n' "$key" | grep -c .)" = "1" ] || die "$tag phase 1 log gave plan keys '$key', expected exactly 1 with ${steps} steps"
    log "$tag plan key: $key"

    staged="$(grep -oE "staging \[e4_gems_inputs_chunk${chunk}of${CHUNK_SIZE}\.xgdb\] as \[[0-9]+\] of \[[0-9]+\] entries; \[[0-9]+\] skipped" "$p1log" | tail -1)" || true
    [ -n "$staged" ] || die "$tag phase 1 log has no staging line for its inputs"
    got="$(sed -E 's/.*as \[([0-9]+)\] of \[([0-9]+)\].*/\1 \2/' <<<"$staged")"
    skipped="$(sed -E 's/.*\[([0-9]+)\] skipped.*/\1/' <<<"$staged")"
    [ "${got% *}" = "${got#* }" ] && [ "$skipped" = "0" ] \
        || die "$tag staged ${got% *} of ${got#* } bins with $skipped skipped -- expected all of them"
    nbins="${got% *}"
    log "$tag inputs: $nbins bins"

    # --- phase 2: launch. --launch takes NEITHER --materialise NOR --import ---
    p2="$(sbatch --parsable -J "$tag" "$SUBMIT" "$CHECKOUT" e4_gems.py \
            run --lane "$lane" --chunk "$chunk" --chunk-size "$CHUNK_SIZE" --launch --tag "$tag")" \
        || die "sbatch refused phase 2 for $tag"
    log "phase 2 submitted as $p2"
    p2log="$LOGS/${tag}.${p2}.out"
    printf '%s %s %s %s\n' "$key" "$nbins" "$p1log" "$p2log" > "$launched_file"
    wait_for "$p2" "$p2" "$tag phase 2"
    [ "$(final_state "$p2")" = "COMPLETED" ] || die "$tag driver $p2 ended $(final_state "$p2") (exit $(exit_code "$p2"))"
    log "$tag driver $p2 COMPLETED"

    archive_lane "$lane" "$chunk" "$tag" "$key" "$nbins" "$p1log" "$p2log"
}

archive_lane() {
    local lane="$1" chunk="$2" tag="$3" key="$4" nbins="$5" p1log="$6" p2log="$7"
    local stage="$HOME_DIR/runs/$key" done_file="$ARCHIVE/$lane/chunk${chunk}.done" d n ign

    # Nextflow reports ignored tasks in the run's own log, not in the driver's.
    ign="$(grep -c 'Error is ignored' "$stage/_metasmith/logs.latest/nxf.log" 2>/dev/null || true)"
    ignored_total=$(( ignored_total + ${ign:-0} ))
    log "$tag run $key: ${ign:-0} ignored steps (running total ${ignored_total})"
    (( ${ign:-0} <= IGNORE_TOLERANCE )) \
        || die "$tag lost ${ign} steps, above IGNORE_TOLERANCE ${IGNORE_TOLERANCE} -- a systematic fault, look before chaining further"

    # --- gate: every bin has its products in results/, less the ignored ones ---
    [ -d "$stage/results" ] || die "$tag has no results/ at $stage"
    [ -f "$stage/results/_metadata/index.yml" ] || die "$tag results/ has no lineage index -- NOT archiving"
    for d in ${FINAL[$lane]}; do
        n="$(find "$stage/results/$d" -maxdepth 1 -type f -name '1-*' 2>/dev/null | wc -l)"
        log "$tag $d: $n of $nbins"
        (( n <= nbins && n >= nbins - ${ign:-0} )) || die "$tag $d holds $n products for $nbins bins with ${ign:-0} ignored -- NOT archiving"
    done

    # --- archive: results/ with its lineage index, and the files that say how the run was made ---
    local out="$ARCHIVE/$lane/chunk${chunk}.${key}.tar.zst" members sources
    ( cd "$stage" && tar -cf - results workflow.nf workflow.config.nf workflow.resources.nf workflow.params.yml \
        workflow.lineage_of_given.json workflow.dag.svg workflow.step_*.meta ) \
        | zstd -3 -T"${SLURM_CPUS_PER_TASK:-4}" -q -o "$out" || die "$tag archive failed"
    members="$(zstd -dc "$out" | tar -tf - | wc -l)"
    sources="$(cd "$stage" && find results workflow.nf workflow.config.nf workflow.resources.nf workflow.params.yml \
        workflow.lineage_of_given.json workflow.dag.svg workflow.step_*.meta | wc -l)"
    [ "$members" = "$sources" ] || die "$tag archive holds $members members for $sources sources -- NOT reclaiming"
    cp "$p1log" "$p2log" "$ARCHIVE/$lane/"
    log "$tag archived to $out: $members members, $(du -b "$out" | cut -f1) B"

    # --- reclaim: this lane's cache entries, then its stage. Imports stay: both lanes read them ---
    local keys="$ARCHIVE/$lane/chunk${chunk}.${key}.keys" left
    sqlite3 "$CACHE_DB" "select lower(hex(key)) from entries where run='$key' and origin != 'imported';" > "$keys" \
        || die "cannot list $key's cache entries -- NOT reclaiming"
    log "$tag evicting $(wc -l < "$keys") cache entries"
    run_job "$tag evict" "$DRIVERS/evict_cache.sbatch" "$CHECKOUT" "$HOME_DIR" "$keys" --apply
    left="$(sqlite3 "$CACHE_DB" "select count(*) from entries where run='$key' and origin != 'imported';")"
    [ "$left" = "0" ] || die "$tag still has $left cache entries under $key after eviction"
    run_job "$tag delete stage" "$DRIVERS/delete_stage.sbatch" "$stage" "$HOME_DIR/task_cache"

    printf 'key=%s bins=%s ignored=%s archive=%s\n' "$key" "$nbins" "${ign:-0}" "$out" > "$done_file"
    read_quota && log "$tag done, quota now ${QUOTA_TIB} TiB, ${QUOTA_INODES} inodes"
}

[ -f "$SUBMIT" ] || die "no submit_driver.sbatch at $SUBMIT"
[ -d "$HOME_DIR" ] || die "no agent home at $HOME_DIR"
for lane in ${LANES//,/ }; do
    [ -n "${STEPS[$lane]:-}" ] || die "unknown lane $lane"
done

log "e4_chain starting: chunks ${FIRST}..${LAST} of ${CHUNK_SIZE} bins, lanes ${LANES}, from $CHECKOUT"
log "INODE_GATE=$INODE_GATE INODE_STOP=$INODE_STOP BYTE_STOP_TIB=$BYTE_STOP_TIB IGNORE_TOLERANCE=$IGNORE_TOLERANCE POLL=${POLL_SECONDS}s RENEW=$RENEW"

# A successor restarts at this chunk, and the .done files skip the lanes already archived.
for (( chunk=FIRST; chunk<=LAST; chunk++ )); do
    for lane in ${LANES//,/ }; do
        if [ "$RENEW" = "1" ] && [ "$SECONDS" -ge "$RENEW_AFTER" ]; then
            log "approaching wall at ${SECONDS}s with chunks ${chunk}..${LAST} left, renewing"
            renew "$chunk" || true
            exit 0
        fi
        run_lane "$lane" "$chunk"
    done
done

log "e4_chain finished: chunks ${FIRST}..${LAST}, lanes ${LANES}, all archived"
