#!/bin/bash
# Run a range of E4 chunks back to back, one at a time, each as submit_driver.sbatch's documented
# two phases, stopping the moment anything looks wrong.
#
#   sbatch <checkout>/research/metasmith_benchmark/drivers/e4_chain.sbatch <checkout> <first> <last>
#
# Thresholds and knobs, overridable at submit time (sbatch --export=ALL,INODE_GATE=...):
#   INODE_GATE    do not START a chunk at or above this inode count (default 900000)
#   INODE_STOP    USR1 the live driver at or above this count (default 970000, the campaign's stop)
#   BYTE_STOP_TIB USR1 the live driver at or above this many TiB (default 17.885)
#   POLL_SECONDS  seconds between polls while a phase runs (default 120)
#   IGNORE_TOLERANCE  ignored steps a single chunk may lose before the chain stops (default 5).
#                 A few are EXPECTED: CarveMe's CPLEX gapfill exhausts its rungs on a few hard MAGs,
#                 about one per 500-MAG chunk. Zero would halt the chain nearly every chunk.
#   RENEW         1 to resubmit a successor for the unfinished range before this job's wall (default 1)
#
# WHY THIS EXISTS. Once the benchmark moved off rrg-shallam-ab a 500-MAG chunk stopped being queue
# bound and started landing in well under an hour, so the 23 remaining chunks are about half a day of
# compute but 23 launch cycles of an operator's attention. The operator became the bottleneck, not the
# cluster. This turns the cycle into one submission.
#
# IT IS ALSO ITS OWN QUOTA GUARD, and that is the point rather than a convenience. quota_stop.sbatch
# takes a fixed list of driver job ids, so it cannot know the id of a chunk that has not launched yet;
# chaining chunks under it would leave every chunk after the first unguarded. Since this script is
# already polling while it waits for its driver, it reads the quota on the same poll and sends the
# driver a gated USR1 on a breach. quota_stop stays as it is, guarding whatever else is named to it.
#
# CAUTION a driver may ONLY be stopped with `scancel --batch --signal=USR1 <jobid>`. A plain scancel
# skips nextflow's shutdown hook and orphans the grid jobs -- it cost this campaign 139 orphaned jobs
# once. This script issues USR1 only, and so must anything that replaces it.
#
# CAUTION the two phases are ONE mutually exclusive argparse group: `--materialise`, `--launch` and
# `--stage-only` cannot be combined. Phase 2 is `--launch` with NEITHER `--materialise` NOR `--import`.
# Passing two of them fails the job in about two seconds, which looks like a launch that happened.
#
# CAUTION one importing job per agent home at a time, because concurrent imports lock the pool's
# sqlite. This script is sequential by construction, which is what satisfies that.
#
# This does NOT sync. Run sync.sh by hand before submitting if the checkout changed, and never into a
# home that holds a live driver.
#SBATCH --account=rpp-shallam
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=24:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/e4_chain.%j.out
set -euo pipefail

if [ "$#" -ne 3 ]; then
    echo "usage: sbatch e4_chain.sbatch <checkout> <first chunk> <last chunk>" >&2
    exit 1
fi
CHECKOUT="$1"
FIRST="$2"
LAST="$3"

INODE_GATE="${INODE_GATE:-900000}"
INODE_STOP="${INODE_STOP:-970000}"
BYTE_STOP_TIB="${BYTE_STOP_TIB:-17.885}"
POLL_SECONDS="${POLL_SECONDS:-120}"
# A few ignored steps per chunk are expected, not anomalous -- see the check after phase 2.
IGNORE_TOLERANCE="${IGNORE_TOLERANCE:-5}"
ignored_total=0
RENEW="${RENEW:-1}"
RENEW_AFTER="${RENEW_AFTER:-79200}"   # 22 h of the 24 h wall, leaving room to finish a chunk

SUBMIT="$CHECKOUT/research/metasmith_benchmark/drivers/submit_driver.sbatch"
HOME_DIR="/scratch/phyberos/metagem/metasmith"
CACHE_DB="$HOME_DIR/task_cache/cache.sqlite"
LOGS="/scratch/phyberos/bench/logs"
# MAGs per chunk, and it must match e4_metagem.py's CHUNK_SIZE -- it is used only to say how many
# cache entries a finished chunk should have banked (3 steps x CHUNK_MAGS) before its stage is
# deleted. The last chunk is short, which is why a shortfall warns rather than aborting.
CHUNK_MAGS="${CHUNK_MAGS:-500}"

log() { printf '%(%Y-%m-%dT%H:%M:%S)T %s\n' -1 "$*"; }
die() { log "STOPPING: $*"; exit 1; }
alive() { squeue -h -j "$1" >/dev/null 2>&1; }

# One quota reading, or empty on a failed/unparseable read. `lfs` marks an over-quota field with a
# trailing '*', so demand bare integers -- a bad read must never be mistaken for "under threshold".
read_quota() {
    local reading kb inodes
    reading="$(lfs quota -p 83115734 /scratch 2>/dev/null | tail -1)" || true
    kb="$(awk '{print $2}' <<<"$reading")" || true
    inodes="$(awk '{print $6}' <<<"$reading")" || true
    [[ "$kb" =~ ^[0-9]+$ && "$inodes" =~ ^[0-9]+$ ]] || return 1
    QUOTA_INODES="$inodes"
    QUOTA_TIB="$(awk -v k="$kb" 'BEGIN{printf "%.3f", k/2^30}')"
}

# sacct is the authority on how a job ended. squeue forgets it the moment it leaves the queue.
final_state() {
    sacct -X -j "$1" -n -o State 2>/dev/null | head -1 | tr -d ' '
}
exit_code() {
    sacct -X -j "$1" -n -o ExitCode 2>/dev/null | head -1 | tr -d ' '
}

# Wait for a job to leave the queue. While waiting, read the quota and gate the driver on a breach.
# `guard_target` is the job to USR1 -- the driver for phase 2, and empty for phase 1, whose own
# footprint is an import rather than a run.
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

renew() {
    local next="$1" me
    me="$(scontrol show job "${SLURM_JOB_ID:-}" 2>/dev/null | tr ' ' '\n' | sed -n 's/^Command=//p' | head -1)" || true
    if [ -z "$me" ] || [ ! -f "$me" ]; then
        log "RENEW FAILED: cannot resolve this script's path (got '${me:-}') -- chunks ${next}..${LAST} are NOT queued, resubmit by hand"
        return 1
    fi
    sbatch --export="ALL,INODE_GATE=$INODE_GATE,INODE_STOP=$INODE_STOP,BYTE_STOP_TIB=$BYTE_STOP_TIB,POLL_SECONDS=$POLL_SECONDS,RENEW=$RENEW" \
        "$me" "$CHECKOUT" "$next" "$LAST" \
        && log "renewed: successor submitted for chunks ${next}..${LAST}" \
        || { log "RENEW FAILED: sbatch refused -- chunks ${next}..${LAST} are NOT queued"; return 1; }
}

[ -f "$SUBMIT" ] || die "no submit_driver.sbatch at $SUBMIT"
[ -d "$HOME_DIR" ] || die "no agent home at $HOME_DIR"

log "e4_chain starting: chunks ${FIRST}..${LAST} from $CHECKOUT"
log "INODE_GATE=$INODE_GATE INODE_STOP=$INODE_STOP BYTE_STOP_TIB=$BYTE_STOP_TIB POLL=${POLL_SECONDS}s RENEW=$RENEW"

for (( chunk=FIRST; chunk<=LAST; chunk++ )); do
    # Hand the rest of the range to a successor rather than dying mid-chunk at the wall.
    if [ "$RENEW" = "1" ] && [ "$SECONDS" -ge "$RENEW_AFTER" ]; then
        log "approaching wall at ${SECONDS}s with chunks ${chunk}..${LAST} left, renewing"
        renew "$chunk" || true
        exit 0
    fi

    tag="e4_c${chunk}"
    log "=== chunk $chunk ($tag) ==="

    read_quota || die "cannot read the quota, refusing to start chunk $chunk"
    log "pre-flight quota: ${QUOTA_TIB} TiB, ${QUOTA_INODES} inodes (gate at ${INODE_GATE})"
    if (( QUOTA_INODES >= INODE_GATE )); then
        die "inodes ${QUOTA_INODES} >= INODE_GATE ${INODE_GATE} -- chunk $chunk is NOT started, reclaim first"
    fi

    # A live driver in this home means something else is running; two drivers in one home is not a
    # supported state and a concurrent import would lock the pool's sqlite.
    locks="$(find "$HOME_DIR" -maxdepth 3 -name PID.lock 2>/dev/null | head -5)" || true
    [ -z "$locks" ] || die "the metagem home holds a live driver (PID.lock): $locks"

    # --- phase 1: materialise images and import the chunk's MAGs into the pool ---
    p1="$(sbatch --parsable -J "${tag}_prep" "$SUBMIT" "$CHECKOUT" e4_metagem.py \
            run --chunk "$chunk" --materialise --import --tag "$tag")" \
        || die "sbatch refused phase 1 for chunk $chunk"
    log "phase 1 submitted as $p1"
    wait_for "$p1" "" "chunk $chunk phase 1"

    st="$(final_state "$p1")"; ec="$(exit_code "$p1")"
    [ "$st" = "COMPLETED" ] || die "chunk $chunk phase 1 ended $st (exit $ec), not COMPLETED"
    log "phase 1 $p1 COMPLETED exit $ec"

    # The plan key is read from the job's own log, never assumed: the given set is part of the digest
    # so every chunk has its own key. Demand exactly one, or the log is not saying what we think.
    p1log="$LOGS/${tag}_prep.${p1}.out"
    [ -f "$p1log" ] || die "chunk $chunk phase 1 log missing at $p1log"
    keys="$(grep -oE 'Plan OK -- 3 steps, key=[A-Za-z0-9]+' "$p1log" | sed 's/.*key=//' | sort -u)" || true
    n_keys="$(printf '%s\n' "$keys" | grep -c . || true)"
    [ "$n_keys" = "1" ] || die "chunk $chunk phase 1 log gave $n_keys plan keys ('$keys'), expected exactly 1"
    log "chunk $chunk plan key: $keys"

    # The chunk's own staging line, named by its xgdb so it cannot be confused with the library
    # stagings that follow it -- several of those legitimately read "[0] of [16]", and a tail -1 over
    # every "entries" line picks one of them up and reports it as the chunk's import.
    staged="$(grep -oE "staging \[e4_inputs_chunk${chunk}\.xgdb\] as \[[0-9]+\] of \[[0-9]+\] entries; \[[0-9]+\] skipped" "$p1log" | tail -1)" || true
    [ -n "$staged" ] || die "chunk $chunk phase 1 log has no staging line for e4_inputs_chunk${chunk}.xgdb"
    log "chunk $chunk import: $staged"
    # A subset import silently shrinks the chunk, so require all of it and nothing skipped.
    got="$(sed -E 's/.*as \[([0-9]+)\] of \[([0-9]+)\].*/\1 \2/' <<<"$staged")"
    skipped="$(sed -E 's/.*\[([0-9]+)\] skipped.*/\1/' <<<"$staged")"
    [ "${got% *}" = "${got#* }" ] && [ "$skipped" = "0" ] \
        || die "chunk $chunk imported ${got% *} of ${got#* } entries with $skipped skipped -- expected all of them"

    # --- phase 2: launch. --launch takes NEITHER --materialise NOR --import ---
    p2="$(sbatch --parsable -J "$tag" "$SUBMIT" "$CHECKOUT" e4_metagem.py \
            run --chunk "$chunk" --launch --tag "$tag")" \
        || die "sbatch refused phase 2 for chunk $chunk"
    log "phase 2 submitted as $p2"
    wait_for "$p2" "$p2" "chunk $chunk phase 2"

    st="$(final_state "$p2")"; ec="$(exit_code "$p2")"
    [ "$st" = "COMPLETED" ] || die "chunk $chunk driver $p2 ended $st (exit $ec), not COMPLETED"

    p2log="$LOGS/${tag}.${p2}.out"
    ign="$(grep -c 'Error is ignored' "$p2log" 2>/dev/null || true)"
    err="$(grep -c 'ERROR ~' "$p2log" 2>/dev/null || true)"
    log "chunk $chunk driver $p2 COMPLETED exit $ec, ${err:-0} errors, ${ign:-0} ignored"

    # An ignored step is a silently dropped product, and the campaign's rule is that a retry ladder
    # swallowing a fault is how a short result gets mistaken for a complete one. But a SMALL number
    # of ignores is expected here rather than anomalous: CarveMe's CPLEX gapfill exhausts its rungs
    # on a few hard MAGs (chunk 1 lost 4 of 2,000 that way, ~0.2%, implying roughly one per
    # 500-MAG chunk), so halting on any ignore at all would stop the chain nearly every chunk and
    # give up the unattended running this script exists for.
    #
    # So tolerate the expected rate and stop on anything above it -- the distinction that matters is
    # a few per-MAG losses versus a systematic fault, which shows up as many at once.
    ignored_total=$(( ignored_total + ${ign:-0} ))
    if [ "${ign:-0}" != "0" ]; then
        log "NOTE chunk $chunk lost ${ign} step(s) to the retry ladder; running total ${ignored_total}. Expected cause is CarveMe/CPLEX rung exhaustion on a hard MAG. The MAGs are named in $p2log."
    fi
    if (( ${ign:-0} > IGNORE_TOLERANCE )); then
        die "chunk $chunk finished with ${ign} ignored step(s), above IGNORE_TOLERANCE ${IGNORE_TOLERANCE} -- that is a systematic fault rather than per-MAG loss, look before chaining further"
    fi

    # --- reclaim: a finished chunk's stage is ~22,000 inodes and nothing reclaims it on its own ---
    #
    # This is not housekeeping, it is what makes the range finishable. A completed chunk leaves its
    # whole work tree behind: chunks 5 and 6 measured 22,157 and 21,559 inodes each, ~20,000 of that
    # nxf_work. The campaign's "17.84 inodes/MAG permanent" figure is explicitly *after cleanup*, and
    # cleanup has been a manual delete_stage every time (chunk 1's was job 59918862). Left alone, 23
    # chunks add ~500,000 inodes of dead work trees on top of the real residue and the chain would
    # gate itself out around chunk 10 having banked every product correctly.
    #
    # The gate before deleting is the cache, not the exit code: the run's shards must be indexed in
    # cache.sqlite, because record_run runs at the END of a run and a live run has 0 rows there. If
    # the products are in the cache the stage is a duplicate; if they are not, the stage is the only
    # copy and deleting it destroys the chunk.
    banked="$(sqlite3 "$CACHE_DB" "select count(*) from entries where run='$keys';" 2>/dev/null)" || true
    [[ "$banked" =~ ^[0-9]+$ ]] || die "cannot read the banked count for $keys from $CACHE_DB (got '${banked:-}') -- NOT deleting the stage"
    log "chunk $chunk banked $banked entries under run=$keys (expect $(( 3 * CHUNK_MAGS )) for 3 steps)"
    if [ "$banked" -lt "$(( 3 * CHUNK_MAGS ))" ]; then
        log "WARNING chunk $chunk banked $banked of $(( 3 * CHUNK_MAGS )) expected entries; leaving the stage in place for inspection"
    else
        stage="$HOME_DIR/runs/$keys"
        if [ -d "$stage" ]; then
            ds="$(sbatch --parsable "$CHECKOUT/research/metasmith_benchmark/drivers/delete_stage.sbatch" \
                    "$stage" "$HOME_DIR/task_cache")" \
                || die "sbatch refused delete_stage for $stage"
            log "reclaim submitted as $ds for $stage"
            wait_for "$ds" "" "chunk $chunk reclaim"
            dst="$(final_state "$ds")"
            # A reclaim that ends badly is NOT fatal to the chain: the products are already banked,
            # so the cost is inodes rather than data, and the pre-flight gate on the next chunk is
            # what actually protects the quota. prune_work once ended NODE_FAIL having finished all
            # its work -- an exit state measures how a job ended, not what it did.
            [ "$dst" = "COMPLETED" ] && log "reclaim $ds COMPLETED" \
                || log "WARNING reclaim $ds ended $dst -- check whether it deleted anything before resubmitting; the chunk's products are safe in the cache either way"
        else
            log "no stage at $stage to reclaim"
        fi
    fi

    read_quota && log "chunk $chunk done, quota now ${QUOTA_TIB} TiB, ${QUOTA_INODES} inodes"
done

log "e4_chain finished: chunks ${FIRST}..${LAST} all COMPLETED"
