#!/bin/bash
# Watch the Lustre project quota (project 83115734, /scratch) and gracefully stop one or more
# benchmark drivers before an exceeded quota fails every write and breaks runs mid-flight.
#
#   sbatch <checkout>/research/metasmith_benchmark/drivers/quota_stop.sbatch <driver job id> [more driver job ids...]
#
# Thresholds, overridable at submit time (sbatch --export=ALL,INODE_STOP=...):
#   INODE_STOP    inode count that trips the guard (default 985000 of the 1,000,000 limit)
#   BYTE_STOP_TIB TiB used that trips the guard (default 17.885, the campaign's 96% stop)
#   POLL_SECONDS  seconds between quota reads (default 300)
#   RENEW         1 to resubmit a successor before this job's wall, 0 to run once (default 1)
#
# The byte limit is 20000000000 KB, which is 18.626 TiB, NOT 20 TiB -- `lfs quota` reports KB and
# the campaign quotes the limit in decimal TB. 96% of it is 17.881 TiB, which is where the earlier
# ad-hoc guard tripped at 17.885. A threshold derived by treating the limit as 20 TiB lands above
# the hard limit and the guard can then never fire.
#
# CAUTION INODE_STOP must sit ABOVE E4's planned peak or it aborts a healthy run mid-chunk, and
# that peak is uncomfortably close to the wall: at CHUNK_SIZE=1000 and a 17.84 inodes/MAG residue
# the peak is ~976,886, leaving only ~23,000 inodes under the limit. Confirm the peak before
# arming this against an E4 chunk, and pass INODE_STOP explicitly rather than trusting the default.
#
# CAUTION a driver may ONLY be stopped with `scancel --batch --signal=USR1 <jobid>`. submit_driver.sbatch
# sets --signal=B:USR1@1800 and traps USR1 to forward it to its child, which nextflow's shutdown hook
# turns into a graceful cancel of the grid jobs, keeping everything that finished. A plain scancel skips
# that hook and orphans the grid jobs -- it has already cost this campaign 139 orphaned jobs on one
# occasion. This script issues USR1 only, never a plain scancel, and so must anything that replaces it.
#
# Four prior ad-hoc guards (/scratch/phyberos/_e3_byte_stop.sbatch) each died with the single driver
# job id they were hard-wired to and left the campaign unguarded until someone noticed. This one takes
# many drivers and renews itself across its own wall, so neither a finished driver nor a 24 h limit
# silently disarms it.
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=24:00:00
#SBATCH --output=/scratch/phyberos/bench/logs/quota_stop.%j.out
set -euo pipefail

if [ "$#" -lt 1 ]; then
    echo "usage: sbatch quota_stop.sbatch <driver job id> [more driver job ids...]" >&2
    exit 1
fi
targets=("$@")

INODE_STOP="${INODE_STOP:-985000}"
BYTE_STOP_TIB="${BYTE_STOP_TIB:-17.885}"
POLL_SECONDS="${POLL_SECONDS:-300}"
RENEW="${RENEW:-1}"
RENEW_AFTER="${RENEW_AFTER:-82800}"   # 23 h of the 24 h wall

log() { printf '%(%Y-%m-%dT%H:%M:%S)T %s\n' -1 "$*"; }
alive() { squeue -h -j "$1" >/dev/null 2>&1; }

# The path of this very script, so a successor can be submitted. scontrol reports the submitted
# command line; its first token is the script. sbatch stages a copy elsewhere, so $0 is no use.
self_path() {
    scontrol show job "${SLURM_JOB_ID:-}" 2>/dev/null \
        | tr ' ' '\n' | sed -n 's/^Command=//p' | head -1
}

renew() {
    local me; me="$(self_path)" || true
    if [ -z "$me" ] || [ ! -f "$me" ]; then
        log "RENEW FAILED: cannot resolve this script's path (got '${me:-}') -- THE CAMPAIGN IS UNGUARDED after this job ends, resubmit by hand"
        return 1
    fi
    sbatch --export="ALL,INODE_STOP=$INODE_STOP,BYTE_STOP_TIB=$BYTE_STOP_TIB,POLL_SECONDS=$POLL_SECONDS,RENEW=$RENEW" \
        "$me" "${targets[@]}" \
        && log "renewed: successor submitted for targets ${targets[*]}" \
        || { log "RENEW FAILED: sbatch refused -- THE CAMPAIGN IS UNGUARDED after this job ends"; return 1; }
}

log "quota_stop starting, targets: ${targets[*]} | INODE_STOP=$INODE_STOP BYTE_STOP_TIB=$BYTE_STOP_TIB POLL=${POLL_SECONDS}s RENEW=$RENEW"

prev_inodes=""
prev_at=""
while :; do
    live=()
    for t in "${targets[@]}"; do
        alive "$t" && live+=("$t")
    done
    targets=(${live[@]+"${live[@]}"})
    if [ "${#targets[@]}" -eq 0 ]; then
        log "all targets are gone, standing down"
        exit 0
    fi

    reading="$(lfs quota -p 83115734 /scratch 2>/dev/null | tail -1)" || true
    kb="$(awk '{print $2}' <<<"$reading")" || true
    inodes="$(awk '{print $6}' <<<"$reading")" || true

    # `lfs` marks an over-quota field with a trailing '*', so demand bare integers and treat
    # anything else as a failed read. A bad read must never be mistaken for "under threshold".
    if ! [[ "$kb" =~ ^[0-9]+$ && "$inodes" =~ ^[0-9]+$ ]]; then
        log "UNPARSEABLE quota reading: '$reading' -- guard still alive, continuing"
        sleep "$POLL_SECONDS"
        continue
    fi
    tib="$(awk -v k="$kb" 'BEGIN{printf "%.3f", k/2^30}')"

    # Rate of rise, so the log itself says how much warning the remaining headroom actually buys.
    now="$SECONDS"
    rate="n/a"
    if [ -n "$prev_inodes" ] && [ "$now" -gt "$prev_at" ]; then
        rate="$(awk -v d="$((inodes - prev_inodes))" -v s="$((now - prev_at))" \
            -v room="$((INODE_STOP - inodes))" 'BEGIN{
                r=d*3600.0/s; printf "%+.0f inodes/h", r;
                if (r > 0) printf ", %.1f h to INODE_STOP", room/r;
            }')"
    fi
    prev_inodes="$inodes"; prev_at="$now"

    log "quota: ${tib} TiB, ${inodes} inodes (stop at ${BYTE_STOP_TIB} TiB / ${INODE_STOP}) rate: ${rate} | targets: ${targets[*]}"

    breach=""
    if (( inodes >= INODE_STOP )); then
        breach="inodes ${inodes} >= ${INODE_STOP}"
    elif (( $(awk -v a="$tib" -v b="$BYTE_STOP_TIB" 'BEGIN{print (a>=b)}') )); then
        breach="bytes ${tib} TiB >= ${BYTE_STOP_TIB} TiB"
    fi

    if [ -n "$breach" ]; then
        log "BREACH: $breach -- reading was ${tib} TiB, ${inodes} inodes"
        signalled=()
        for t in "${targets[@]}"; do
            if alive "$t"; then
                scancel --batch --signal=USR1 "$t"
                signalled+=("$t")
            else
                log "$t already gone, not signalling"
            fi
        done
        log "signalled USR1 to: ${signalled[*]:-none}"
        log "guard stopping after breach"
        exit 0
    fi

    # Hand over before the wall rather than dying quietly, and hand over the live target list so
    # the successor does not re-adopt drivers that have already finished.
    if [ "$RENEW" = "1" ] && [ "$SECONDS" -ge "$RENEW_AFTER" ]; then
        log "approaching wall at ${SECONDS}s, renewing"
        renew || true
        exit 0
    fi

    sleep "$POLL_SECONDS"
done
