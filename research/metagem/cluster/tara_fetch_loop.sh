#!/bin/bash
# Bounded retry driver for the Tara Oceans (sunagawa2015) .sra pull.
#
# fetch_sra.sh's own internal loop is bounded by MINUTES and dies there --
# nothing restarts it. Measured rate (this session, 2026-09-12): ~1.2 runs/min
# at width 16, so 246 runs projects to ~200 minutes, past any single pass's
# 175-180 minute bound. This wraps repeated bounded passes so the fetch drives
# itself to completion (or to an honest stop) with nobody watching.
#
#   nohup setsid timeout -k 30 <total_seconds> ./tara_fetch_loop.sh > log 2>&1 < /dev/null &
#
# The loop's own outer timeout (from the launch idiom above) is the backstop;
# MAX_PASSES below is the primary bound and should finish well inside it.
set -uo pipefail

ROOT=${METAGEM_ROOT:-/scratch/phyberos/metagem}
DS=sunagawa2015
WIDTH=${TARA_WIDTH:-16}
PASS_MINUTES=${TARA_PASS_MINUTES:-170}   # fetch_sra.sh's own internal budget
PASS_TIMEOUT=$(( (PASS_MINUTES + 8) * 60 ))  # outer hard kill, margin over the internal one
MAX_PASSES=${TARA_MAX_PASSES:-5}
LOG=$ROOT/_logs/tara_loop.log
TABLE=$ROOT/runs.$DS.tsv
SRA=$ROOT/.sra/$DS

mkdir -p "$ROOT/_logs" "$SRA" "$ROOT/.sra_staging/$DS"

log() { echo "[$(date -Is)] $*" >> "$LOG"; }

want=$(( $(wc -l < "$TABLE") - 1 ))
count_done() { ls "$SRA" 2>/dev/null | wc -l; }

# Raw CAMI archives are the cheapest thing on scratch to sacrifice -- a
# Globus re-fetch from chinook lands in ~20 minutes (measured this session:
# 1.263 TB at 1.14 GB/s). Reclaim them, and only them, if headroom is tight.
# Literal paths, no variable-led rm -rf.
reclaim_cami_if_tight() {
    local report line used_gib total_tib total_gib free_gib used_files_k total_files_k free_files_k
    report=$(diskusage_report 2>&1)
    line=$(echo "$report" | grep '/scratch (user')
    log "headroom: $line"
    # Parse "6731GiB/  19TiB" and "374K/1000K" off the scratch line.
    used_gib=$(echo "$line" | grep -oE '[0-9]+GiB' | head -1 | tr -d 'GiB')
    total_tib=$(echo "$line" | grep -oE '[0-9]+TiB' | head -1 | tr -d 'TiB')
    used_files_k=$(echo "$line" | grep -oE '[0-9]+K/' | head -1 | tr -d 'K/')
    total_files_k=$(echo "$line" | grep -oE '/[0-9]+K' | head -1 | tr -d '/K')
    if [ -z "$used_gib" ] || [ -z "$total_tib" ] || [ -z "$used_files_k" ] || [ -z "$total_files_k" ]; then
        log "could not parse diskusage_report, skipping headroom check this pass"
        return
    fi
    total_gib=$(( total_tib * 1024 ))
    free_gib=$(( total_gib - used_gib ))
    free_files_k=$(( total_files_k - used_files_k ))
    log "free: ${free_gib} GiB, ${free_files_k}K files"
    if [ "$free_gib" -lt 1500 ] || [ "$free_files_k" -lt 50 ]; then
        if [ -d /scratch/phyberos/cami/cami2_challenge ] || [ -d /scratch/phyberos/cami/cami3_toy_humangut ]; then
            log "headroom tight (${free_gib} GiB / ${free_files_k}K files free) -- reclaiming raw CAMI archives"
            rm -rf /scratch/phyberos/cami/cami2_challenge/marine/long_read
            rm -rf /scratch/phyberos/cami/cami2_challenge/strain/long_read
            rm -rf /scratch/phyberos/cami/cami2_challenge/plant_associated/long_read_nano
            rm -rf /scratch/phyberos/cami/cami2_challenge/plant_associated/long_read_pacbio
            rm -rf /scratch/phyberos/cami/cami3_toy_humangut/long
            rm -rf /scratch/phyberos/cami/cami3_toy_humangut/short
            log "reclaimed raw CAMI long-read + CAMI III archives (unpacked work/ output is untouched; re-fetch via research/cami/cluster/globus_batch_longreads.txt, ~20 min)"
        else
            log "headroom tight but raw CAMI archives already gone -- nothing left to reclaim"
        fi
    fi
}

prev=$(count_done)
log "loop start: target=$want runs, baseline=$prev staged"

if [ "$prev" -ge "$want" ]; then
    log "already complete ($prev/$want), nothing to do"
    exit 0
fi

for pass in $(seq 1 "$MAX_PASSES"); do
    reclaim_cami_if_tight
    log "pass $pass: launching fetch_sra.sh (width=$WIDTH, minutes=$PASS_MINUTES, outer timeout=${PASS_TIMEOUT}s)"
    timeout -k 30 "$PASS_TIMEOUT" "$ROOT/fetch_sra.sh" "$DS" "$WIDTH" "$PASS_MINUTES" >> "$LOG" 2>&1

    cur=$(count_done)
    log "pass $pass result: $cur/$want runs staged"

    if [ "$cur" -ge "$want" ]; then
        log "all $want runs staged, loop complete"
        exit 0
    fi
    if [ "$cur" -eq "$prev" ]; then
        log "STALL: pass $pass verified zero new runs ($cur/$want) -- stopping rather than retrying identically"
        exit 1
    fi
    prev=$cur
done

log "MAX_PASSES=$MAX_PASSES reached with $prev/$want staged -- stopping, someone should look"
exit 2
