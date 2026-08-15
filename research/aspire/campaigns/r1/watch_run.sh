#!/bin/bash
# One status pass over a running r1 workflow. Run on fir.
#
# Every count here is deliberate about WHICH POPULATION it is over, because
# this run's recurring failure has been an instrument reporting health while
# measuring something else. Specifically:
#
#   * `squeue -r` expands job arrays. Plain `squeue` collapses a wholly-pending
#     array to ONE line, so counting lines undercounts the work -- badly, when
#     34 tasks show as 1.
#   * Job attribution is by WorkDir, not by name. This SLURM account is shared
#     with other workflows, and names are not unique across them.
#   * `sacct` is consulted, not just the queue: a task that dies before writing
#     an exit code leaves nothing on disk and appears ONLY in accounting.
#   * Products are counted per step against the expected fan-out. The error
#     strategy retries and then IGNORES, and nothing downstream turns an
#     ignored task into a run failure -- so a library can vanish from the
#     results while the run reports success. This count is the only thing that
#     catches that.
#
# A transient zero in running-task counts at a step boundary is NORMAL: it is
# the collector submitting the next array, and it has the same shape as a
# genuine stall. Distinguish them with the nextflow head liveness and the
# nxf.log timestamp, not with the queue count alone.

set -uo pipefail

RUN=${1:?usage: watch_run.sh <run_dir> [n_samples]}
NSAMP=${2:-34}
RUNKEY=$(basename "$RUN")

echo "=========== $RUNKEY  $(date -u '+%Y-%m-%d %H:%M:%SZ') ==========="

# ── head liveness ────────────────────────────────────────────────────────────
head_pid=$(pgrep -u "$USER" -f "nextflow-26.*$RUNKEY" 2>/dev/null | head -1)
[ -z "$head_pid" ] && head_pid=$(pgrep -u "$USER" -f "nextflow-26" 2>/dev/null | head -1)
if [ -n "$head_pid" ]; then
    echo "head       : alive pid=$head_pid elapsed=$(ps -o etime= -p "$head_pid" 2>/dev/null | tr -d ' ')"
else
    echo "head       : NOT RUNNING"
fi

LOG="$RUN/_metasmith/logs.latest/nxf.log"
if [ -f "$LOG" ]; then
    age=$(( $(date +%s) - $(stat -c %Y "$LOG") ))
    echo "nxf.log    : last written ${age}s ago"
    [ "$age" -gt 3600 ] && echo "             ^ WARNING: over an hour with no log activity"
else
    echo "nxf.log    : ABSENT at $LOG (metasmith redirects it here; .nextflow.log is not it)"
fi

# ── queue, attributed by WorkDir ─────────────────────────────────────────────
mine=0; others=0
declare -A BYSTEP BYSTATE
while read -r jid st name; do
    [ -z "$jid" ] && continue
    wd=$(scontrol show job "$jid" 2>/dev/null | grep -oP 'WorkDir=\K\S+')
    case "$wd" in
        "$RUN"*) mine=$((mine+1));
                 s=$(echo "$name" | sed 's/.*__//; s/[^a-zA-Z_].*//')
                 BYSTEP[$s]=$(( ${BYSTEP[$s]:-0} + 1 ))
                 BYSTATE[$st]=$(( ${BYSTATE[$st]:-0} + 1 )) ;;
        *)       others=$((others+1)) ;;
    esac
done < <(squeue -u "$USER" -r -h -o "%i %T %j" 2>/dev/null)

echo "queue      : $mine task(s) for THIS run; $others for other work under the same account"
for s in "${!BYSTEP[@]}"; do printf "             %-28s %s\n" "$s" "${BYSTEP[$s]}"; done
for s in "${!BYSTATE[@]}"; do printf "             [%s] %s\n" "$s" "${BYSTATE[$s]}"; done

# ── launch-time failures: only accounting sees these ─────────────────────────
# Windowed to the CURRENT ATTEMPT's start. sacct has no WorkDir, so an
# unwindowed query is over every job this user ran -- which reported 80 FAILED
# and 66 CANCELLED for a run that had zero failures, because those belonged to a
# previous, killed run under a different key.
#
# The window is the live log directory's mtime, NOT the run directory's creation
# time. A run that is restarted under the same key keeps its directory, so
# dating from the directory silently folds every previous attempt's failures --
# and every job the restart itself cancelled -- into the current attempt's
# tally. metasmith stamps a fresh `logs.<timestamp>` on each start and repoints
# `logs.latest` at it, so that symlink's target is the one thing on disk that
# tracks attempts rather than keys.
# Read the attempt time out of the directory NAME, not its mtime: a directory's
# mtime moves whenever an entry is added, so mtime drifts off the attempt start.
# The name is stamped once. Format is logs.YYYY-MM-DD_HH-MM-SS, in fir's local
# time -- which is what sacct wants, so no conversion.
# SINCE is seeded empty on purpose: `set -u` is on, the `case` below may not
# match, and every fallback here is written as `[ -z "$SINCE" ]`. Without the
# seed an unmatched case aborts the script at the first fallback test -- which
# is how this failed after the previous edit, killing the product and task
# counts that come after it.
SINCE=""
LOGDIR=$(basename "$(readlink -f "$RUN/_metasmith/logs.latest" 2>/dev/null)" 2>/dev/null)
case "$LOGDIR" in
    logs.*) _stamp=${LOGDIR#logs.}
            SINCE=$(date -d "${_stamp%%_*} $(echo "${_stamp#*_}" | tr '-' ':')" \
                    '+%Y-%m-%dT%H:%M:%S' 2>/dev/null) ;;
esac
[ -z "$SINCE" ] && SINCE=$(date -d "@$(stat -c %W "$RUN" 2>/dev/null || stat -c %Y "$RUN")" '+%Y-%m-%dT%H:%M:%S' 2>/dev/null)
[ -z "$SINCE" ] && SINCE=$(date -d '1 day ago' '+%Y-%m-%dT%H:%M:%S')
echo "sacct      : (since $SINCE, this run only)"
sacct -u "$USER" -S "$SINCE" -n -X -o State%-20,JobID%-16 2>/dev/null \
  | awk '{print $1}' | sort | uniq -c | sort -rn | head -8 | sed 's/^/             /'

# ── per-step product counts vs expected fan-out ──────────────────────────────
echo "products   : (per-sample steps should reach $NSAMP)"
if [ -d "$RUN/results" ]; then
    for d in "$RUN"/results/*/; do
        [ -d "$d" ] || continue
        n=$(find "$d" -maxdepth 1 -type f 2>/dev/null | wc -l)
        label=$(basename "$d")
        flag=""
        case "$label" in
            *cluster_table*|*quality_bin*) ;;                       # cross-sample, not $NSAMP
            _metadata) ;;                                          # bookkeeping dir, not a product
            *) [ "$n" -gt 0 ] && [ "$n" -lt "$NSAMP" ] && flag="  <-- INCOMPLETE" ;;
        esac
        printf "             %-46s %4d%s\n" "$label" "$n" "$flag"
    done
else
    echo "             (no results/ yet)"
fi

# ── completed tasks by exit code ─────────────────────────────────────────────
if [ -d "$RUN/nxf_work" ]; then
    # ONE pass. Two separate `find` runs disagree by however many tasks finish
    # between them, which reads as a phantom non-zero exit that then fails to
    # appear in the listing -- observed on the first pass of this very script.
    snap=$(mktemp)
    find "$RUN/nxf_work" -maxdepth 3 -name .exitcode 2>/dev/null \
      | while read -r f; do printf '%s\t%s\n' "$(cat "$f" 2>/dev/null)" "$(dirname "$f")"; done > "$snap"
    all=$(grep -c . "$snap"); all=${all:-0}
    ok=$(awk -F'\t' '$1=="0"' "$snap" | grep -c .); ok=${ok:-0}
    echo "tasks      : $ok/$all completed with exit 0"
    if [ "$all" -gt "$ok" ]; then
        echo "             non-zero exits (the ignore strategy hides these from the run status):"
        awk -F'\t' '$1!="0" && $1!="" {printf "               exit=%s %s\n", $1, $2}' "$snap" | head -10
    fi
    rm -f "$snap"
fi
echo
