#!/bin/bash
# Liveness check for a metasmith driver launched with METASMITH_DRIVER_SLURM=1
# (see src/metasmith/agents/workflow_ops.py -- the opt-in Slurm-wrapped launch
# added for T7). Run this on fir, once per run directory, from a systemd user
# timer (`systemctl --user list-timers`) -- not from this workstation, since
# the check has to survive exactly the kind of disconnect it exists to catch.
#
# What "silent" looked like before this existed, reproduced deliberately on
# throwaway run YvTilYr6: scancel the driver's Slurm job mid-run and nothing
# else changes. agent.log stops growing with no `run completed at` / `run
# failed at` line -- the two sentinels runner.py already ends every real run
# with -- and PID.lock / RUN.pgid are left behind on disk, which is exactly
# what would make a naive `[ -e PID.lock ]` check claim the run is still
# alive. squeue/sacct on the recorded job id is the one source that cannot be
# fooled by stale files, because it asks Slurm rather than the filesystem.
#
# Usage: driver_watchdog.sh <run-dir> [<max-silent-seconds>]
# Exit 0: alive, or finished cleanly (a sentinel is present).
# Exit 1: driver's Slurm job is gone and no sentinel appeared -- the silent
#         gap. A DRIVER_LOST marker is written into the run's _metasmith/ dir
#         so the gap is a file on disk, not just a line this script printed
#         and nobody read.
set -euo pipefail

RUN_DIR="${1:?usage: driver_watchdog.sh <run-dir> [<max-silent-seconds>]}"
MAX_SILENT_S="${2:-3600}"

INTERNALS="$RUN_DIR/_metasmith"
JOBID_FILE="$RUN_DIR/RUN.slurmjob"
LOG="$INTERNALS/logs.latest/agent.log"
MARKER="$INTERNALS/DRIVER_LOST"

if [ ! -e "$JOBID_FILE" ]; then
    echo "no RUN.slurmjob at [$RUN_DIR] -- not a Slurm-wrapped driver, nothing to check" >&2
    exit 0
fi
JOBID=$(head -n1 "$JOBID_FILE")

# Sentinel check first: a run that finished (successfully or not) is not a
# liveness problem even if the job has since left the queue.
if [ -e "$LOG" ] && grep -qE "run (completed|failed) at" "$LOG"; then
    [ -e "$MARKER" ] && rm -f "$MARKER"
    echo "driver job [$JOBID] finished with a sentinel in agent.log -- not lost"
    exit 0
fi

STATE=$(squeue -h -j "$JOBID" -o "%T" 2>/dev/null || true)
if [ -n "$STATE" ]; then
    echo "driver job [$JOBID] is [$STATE] -- alive"
    exit 0
fi

# Job left the queue with no sentinel. Grace period: a job that JUST finished
# may not have flushed agent.log's last lines yet. Use the log's own mtime,
# not wall-clock-since-launch, so a run that has been silent a long time
# before this check ever ran is still caught.
NOW=$(date +%s)
if [ -e "$LOG" ]; then
    LAST=$(stat -c %Y "$LOG")
else
    LAST=0
fi
SILENT_S=$((NOW - LAST))
if [ "$SILENT_S" -lt "$MAX_SILENT_S" ]; then
    echo "driver job [$JOBID] left the queue ${SILENT_S}s ago; inside the ${MAX_SILENT_S}s grace window, not yet calling it lost"
    exit 0
fi

SACCT_STATE=$(sacct -n -j "$JOBID" -o State --parsable2 2>/dev/null | head -n1 || echo "unknown")
{
    echo "driver lost at $(date -u +%FT%TZ)"
    echo "run_dir=$RUN_DIR"
    echo "slurm_job=$JOBID"
    echo "slurm_final_state=$SACCT_STATE"
    echo "agent_log_last_mtime=$(date -u -d "@$LAST" +%FT%TZ 2>/dev/null || echo unknown)"
    echo "silent_for_s=$SILENT_S"
} > "$MARKER"
echo "DRIVER LOST: job [$JOBID] (final state [$SACCT_STATE]) is gone and [$LOG]" \
     "carries no completion sentinel after ${SILENT_S}s -- wrote $MARKER" >&2
exit 1
