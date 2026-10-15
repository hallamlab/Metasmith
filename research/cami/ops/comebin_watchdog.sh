#!/bin/bash
# COMEBin's Leiden-sweep watchdog (T7, FIX 2).
#
# The defect (journal 5200c979-c6db-4e56-9f23-d203bb522bcb, section 3): the
# sweep forks a multiprocessing.Pool from a parent that k-means has already
# left threaded through joblib's loky backend. That is a fork-after-threads
# deadlock and it is not deterministic -- 2 of 10 tasks hung at 96 cores in
# the ten-sample run, produced nothing for 10h08m and 8h16m, wrote zero of
# 120 sweep files, and were killed only by the 24h wall: 4,608 core-hours for
# no output, thirty percent of that run.
#
# The real fix is forcing the pool's start method to `spawn` inside
# src/metasmith_libraries/transforms/metagenomics/binning/comebin.py or the
# image -- NOT done here on purpose. A transform's identity hashes its whole
# source file, so editing it retires every cache shard beneath it, and a
# campaign is mid-flight. See the recommendation at the bottom of this file's
# header comment and in the T7 report.
#
# The detector is cheap and already known: a comebin task whose log has
# printed "Start clustering." and which has written no new
# comebin_res/cluster_res/*.tsv file within an hour is hung -- of 120 sweep
# points, a healthy run writes the first few within minutes (16 cores swept
# all 120 in 16m38s; 96 cores did the same in ~8m for 8 of 10 tasks).
#
# This scans one run's nxf_work tree for comebin task directories, and for
# each one past the hang threshold, scancels the Slurm job whose current
# working directory is that task dir (`squeue -o "%A %Z"` -- Nextflow's own
# SLURM executor cd's the job into the task's work dir, which is the only
# handle correlating a work dir back to a job id without touching the
# transform or nextflow's own bookkeeping) and writes a durable marker.
#
# Usage: comebin_watchdog.sh <run-dir> [<hang-threshold-seconds>]
# Safe to run repeatedly (idempotent -- a task already marked hung is not
# re-scanned) and against a run with no comebin tasks yet (no-op).
set -euo pipefail

RUN_DIR="${1:?usage: comebin_watchdog.sh <run-dir> [<hang-threshold-seconds>]}"
HANG_THRESHOLD_S="${2:-3600}"

NXF_WORK="$RUN_DIR/nxf_work"
[ -d "$NXF_WORK" ] || { echo "no nxf_work under [$RUN_DIR], nothing to check"; exit 0; }

NOW=$(date +%s)
FOUND_HUNG=0

# `nxf_work/xx/xxxxxx.../.command.log` is one task's own stdout+stderr.
while IFS= read -r -d '' log; do
    taskdir=$(dirname "$log")
    marker="$taskdir/COMEBIN_KILLED"
    [ -e "$marker" ] && continue  # already handled

    grep -q "Start clustering\." "$log" 2>/dev/null || continue  # not at the sweep yet

    # When clustering started, from the log line's own mtime at first write --
    # not perfect (the file keeps being appended to) but `grep -m1` combined
    # with the *first* appearance is what we want, and stat on the whole file
    # is the closest cheap proxy: if the file has had ANY write (including the
    # sweep's own tsv output, checked next) more recently than the threshold,
    # this task is still making progress and is not yet hung.
    newest_tsv=$(find "$taskdir/comebin_res/cluster_res" -name "*.tsv" -printf "%T@\n" 2>/dev/null | sort -rn | head -1 || true)
    if [ -n "$newest_tsv" ]; then
        last_progress=${newest_tsv%.*}
    else
        # Zero sweep files written yet -- the exact signature from the hung
        # tasks in journal 5200c979. Fall back to the log's own mtime.
        last_progress=$(stat -c %Y "$log")
    fi
    silent_s=$((NOW - last_progress))
    if [ "$silent_s" -lt "$HANG_THRESHOLD_S" ]; then
        continue  # still within the grace window
    fi

    n_tsv=$(find "$taskdir/comebin_res/cluster_res" -name "*.tsv" 2>/dev/null | wc -l)

    # Find the Slurm job whose CWD is this task dir. %Z is squeue's current
    # working directory field -- Nextflow's SLURM executor cd's the submitted
    # job script into the task dir before running .command.run, so this is
    # the one correlation that needs no cooperation from the transform.
    jobid=$(squeue -h -u "$USER" -o "%A %Z" 2>/dev/null | awk -v d="$taskdir" '$2==d {print $1; exit}')

    {
        echo "comebin watchdog kill at $(date -u +%FT%TZ)"
        echo "task_dir=$taskdir"
        echo "slurm_job=${jobid:-unknown}"
        echo "sweep_tsv_files_written=$n_tsv of 120"
        echo "silent_for_s=$silent_s (threshold ${HANG_THRESHOLD_S}s)"
    } > "$marker"

    if [ -n "$jobid" ]; then
        scancel "$jobid" 2>&1 | tee -a "$marker" || true
        echo "COMEBIN WATCHDOG: killed job [$jobid] at [$taskdir] -- $n_tsv/120 sweep files after ${silent_s}s silent" >&2
    else
        echo "COMEBIN WATCHDOG: [$taskdir] looks hung ($n_tsv/120, ${silent_s}s silent) but no matching Slurm job was found (already gone?) -- marker written, no scancel issued" >&2
    fi
    FOUND_HUNG=1
done < <(find "$NXF_WORK" -mindepth 3 -maxdepth 3 -name ".command.log" -print0 2>/dev/null)

exit "$FOUND_HUNG"
