#!/usr/bin/env python3
"""Build E2's per-task Slurm runtime/MaxRSS table and per-transform summary.

Reads two raw sacct dumps captured on fir under /scratch/phyberos/bench/evidence/,
committed gzipped in raw/ beside this script. raw/COMMANDS.txt holds the sacct
invocations that produced them.

  e2_window_jobs.psv  -- `sacct -X` allocation rows, 2026-09-09..2026-09-18,
                          every job on the account (not just E2). Carries
                          Start/End/Elapsed/AllocCPUS/ReqMem/WorkDir/State/ExitCode.
  e2_window_steps.psv -- `sacct` (no -X) step rows for the same window. Carries
                          MaxRSS on the `<jobid>.batch` row only; WorkDir is blank
                          on step rows, so the join key is the base JobID.

Only WorkDir ties a job to an E2 run key: E2 job WorkDirs look like
`/scratch/phyberos/cami/metasmith/runs/<run_key>/nxf_work/<hash>`. JobName carries
`nf-p<NN>__<transform>_(<base>)`, where <base> is the 1-based starting sample
offset of that step's Slurm-array submission batch (arrays are capped at 100
elements here, so a 208-sample step submits three batches based at 1, 101, 201).
The array task-id suffix on JobID (e.g. `..._0`, `..._1`) is the offset within
that batch. This lets you reconstruct a 1-based ordinal position within the arm
(base + task-id), but NOT the actual sample name/id: metasmith's per-run
nxf_work directories (and hence .command.sh/.command.out) are already deleted
on fir for all six run keys, and the ordinal-to-sample-name mapping lives only
in the original samplesheet enumeration order, which is not Slurm evidence.
So `sample` is left blank here -- see the run's report for detail.
"""
import csv
import gzip
import re
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
JOBS_PSV = HERE / "raw" / "e2_window_jobs.psv.gz"
STEPS_PSV = HERE / "raw" / "e2_window_steps.psv.gz"
OUT_TASKS = HERE / "e2_slurm_tasks.tsv"
OUT_SUMMARY = HERE / "e2_runtime_by_transform.tsv"

RUN_KEY_ARM = {
    "WfOlaqLT": "short",
    "sxDeVO5L": "short",
    "MjMN02CK": "short",
    "33hlLu8Q": "long",
    "F1yIPPmC": "long",
    "VgUw0A7c": "long",
}
RUN_ROOT = "/scratch/phyberos/cami/metasmith/runs"

JOBNAME_RE = re.compile(r"^nf-p(?P<step>\d+)__(?P<transform>.+)_\((?P<base>\d+)\)$")


def parse_jobname(name):
    m = JOBNAME_RE.match(name)
    if not m:
        return None, None, None
    return f"p{m.group('step')}", m.group("transform"), int(m.group("base"))


def maxrss_to_kb(raw):
    """sacct --units=K MaxRSS is like '11919660K' (or blank)."""
    raw = (raw or "").strip()
    if not raw:
        return None
    if raw.endswith("K"):
        return int(raw[:-1])
    # Fallback: unexpected unit suffix (M/G) -- normalize defensively.
    for suffix, mult in (("M", 1024), ("G", 1024 * 1024)):
        if raw.endswith(suffix):
            return int(float(raw[:-1]) * mult)
    return int(raw)


def load_maxrss_by_base_jobid(path):
    maxrss = {}
    with gzip.open(path, "rt", newline="") as f:
        reader = csv.DictReader(f, delimiter="|")
        for row in reader:
            jobid = row["JobID"]
            if not jobid.endswith(".batch"):
                continue
            base = jobid[: -len(".batch")]
            kb = maxrss_to_kb(row.get("MaxRSS"))
            if kb is not None:
                maxrss[base] = kb
    return maxrss


def elapsed_to_s(elapsed_raw):
    if elapsed_raw in (None, ""):
        return None
    return int(elapsed_raw)


def main():
    maxrss_by_jobid = load_maxrss_by_base_jobid(STEPS_PSV)

    task_rows = []
    with gzip.open(JOBS_PSV, "rt", newline="") as f:
        reader = csv.DictReader(f, delimiter="|")
        for row in reader:
            workdir = row.get("WorkDir") or ""
            matched_key = None
            for key in RUN_KEY_ARM:
                if workdir.startswith(f"{RUN_ROOT}/{key}/"):
                    matched_key = key
                    break
            if matched_key is None:
                continue

            step, transform, base = parse_jobname(row["JobName"])
            jobid = row["JobID"]
            task_id = None
            if "_" in jobid:
                head, _, tail = jobid.rpartition("_")
                if tail.isdigit():
                    task_id = int(tail)
            sample_ordinal = (base + task_id) if (base is not None and task_id is not None) else None

            task_rows.append(
                {
                    "jobid": jobid,
                    "run_key": matched_key,
                    "arm": RUN_KEY_ARM[matched_key],
                    "step": step or "",
                    "transform": transform or row["JobName"],
                    "sample": "",  # not derivable -- see script docstring / report
                    "sample_ordinal_in_batch": sample_ordinal if sample_ordinal is not None else "",
                    "State": row["State"],
                    "ExitCode": row["ExitCode"],
                    "Submit": row["Submit"],
                    "Start": row["Start"],
                    "End": row["End"],
                    "elapsed_s": elapsed_to_s(row.get("ElapsedRaw")),
                    "AllocCPUS": row["AllocCPUS"],
                    "ReqMem": row["ReqMem"],
                    "maxrss_kb": maxrss_by_jobid.get(jobid, ""),
                    "WorkDir": workdir,
                }
            )

    task_cols = [
        "jobid",
        "run_key",
        "arm",
        "step",
        "transform",
        "sample",
        "sample_ordinal_in_batch",
        "State",
        "ExitCode",
        "Submit",
        "Start",
        "End",
        "elapsed_s",
        "AllocCPUS",
        "ReqMem",
        "maxrss_kb",
        "WorkDir",
    ]
    with OUT_TASKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=task_cols, delimiter="\t")
        writer.writeheader()
        for r in task_rows:
            writer.writerow(r)

    # --- per-transform summary, successful attempts only ---
    def is_success(state):
        return state == "COMPLETED"

    groups = {}  # (arm, transform) -> list of rows
    for r in task_rows:
        groups.setdefault((r["arm"], r["transform"]), []).append(r)

    summary_rows = []
    for (arm, transform), rows in sorted(groups.items()):
        ok = [r for r in rows if is_success(r["State"]) and r["elapsed_s"] is not None]
        failed = [r for r in rows if not is_success(r["State"])]

        elapsed_ok = [r["elapsed_s"] for r in ok]
        maxrss_ok = [r["maxrss_kb"] for r in ok if r["maxrss_kb"] != ""]

        summary_rows.append(
            {
                "arm": arm,
                "transform": transform,
                "n_tasks": len(ok),
                "sum_elapsed_s": sum(elapsed_ok) if elapsed_ok else "",
                "median_elapsed_s": int(statistics.median(elapsed_ok)) if elapsed_ok else "",
                "max_elapsed_s": max(elapsed_ok) if elapsed_ok else "",
                "median_maxrss_kb": int(statistics.median(maxrss_ok)) if maxrss_ok else "",
                "max_maxrss_kb": max(maxrss_ok) if maxrss_ok else "",
                "n_failed_attempts": len(failed),
                "sum_failed_elapsed_s": sum(
                    r["elapsed_s"] for r in failed if r["elapsed_s"] is not None
                ),
            }
        )

    summary_cols = [
        "arm",
        "transform",
        "n_tasks",
        "sum_elapsed_s",
        "median_elapsed_s",
        "max_elapsed_s",
        "median_maxrss_kb",
        "max_maxrss_kb",
        "n_failed_attempts",
        "sum_failed_elapsed_s",
    ]
    with OUT_SUMMARY.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=summary_cols, delimiter="\t")
        writer.writeheader()
        for r in summary_rows:
            writer.writerow(r)

    # --- console report ---
    print(f"task rows: {len(task_rows)}")
    by_key = {}
    for r in task_rows:
        by_key[r["run_key"]] = by_key.get(r["run_key"], 0) + 1
    for k, v in sorted(by_key.items()):
        print(f"  {k} ({RUN_KEY_ARM[k]}): {v}")

    n_with_rss = sum(1 for r in task_rows if r["maxrss_kb"] != "")
    print(f"maxrss coverage: {n_with_rss}/{len(task_rows)} rows have a MaxRSS value")

    n_no_step = sum(1 for r in task_rows if not r["step"])
    print(f"rows whose JobName did not parse as nf-p<NN>__<transform>_(<n>): {n_no_step}")
    if n_no_step:
        bad = {}
        for r in task_rows:
            if not r["step"]:
                bad[r["transform"]] = bad.get(r["transform"], 0) + 1
        for name, c in sorted(bad.items(), key=lambda x: -x[1]):
            print(f"    {name!r}: {c}")


if __name__ == "__main__":
    main()
