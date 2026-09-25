#!/usr/bin/env python3
"""Build E1's (nf-core/mag) per-task Slurm runtime/MaxRSS table and per-process summary,
measured the same way as E2's build_e2_slurm_tables.py: Slurm Start->End for runtime, the
`.batch` step's MaxRSS for memory.

Reads four kinds of raw evidence, copied into raw/ alongside this script so they persist
independently of the evidence host:

  raw/e1_jobs.psv   -- `sacct -X` allocation rows (Submit/Start/End/Elapsed/AllocCPUS/ReqMem/
                        State/ExitCode/WorkDir), filtered from fir's e2_window_jobs.psv
                        (2026-09-09..2026-09-18, every job on the account) down to WorkDir
                        under /scratch/phyberos/bench/e1/, by WorkDir prefix, never JobName.
  raw/e1_steps.psv  -- matching `.batch` step rows (MaxRSS, --units=K) from the same window's
                        e2_window_steps.psv, filtered to the base JobIDs present in e1_jobs.psv.
  raw/trace.*.tsv   -- E1 SHORT's own nine nextflow execution traces (one per resumed session),
                        copied unmodified from fir:/scratch/phyberos/bench/e1/short/reports/.
                        Each trace's `native_id` column is the Slurm JobID (with array task
                        suffix) that ran it, and `name` is
                        "<colon-delimited nf-core process path> (<tag>)" -- the tag is usually
                        the CAMI sample name, sometimes with an assembler/binner prefix or a
                        "_runN[_raw|_filtered]" suffix. This is the ONLY sample-attribution
                        source used here; E1 LONG has no surviving trace anywhere (checked:
                        neither bench/e1/long/reports/ nor the archived pipeline_info/ carry
                        one -- only params.json and an execution_timeline.html per session), so
                        every E1 long row's sample is left blank rather than guessed.

Process identity for BOTH arms comes from the Slurm JobName alone (reliable for both arms,
independent of the trace): JobName is "nf-<PATH>_(<tag>)", where <PATH> is the underscore-
joined process path exactly as Slurm truncated/rendered it. This script keeps that literal
underscore path as `process_full` -- NOT nextflow's colon-delimited internal name, which was
directly confirmed from the trace for the processes shared with the short arm but was not
going to be guessed for the seven processes (FLYE, the two MINIMAP2 steps, CHOPPER, the two
NANOPLOT steps, PORECHOP_ABI, QUAST_BINS, CONCAT_QUAST_SUMMARY, GUNZIP's long variant) that
appear only in the long arm, where no trace exists to confirm the colon boundaries.

The JobName's own parenthesized tag is NOT sample identity for most early per-sample
processes -- confirmed by inspection, not assumed: e.g. every SHORTREAD_PREPROCESSING_FASTP
JobName on the short arm carries only one of three tags {1, 100, 200} across all 208 samples.
Only the trace's `name` field carries the real sample. Later per-sample steps (COMEBIN,
CHECKM2_PREDICT, DASTOOL, ...) DO carry the real sample in their JobName tag on the short arm,
but this script still sources sample from the trace uniformly, since the trace is confirmed and
covers those steps too.

`e2_transform` is a hand-built lookup (PROCESS_MAP below) from TOOL_FOR_TOOL.md's live tool
tables and E1_E2_COMPARISON.md's process/transform counts -- not from any observed suffix or
heuristic. A process is left unmapped (blank e2_transform) whenever TOOL_FOR_TOOL.md does not
state a corresponding metasmith E2 transform, per that document's own "NF-CORE PROCESSES WITH
NO METASMITH COUNTERPART" list and the absence of PRODIGAL/QUAST from E2's own 15-short/14-long
transform-position count in E1_E2_COMPARISON.md's "Plan shape, counted" section (E2 evidently
never materialises a standalone Slurm task for either, despite TOOL_FOR_TOOL's live table
claiming tool-level parity on gene calling and QUAST being a non-issue by CORRECTION 6).

`run_key` is the nextflow session (its own Slurm driver job id) when attributable:
  - short: the driver job id is literally the trace filename (trace.<jobid>.tsv), joined via
    native_id -> which file the row's JobID was found in.
  - long: E1 long ran in exactly one session, Slurm job 59634610 ("e1_long", COMPLETED,
    2026-09-13T02:39:44 -> 09:52:42, WorkDir /home/phyberos so it never appears among the task
    rows itself) -- confirmed directly in e1_jobs.psv's unfiltered source, so every long task
    row gets that one run_key.
  - the B19 hand-rerun jobs (JobName e1_b19_long/_rows/_merge, WorkDir exactly
    "/scratch/phyberos/bench/e1/long" with no /work/ subpath) are not nextflow at all --
    they are the hand sbatch rerun TOOL_FOR_TOOL.md's Exception B describes. No session id
    applies; run_key is left blank for these.

`sample_ordinal_in_batch` is left blank throughout: unlike E2's fixed 100-wide array-batch
convention, E1's Slurm arrays carry no confirmed base-offset encoding in JobName, and the real
sample (via the trace, on the short arm) is a strictly better identifier than a guessed ordinal
would be.

`final_success` is True on exactly one COMPLETED attempt per (arm, process, sample), and only
where sample is known (never on the long arm, which has no known samples, and never on
sample-less bookkeeping processes like BIN_SUMMARY/MULTIQC). Where a group has more than one
COMPLETED attempt (only possible on the short arm's nine resumed sessions), the attempt with
the latest End timestamp is the final one.
"""
import csv
import re
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
RAW = HERE / "raw"
JOBS_PSV = RAW / "e1_jobs.psv"
STEPS_PSV = RAW / "e1_steps.psv"
OUT_TASKS = HERE / "e1_slurm_tasks.tsv"
OUT_SUMMARY = HERE / "e1_runtime_by_process.tsv"

SHORT_PREFIX = "/scratch/phyberos/bench/e1/short/"
LONG_PREFIX = "/scratch/phyberos/bench/e1/long"
LONG_RUN_KEY = "59634610"  # e1_long driver job, confirmed COMPLETED, WorkDir /home/phyberos

# JobName's "nf-<PATH>_(<tag>)" body, stripped of the "nf-" prefix and the tag -- exactly as
# enumerated from every distinct JobName seen in e1_jobs.psv (25 short + 28 long = 36 unique).
# (process_short, e2_transform) -- e2_transform "" means TOOL_FOR_TOOL.md names no metasmith
# counterpart (or, for PRODIGAL/QUAST, E2 never runs it as a standalone Slurm task either).
PROCESS_MAP = {
    # shared between short and long
    "NFCORE_MAG_MAG_BINNING_COMEBIN_RUNCOMEBIN": ("COMEBIN", "comebin"),
    "NFCORE_MAG_MAG_BINNING_METABAT2_METABAT2": ("METABAT2", "metabat2"),
    "NFCORE_MAG_MAG_BINNING_REFINEMENT_DASTOOL_DASTOOL": ("DASTOOL", "das_tool"),
    "NFCORE_MAG_MAG_BINNING_REFINEMENT_DASTOOL_FASTATOCONTIG2BIN": ("FASTATOCONTIG2BIN", ""),
    "NFCORE_MAG_MAG_BINNING_REFINEMENT_RENAME_POSTDASTOOL": ("RENAME_POSTDASTOOL", ""),
    "NFCORE_MAG_MAG_BINNING_REFINEMENT_RENAME_PREDASTOOL": ("RENAME_PREDASTOOL", ""),
    "NFCORE_MAG_MAG_BINNING_SEMIBIN_SINGLEEASYBIN": ("SEMIBIN_SINGLEEASYBIN", "semibin2"),
    "NFCORE_MAG_MAG_BINNING_SEQKIT_STATS": ("SEQKIT_STATS", ""),
    "NFCORE_MAG_MAG_BINNING_SPLIT_FASTA": ("SPLIT_FASTA", ""),
    "NFCORE_MAG_MAG_BIN_QC_CHECKM2_PREDICT": ("CHECKM2_PREDICT", "checkm2"),
    "NFCORE_MAG_MAG_BIN_QC_CONCAT_CHECKM2_TSV": ("CONCAT_CHECKM2_TSV", ""),
    "NFCORE_MAG_MAG_BIN_SUMMARY": ("BIN_SUMMARY", ""),
    "NFCORE_MAG_MAG_DEPTHS_MAG_DEPTHS": ("MAG_DEPTHS", ""),
    "NFCORE_MAG_MAG_DEPTHS_MAG_DEPTHS_SUMMARY": ("MAG_DEPTHS_SUMMARY", ""),
    "NFCORE_MAG_MAG_MULTIQC": ("MULTIQC", ""),
    "NFCORE_MAG_MAG_PRODIGAL": ("PRODIGAL", ""),
    "NFCORE_MAG_MAG_QUAST": ("QUAST", ""),
    # short-only
    "NFCORE_MAG_MAG_ASSEMBLY_GUNZIP_SHORTREAD_ASSEMBLIES": ("GUNZIP", ""),
    "NFCORE_MAG_MAG_ASSEMBLY_SHORTREAD_ASSEMBLY_MEGAHIT": ("MEGAHIT", "megahit"),
    "NFCORE_MAG_MAG_BINNING_METABAT2_JGISUMMARIZEBAMCONTIGDEPTHS_SHORTREAD": (
        "JGISUMMARIZEBAMCONTIGDEPTHS",
        "metabat2",
    ),
    "NFCORE_MAG_MAG_BINNING_PREPARATION_SHORTREAD_BINNING_PREPARATION_BOWTIE2_ASSEMBLY_ALIGN": (
        "BOWTIE2_ASSEMBLY_ALIGN",
        "bowtie2_binning_bam",
    ),
    "NFCORE_MAG_MAG_BINNING_PREPARATION_SHORTREAD_BINNING_PREPARATION_BOWTIE2_ASSEMBLY_BUILD": (
        "BOWTIE2_ASSEMBLY_BUILD",
        "bowtie2_binning_bam",
    ),
    "NFCORE_MAG_MAG_SHORTREAD_PREPROCESSING_FASTP": ("FASTP", "fastp"),
    "NFCORE_MAG_MAG_SHORTREAD_PREPROCESSING_FASTQC_RAW": ("FASTQC_RAW", "fastqc_raw"),
    "NFCORE_MAG_MAG_SHORTREAD_PREPROCESSING_FASTQC_TRIMMED": ("FASTQC_TRIMMED", "fastqc_trimmed"),
    # long-only
    "NFCORE_MAG_MAG_ASSEMBLY_GUNZIP_LONGREAD_ASSEMBLIES": ("GUNZIP", ""),
    "NFCORE_MAG_MAG_ASSEMBLY_LONGREAD_ASSEMBLY_FLYE": ("FLYE", "flye"),
    "NFCORE_MAG_MAG_BINNING_METABAT2_JGISUMMARIZEBAMCONTIGDEPTHS_LONGREAD": (
        "JGISUMMARIZEBAMCONTIGDEPTHS",
        "metabat2",
    ),
    "NFCORE_MAG_MAG_BINNING_PREPARATION_LONGREAD_BINNING_PREPARATION_MINIMAP2_ASSEMBLY_ALIGN": (
        "MINIMAP2_ASSEMBLY_ALIGN",
        "minimap2_binning_bam",
    ),
    "NFCORE_MAG_MAG_BINNING_PREPARATION_LONGREAD_BINNING_PREPARATION_MINIMAP2_ASSEMBLY_INDEX": (
        "MINIMAP2_ASSEMBLY_INDEX",
        "minimap2_binning_bam",
    ),
    "NFCORE_MAG_MAG_CONCAT_QUAST_SUMMARY": ("CONCAT_QUAST_SUMMARY", ""),
    "NFCORE_MAG_MAG_LONGREAD_PREPROCESSING_CHOPPER": ("CHOPPER", "chopper"),
    "NFCORE_MAG_MAG_LONGREAD_PREPROCESSING_NANOPLOT_FILTERED": ("NANOPLOT_FILTERED", ""),
    "NFCORE_MAG_MAG_LONGREAD_PREPROCESSING_NANOPLOT_RAW": ("NANOPLOT_RAW", ""),
    "NFCORE_MAG_MAG_LONGREAD_PREPROCESSING_PORECHOP_ABI": ("PORECHOP_ABI", "porechop_abi"),
    "NFCORE_MAG_MAG_QUAST_BINS": ("QUAST_BINS", ""),
}

B19_MAP = {
    "e1_b19_long": ("METABAT2_B19_RERUN", "metabat2"),
    "e1_b19_rows": ("B19_ROWS_BOOKKEEPING", ""),
    "e1_b19_merge": ("B19_MERGE_BOOKKEEPING", ""),
}

JOBNAME_RE = re.compile(r"^nf-(?P<path>.+)_\((?P<tag>.*)\)$")
TRACE_NAME_RE = re.compile(r"^(?P<path>\S+) \((?P<tag>.*)\)$")
RUN_SUFFIX_RE = re.compile(r"_run\d+(?:_raw|_filtered)?$")
SAMPLE_RE = re.compile(r"^[A-Za-z0-9_]+_sample_\d+$")


def extract_sample(tag):
    """Recover the bare CAMI sample name from a trace/JobName tag, or "" if it doesn't look
    like one. Confirmed against every process's actual tag shape (see module docstring):
    strip a trailing _runN[_raw|_filtered], then take the last '-'-delimited segment (which
    drops any assembler/binner/"unclassified" prefix nf-core joins onto it with '-')."""
    if not tag:
        return ""
    core = RUN_SUFFIX_RE.sub("", tag)
    seg = core.rsplit("-", 1)[-1]
    return seg if SAMPLE_RE.match(seg) else ""


def maxrss_to_kb(raw):
    raw = (raw or "").strip()
    if not raw:
        return None
    if raw.endswith("K"):
        return int(raw[:-1])
    for suffix, mult in (("M", 1024), ("G", 1024 * 1024)):
        if raw.endswith(suffix):
            return int(float(raw[:-1]) * mult)
    return int(raw)


def load_maxrss_by_base_jobid(path):
    maxrss = {}
    with path.open(newline="") as f:
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


def load_trace_index():
    """native_id -> (run_key, process_path_from_trace, sample, raw_tag). E1 short only.

    raw_tag is the trace's full, un-collapsed tag (e.g. "MEGAHIT-COMEBin-unclassified-
    marine_sample_0"), kept separately from the derived bare `sample` because several
    processes (CHECKM2_PREDICT, SEQKIT_STATS, RENAME_PRE/POSTDASTOOL, MAG_DEPTHS,
    DASTOOL_FASTATOCONTIG2BIN) fan out per bin-set, not per sample: four genuinely different
    tasks for the same sample collapse to the same bare sample name, and deduplicating
    final_success on the bare (process, sample) key alone would wrongly discard three of
    every four as "duplicate attempts". raw_tag disambiguates them; the bare sample is used
    only for the sample column and for grouping when a process really is one-task-per-sample.
    """
    index = {}
    for fp in sorted(RAW.glob("trace.*.tsv")):
        run_key = fp.stem.split(".", 1)[1]
        with fp.open(newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                native_id = row.get("native_id")
                if not native_id or native_id == "-":
                    continue
                m = TRACE_NAME_RE.match(row.get("name", ""))
                if not m:
                    continue
                tag = m.group("tag")
                sample = extract_sample(tag)
                index[native_id] = (run_key, m.group("path"), sample, tag)
    return index


def elapsed_to_s(elapsed_raw):
    if elapsed_raw in (None, ""):
        return None
    return int(elapsed_raw)


def main():
    maxrss_by_jobid = load_maxrss_by_base_jobid(STEPS_PSV)
    trace_index = load_trace_index()

    task_rows = []
    with JOBS_PSV.open(newline="") as f:
        reader = csv.DictReader(f, delimiter="|")
        for row in reader:
            workdir = row.get("WorkDir") or ""
            if workdir.startswith(SHORT_PREFIX):
                arm = "short"
            elif workdir.startswith(LONG_PREFIX):
                arm = "long"
            else:
                continue

            jobid = row["JobID"]
            jobname = row["JobName"]
            dedup_tag = ""

            if jobname in B19_MAP:
                process, e2_transform = B19_MAP[jobname]
                process_full = jobname
                run_key = ""
                sample = ""
            else:
                m = JOBNAME_RE.match(jobname)
                if m:
                    process_full = m.group("path")
                elif jobname.startswith("nf-"):
                    # No parenthesized tag at all (e.g. MAG_DEPTHS_SUMMARY, a once-per-run
                    # bookkeeping step with no per-sample tag to carry).
                    process_full = jobname[len("nf-") :]
                else:
                    process_full = jobname
                if jobname.startswith("nf-"):
                    process, e2_transform = PROCESS_MAP.get(process_full, (process_full, ""))
                    if arm == "short":
                        trace_hit = trace_index.get(jobid)
                        if trace_hit:
                            run_key, _trace_path, sample, dedup_tag = trace_hit
                        else:
                            run_key, sample = "", ""
                    else:
                        run_key, sample = LONG_RUN_KEY, ""
                else:
                    # Not a "nf-*" nextflow task JobName and not a known B19 name either --
                    # not observed in this window, but handled rather than silently mis-typed.
                    process, e2_transform, run_key, sample = jobname, "", "", ""

            task_rows.append(
                {
                    "jobid": jobid,
                    "run_key": run_key,
                    "arm": arm,
                    "step": "",
                    "process": process,
                    "process_full": process_full,
                    "e2_transform": e2_transform,
                    "sample": sample,
                    "_dedup_tag": dedup_tag,
                    "sample_ordinal_in_batch": "",
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
                    "final_success": False,
                }
            )

    # --- final_success: one COMPLETED attempt per (arm, process, sample), sample known only ---
    # Deduplicated on (arm, process, _dedup_tag) rather than the bare sample: several
    # processes fan out per bin-set (CHECKM2_PREDICT, SEQKIT_STATS, RENAME_*, MAG_DEPTHS,
    # DASTOOL_FASTATOCONTIG2BIN), so the same bare sample legitimately has several distinct
    # tasks under one process, and only rows sharing the identical raw tag are true repeat
    # attempts of the SAME task. See load_trace_index's docstring.
    groups = {}
    for r in task_rows:
        if not r["sample"]:
            continue
        groups.setdefault((r["arm"], r["process"], r["_dedup_tag"]), []).append(r)

    n_multi_completed_groups = 0
    for key, rows in groups.items():
        completed = [r for r in rows if r["State"] == "COMPLETED" and r["elapsed_s"] is not None]
        if not completed:
            continue
        if len(completed) > 1:
            n_multi_completed_groups += 1
        finalrow = max(completed, key=lambda r: r["End"])
        finalrow["final_success"] = True

    for r in task_rows:
        del r["_dedup_tag"]

    task_cols = [
        "jobid",
        "run_key",
        "arm",
        "step",
        "process",
        "process_full",
        "e2_transform",
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
        "final_success",
    ]
    with OUT_TASKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=task_cols, delimiter="\t")
        writer.writeheader()
        for r in task_rows:
            writer.writerow(r)

    # --- per-process summary, successful (COMPLETED) attempts only -- same method as E2 ---
    def is_success(state):
        return state == "COMPLETED"

    e2_transform_by_key = {}
    sgroups = {}
    for r in task_rows:
        key = (r["arm"], r["process"])
        sgroups.setdefault(key, []).append(r)
        e2_transform_by_key[key] = r["e2_transform"]

    summary_rows = []
    for (arm, process), rows in sorted(sgroups.items()):
        ok = [r for r in rows if is_success(r["State"]) and r["elapsed_s"] is not None]
        failed = [r for r in rows if not is_success(r["State"])]

        elapsed_ok = [r["elapsed_s"] for r in ok]
        maxrss_ok = [r["maxrss_kb"] for r in ok if r["maxrss_kb"] != ""]

        summary_rows.append(
            {
                "arm": arm,
                "process": process,
                "e2_transform": e2_transform_by_key[(arm, process)],
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
        "process",
        "e2_transform",
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
    by_arm = {}
    for r in task_rows:
        by_arm[r["arm"]] = by_arm.get(r["arm"], 0) + 1
    for k, v in sorted(by_arm.items()):
        print(f"  {k}: {v}")

    n_with_rss = sum(1 for r in task_rows if r["maxrss_kb"] != "")
    print(f"maxrss coverage: {n_with_rss}/{len(task_rows)} rows have a MaxRSS value")
    for arm in ("short", "long"):
        rows = [r for r in task_rows if r["arm"] == arm]
        missing = [r for r in rows if r["maxrss_kb"] == ""]
        print(f"  {arm}: {len(rows) - len(missing)}/{len(rows)}; missing-state census:")
        census = {}
        for r in missing:
            census[r["State"]] = census.get(r["State"], 0) + 1
        for state, c in sorted(census.items(), key=lambda x: -x[1]):
            print(f"    {state}: {c}")

    n_sample_known = sum(1 for r in task_rows if r["sample"])
    print(f"rows with a known sample: {n_sample_known}/{len(task_rows)}")
    for arm in ("short", "long"):
        rows = [r for r in task_rows if r["arm"] == arm]
        known = sum(1 for r in rows if r["sample"])
        print(f"  {arm}: {known}/{len(rows)}")

    n_final = sum(1 for r in task_rows if r["final_success"])
    print(f"final_success rows: {n_final}")
    print(f"(arm, process, sample) groups with >1 COMPLETED attempt: {n_multi_completed_groups}")

    unmapped = sorted({r["process"] for r in task_rows if not r["e2_transform"]})
    print(f"processes with no e2_transform ({len(unmapped)}): {unmapped}")


if __name__ == "__main__":
    main()
