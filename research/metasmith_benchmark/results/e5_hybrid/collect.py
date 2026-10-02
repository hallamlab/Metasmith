#!/usr/bin/env python3
# Gather the E5 hybrid pilot's per-step costs and report tarballs from the two agent homes' task caches.
# Runs on fir. A cache entry's .command.sh names its step and its sample's import directory, and its
# .command.out carries the Slurm scratch directory phyberos.<jobid>, the key into sacct. Entries whose log
# lacks it are matched through their Nextflow work directory, whose hash the run's trace maps to a job.
import csv, re, subprocess, sys
from pathlib import Path

HOMES = [Path("/scratch/phyberos/cami/metasmith"), Path("/scratch/phyberos/pratama2026/metasmith")]
RUNS = {"kCByAwkU", "aRB4jlc0", "oeGtlXNs", "AlYvDWmZ", "NBqReG0G", "XjL5E3Zv"}
# kCByAwkU ran CAMI on Pratama's QC, which starved every short-read step, and AlYvDWmZ reran its OPERA-MS on those
# starved MEGAHIT contigs. Only kCByAwkU's Flye stands.
STARVED = {"kCByAwkU", "AlYvDWmZ"}
STEPS = {"seqkit_reads", "bbduk", "bbduk_pratama", "megahit", "flye", "polca", "opera_ms", "quast", "metaquast"}
REPORTS = {"quast", "metaquast"}
TRACES = [*Path("/scratch/phyberos/bench/e5_hybrid/runlogs").glob("*/logs.*/nxf_trace.tsv"),
          *(h / "runs" / k / "_metasmith" for h in HOMES for k in RUNS)]
OUT = Path(__file__).resolve().parent

STEP_RE = re.compile(r'^echo "([a-z_]+)"$', re.M)
SAMPLE_RE = re.compile(r"/imports/e5h/([A-Za-z0-9_]+)/read_metadata@")
# seqkit_reads requires the reads alone, not their metadata, so it is placed by the reads file bbduk also reads.
READS_RE = re.compile(r"/scratch/[\w./-]+/anonymous_reads\.fq\.gz")
RUN_RE = re.compile(r"/msm_home/runs/([A-Za-z0-9]{8})/")
JOB_RE = re.compile(r"phyberos\.(\d+)\.\d+")
WORK_RE = re.compile(r"cwd \[\S+/runs/([A-Za-z0-9]{8})/nxf_work/([0-9a-f]{2})/([0-9a-f]{6})")


def entries():
    rows = list(raw_entries())
    sample_of = {r["reads"]: r["sample"] for r in rows if r["sample"] and r["reads"]}
    for r in rows:
        r["sample"] = r["sample"] or sample_of.get(r["reads"])
        if r["sample"]:
            yield r


def raw_entries():
    for home in HOMES:
        for sh in (home / "task_cache").glob("*/*/logs/.command.sh"):
            text = sh.read_text(errors="replace")
            step = STEP_RE.search(text)
            run = RUN_RE.search(text)
            if not (step and run) or step[1] not in STEPS or run[1] not in RUNS:
                continue
            sample = SAMPLE_RE.search(text)
            reads = READS_RE.findall(text) if step[1] in ("seqkit_reads", "bbduk") else []
            if run[1] in STARVED and step[1] != "flye":
                continue
            out = sh.with_name(".command.out")
            text = out.read_text(errors="replace") if out.exists() else ""
            job, work = JOB_RE.search(text), WORK_RE.search(text)
            yield {"home": home.parent.name, "run": run[1], "step": step[1], "sample": sample[1] if sample else None,
                   "reads": reads[0] if reads else None,
                   "job": job[1] if job else "", "work": (work[1], f"{work[2]}/{work[3]}") if work else None,
                   "entry": str(sh.parent.parent)}


def trace_jobs():
    jobs = {}
    for t in TRACES:
        run = next(x for x in t.parts if x in RUNS)
        for f in ([t] if t.is_file() else t.glob("logs.*/nxf_trace.tsv")):
            for row in csv.DictReader(f.open(), delimiter="\t"):
                if row["native_id"] and row["native_id"] != "-":
                    jobs[(run, row["hash"])] = row["native_id"]
    return jobs


def place(rows):
    jobs = trace_jobs()
    native = {r["work"]: jobs[r["work"]] for r in rows if not r["job"] and r["work"] in jobs}
    res = subprocess.run(["sacct", "-X", "-P", "-n", "-j", ",".join(set(native.values())), "-o", "JobID,JobIDRaw"],
                         capture_output=True, text=True, check=True)
    raw = dict(line.split("|") for line in res.stdout.splitlines())
    for r in rows:
        if not r["job"] and r["work"] in native:
            r["job"] = raw.get(native[r["work"]], "")


def sacct(jobs):
    rows = {}
    for i in range(0, len(jobs), 200):
        res = subprocess.run(["sacct", "-P", "-n", "-j", ",".join(jobs[i:i + 200]),
                              "-o", "JobIDRaw,ElapsedRaw,AllocCPUS,TotalCPU,MaxRSS,State,ReqMem"],
                             capture_output=True, text=True, check=True)
        for line in res.stdout.splitlines():
            jid, elapsed, cpus, total, rss, state, req = line.split("|")
            base = jid.split(".")[0]
            r = rows.setdefault(base, {"elapsed_s": 0, "cpus": 0, "cpu_used": "", "max_rss_kb": 0,
                                       "state": "", "req_mem": ""})
            if "." not in jid:
                r.update(elapsed_s=int(elapsed or 0), cpus=int(cpus or 0), cpu_used=total, state=state, req_mem=req)
            r["max_rss_kb"] = max(r["max_rss_kb"], rss_kb(rss))
    return rows


def rss_kb(v):
    if not v:
        return 0
    unit = v[-1]
    if unit.isdigit():
        return int(v) // 1024
    return int(float(v[:-1]) * {"K": 1, "M": 1024, "G": 1024 ** 2, "T": 1024 ** 3}[unit])


def main():
    rows = sorted(entries(), key=lambda r: (r["sample"], r["step"], r["run"]))
    place(rows)
    acct = sacct(sorted({r["job"] for r in rows if r["job"]}))
    with (OUT / "steps.tsv").open("w") as f:
        cols = ["sample", "step", "run", "job", "elapsed_s", "cpus", "cpu_used", "max_rss_kb", "req_mem", "state", "entry"]
        w = csv.DictWriter(f, cols, delimiter="\t", extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({**r, **acct.get(r["job"], {})})
    reports = OUT / "reports"
    reports.mkdir(exist_ok=True)
    for r in rows:
        if r["step"] in REPORTS:
            for tar in Path(r["entry"], "out").glob("*.tar.gz"):
                (reports / f"{r['sample']}.{r['step']}.tar.gz").write_bytes(tar.read_bytes())
    print(f"{len(rows)} cache entries, {sum(1 for r in rows if r['job'] in acct)} with sacct rows", file=sys.stderr)


if __name__ == "__main__":
    main()
