#!/usr/bin/env python3
# Per-sample CPU cost of E5's two Pratama assembly shapes, from fir's Slurm accounting. Runs on fir with plain python3
# and writes its TSVs next to itself. A shape's entries are the cache shards its run took from cache_hits.jsonl, plus
# every task the run computed itself, read from its own work directory: a run stores its shards only when it ends.
# A shard's .command.out carries the Slurm scratch directory phyberos.<jobid>, the key into sacct; a shard whose log
# lacks it is matched through its Nextflow work directory, whose hash the computing run's trace maps to a job.
import csv, json, re, statistics, subprocess, sys
from collections import defaultdict
from pathlib import Path

HOME = Path("/scratch/phyberos/e5/metasmith")
SHAPES = {
    "pratama_hybrid_ont": {"run": "N84FrRPY", "imports": "pratama_hybrid",
                           "qc": ["seqkit_reads", "bbduk"], "assembly": ["megahit_draft", "opera_ms"]},
    "pratama_pe": {"run": "BxTeX5c0", "imports": "pratama_short",
                   "qc": ["seqkit_reads", "bbduk"], "assembly": ["megahit"]},
}
OUT = Path(__file__).resolve().parent

STEP_RE = re.compile(r'^echo "([a-z_0-9]+)"$', re.M)
RUN_RE = re.compile(r"^bootstrap \S+/runs/([A-Za-z0-9]{8}) ", re.M)
SAMPLE_RE = re.compile(r"/imports/e5/([a-z_]+)/([A-Za-z0-9_]+)/read_metadata@")
# seqkit_reads requires the reads alone, not their metadata, so it is placed by the reads file bbduk also reads.
READS_RE = re.compile(r"/scratch/phyberos/pratama2026/[\w./-]+?\.f(?:ast)?q\.gz")
JOB_RE = re.compile(r"phyberos\.(\d+)\.\d+")
WORK_RE = re.compile(r"cwd \[\S+/runs/([A-Za-z0-9]{8})/nxf_work/([0-9a-f]{2})/([0-9a-f]{6})")


def steps(shape):
    return SHAPES[shape]["qc"] + SHAPES[shape]["assembly"]


def sources():
    for shape, cfg in SHAPES.items():
        for line in (HOME / "runs" / cfg["run"] / "_metasmith" / "cache_hits.jsonl").open():
            hit = json.loads(line)
            if hit["step_name"] in steps(shape):
                shard = Path(hit["shard"])
                yield shape, shard / "logs" / ".command.sh", shard / "logs" / ".command.out", None
        run = HOME / "runs" / cfg["run"]
        for f in (run / "_metasmith").glob("logs.2*/nxf_trace.tsv"):
            for row in csv.DictReader(f.open(), delimiter="\t"):
                if row["status"] == "COMPLETED" and not row["name"].split(" ")[0].endswith("_cached"):
                    for work in run.glob(f"nxf_work/{row['hash']}*"):
                        yield shape, work / ".command.sh", work / ".command.out", row["native_id"]


def entries():
    rows = []
    for shape, sh, out, native in sources():
        text = sh.read_text(errors="replace")
        step, run = STEP_RE.search(text), RUN_RE.search(text)
        if not step or step[1] not in steps(shape):
            continue
        sample = SAMPLE_RE.search(text)
        if sample and sample[1] != SHAPES[shape]["imports"]:
            sys.exit(f"{sh}: {shape} entry imports from {sample[1]}")
        log = out.read_text(errors="replace") if out.exists() else ""
        job, work = JOB_RE.search(log), WORK_RE.search(log)
        rows.append({"shape": shape, "step": step[1], "sample": sample[2] if sample else None,
                     "reads": READS_RE.findall(text), "run": run[1] if run else "", "native": native,
                     "job": job[1] if job else "", "work": (work[1], f"{work[2]}/{work[3]}") if work else None,
                     "source": str(sh.parent.parent if native is None else sh.parent)})
    sample_of = {(r["shape"], p): r["sample"] for r in rows if r["sample"] for p in r["reads"]}
    for r in rows:
        r["sample"] = r["sample"] or next((sample_of[(r["shape"], p)] for p in r["reads"] if (r["shape"], p) in sample_of), None)
    return rows


def trace_jobs(runs):
    jobs = {}
    for run in runs:
        for f in (HOME / "runs" / run / "_metasmith").glob("logs.2*/nxf_trace.tsv"):
            for row in csv.DictReader(f.open(), delimiter="\t"):
                if row["native_id"] and row["native_id"] != "-":
                    jobs[(run, row["hash"])] = row["native_id"]
    return jobs


def place(rows):
    jobs = trace_jobs({r["work"][0] for r in rows if not r["job"] and r["work"]})
    native = {r["work"]: jobs[r["work"]] for r in rows if not r["job"] and r["work"] in jobs}
    ids = sorted(set(native.values()) | {r["native"] for r in rows if r["native"]})
    raw = {}
    for i in range(0, len(ids), 200):
        res = subprocess.run(["sacct", "-X", "-P", "-n", "-j", ",".join(ids[i:i + 200]), "-o", "JobID,JobIDRaw"],
                             capture_output=True, text=True, check=True)
        raw.update(line.split("|") for line in res.stdout.splitlines())
    for r in rows:
        if r["native"]:
            r["job"] = raw.get(r["native"], "")
        elif not r["job"] and r["work"] in native:
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
            r = rows.setdefault(base, {"elapsed_s": 0, "cpus": 0, "cpu_used_s": 0, "max_rss_kb": 0, "state": "", "req_mem": ""})
            if "." not in jid:
                r.update(elapsed_s=int(elapsed or 0), cpus=int(cpus or 0), cpu_used_s=seconds(total),
                         state=state.split()[0], req_mem=req)
            r["max_rss_kb"] = max(r["max_rss_kb"], kb(rss))
    return rows


def kb(v):
    if not v:
        return 0
    v = v.rstrip("nc")
    if v[-1].isdigit():
        return int(v) // 1024
    return int(float(v[:-1]) * {"K": 1, "M": 1024, "G": 1024 ** 2, "T": 1024 ** 3}[v[-1]])


def seconds(v):
    days, _, hms = v.rpartition("-")
    parts = [float(x) for x in hms.split(":")]
    while len(parts) < 3:
        parts.insert(0, 0.0)
    return int(days or 0) * 86400 + parts[0] * 3600 + parts[1] * 60 + parts[2]


def fmt(v, digits=2):
    return "" if v is None else f"{v:.{digits}f}"


def expected():
    return {shape: sorted(p.name for p in (HOME / "imports" / "e5" / cfg["imports"]).iterdir() if p.is_dir() and "@" not in p.name)
            for shape, cfg in SHAPES.items()}


def main():
    rows = entries()
    for r in rows:
        if not r["sample"]:
            print(f"no sample for {r['shape']} {r['step']} {r['source']}", file=sys.stderr)
    place(rows)
    acct = sacct(sorted({r["job"] for r in rows if r["job"]}))
    by = defaultdict(list)
    for r in rows:
        by[(r["shape"], r["sample"], r["step"])].append(r)

    out, problems, cost = [], [], {}
    for shape, samples in expected().items():
        for s in samples:
            for step in steps(shape):
                found = by.get((shape, s, step), [])
                if len(found) > 1:
                    problems.append(f"{shape} {s} {step}: {len(found)} entries: {[f['source'] for f in found]}")
                if not found:
                    problems.append(f"{shape} {s} {step}: no cache entry")
                    out.append({"shape": shape, "sample": s, "step": step, "state": "NO_ENTRY"})
                    continue
                r = found[0]
                a = acct.get(r["job"])
                if not a:
                    problems.append(f"{shape} {s} {step}: no sacct row (job {r['job'] or '?'}, {r['source']})")
                    out.append({**r, "state": "NO_SACCT"})
                    continue
                if a["state"] != "COMPLETED":
                    problems.append(f"{shape} {s} {step}: job {r['job']} is {a['state']}")
                else:
                    cost[(shape, s, step)] = a
                out.append({**r, "elapsed_s": a["elapsed_s"], "cpus": a["cpus"],
                            "cpu_h": fmt(a["elapsed_s"] * a["cpus"] / 3600, 3),
                            "max_rss_gb": fmt(a["max_rss_kb"] / 1024 ** 2), "req_gb": fmt(kb(a["req_mem"]) / 1024 ** 2, 1),
                            "state": a["state"], "cpu_used_h": fmt(a["cpu_used_s"] / 3600, 3)})
    cols = ["shape", "sample", "step", "run", "job", "elapsed_s", "cpus", "cpu_h", "max_rss_gb", "req_gb", "state",
            "cpu_used_h", "source"]
    with (OUT / "pratama_assembly_cost.tsv").open("w") as f:
        w = csv.DictWriter(f, cols, delimiter="\t", extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(out)

    lanes = []
    for shape, samples in expected().items():
        cfg = SHAPES[shape]
        for s in samples:
            got = {st: cost.get((shape, s, st)) for st in steps(shape)}
            cpu = {st: a["elapsed_s"] * a["cpus"] / 3600 for st, a in got.items() if a}
            missing = [st for st, a in got.items() if not a]
            qc = sum(cpu[st] for st in cfg["qc"]) if all(st in cpu for st in cfg["qc"]) else None
            asm = sum(cpu[st] for st in cfg["assembly"]) if all(st in cpu for st in cfg["assembly"]) else None
            wall = sum(got[st]["elapsed_s"] for st in cfg["assembly"]) / 3600 if asm is not None else None
            lanes.append({"shape": shape, "sample": s, "qc_cpu_h": fmt(qc, 3), "assembly_cpu_h": fmt(asm, 3),
                          "assembly_wall_h": fmt(wall, 3),
                          "total_cpu_h": fmt(qc + asm, 3) if qc is not None and asm is not None else "",
                          "missing": ",".join(missing)})
    with (OUT / "pratama_assembly_cost_lanes.tsv").open("w") as f:
        w = csv.DictWriter(f, ["shape", "sample", "qc_cpu_h", "assembly_cpu_h", "assembly_wall_h", "total_cpu_h", "missing"],
                           delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(lanes)

    for shape, samples in expected().items():
        found = {r["sample"] for r in rows if r["shape"] == shape and r["sample"]}
        print(f"{shape}: {len(found & set(samples))}/{len(samples)} samples with cache entries"
              + (f", unexpected {sorted(found - set(samples))}" if found - set(samples) else ""))
        ls = [l for l in lanes if l["shape"] == shape]
        for m in ["qc_cpu_h", "assembly_cpu_h", "assembly_wall_h", "total_cpu_h"]:
            vals = [float(l[m]) for l in ls if l[m]]
            if vals:
                print(f"  {m}: n={len(vals)} median={statistics.median(vals):.2f} total={sum(vals):.1f}")
    for p in problems:
        print("MISSING/ODD:", p, file=sys.stderr)


if __name__ == "__main__":
    main()
