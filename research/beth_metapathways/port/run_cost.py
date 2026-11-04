#!/usr/bin/env python3
"""Per-step Slurm cost of one finished run, from its nxf_tasks.csv and sacct.

Prints one row per step (tasks, summed wall, CPU-hours allocated, CPU-hours used, GPU-hours,
max MaxRSS) and a total. Local and cached tasks have no Slurm job and count zero.
Run on fir: stdlib only. Usage: run_cost.py <run_dir>
"""

import argparse
import csv
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


def seconds(text):
    days, _, rest = text.rpartition("-")
    parts = [float(p) for p in rest.split(":")]
    while len(parts) < 3:
        parts.insert(0, 0.0)
    h, m, s = parts
    return (float(days) if days else 0) * 86400 + h * 3600 + m * 60 + s


def gib(text):
    if not text:
        return 0.0
    units = {"K": 1 / 1024 ** 2, "M": 1 / 1024, "G": 1.0, "T": 1024.0}
    return float(text[:-1]) * units[text[-1]] if text[-1] in units else float(text) / 1024 ** 3


def sacct(ids):
    rows = {}
    for i in range(0, len(ids), 200):
        out = subprocess.run(
            ["sacct", "-P", "-n", "-j", ",".join(ids[i:i + 200]),
             "-o", "JobID,State,ElapsedRaw,AllocCPUS,TotalCPU,MaxRSS,AllocTRES"],
            capture_output=True, text=True, check=True).stdout
        for line in out.splitlines():
            jid, state, elapsed, cpus, total, rss, tres = line.split("|")
            base = jid.split(".")[0]
            r = rows.setdefault(base, dict(state="", elapsed=0, cpus=0, used=0.0, rss=0.0, gpus=0))
            if "." not in jid:
                gpus = sum(int(n) for n in re.findall(r"gres/gpu(?::[^=,]+)?=(\d+)", tres)[:1])
                r.update(state=state, elapsed=int(elapsed or 0), cpus=int(cpus or 0), gpus=gpus)
            r["used"] = max(r["used"], seconds(total)) if total else r["used"]
            r["rss"] = max(r["rss"], gib(rss))
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run_dir", type=Path)
    a = ap.parse_args()
    tasks = list(csv.DictReader((a.run_dir / "_metasmith" / "logs.latest" / "nxf_tasks.csv").open()))
    ids = sorted({t["native_id"] for t in tasks if re.fullmatch(r"\d+(_\d+)?", t["native_id"] or "")
                  and "_cached" not in t["name"]})
    jobs = sacct(ids) if ids else {}
    steps = defaultdict(lambda: dict(n=0, ok=0, wall=0.0, alloc=0.0, used=0.0, gpu=0.0, rss=0.0))
    for t in tasks:
        step = t["name"].split(" ")[0]
        s = steps[step]
        s["n"] += 1
        s["ok"] += t["status"] == "COMPLETED"
        j = jobs.get(t["native_id"])
        if j is None:
            continue
        s["wall"] += j["elapsed"]
        s["alloc"] += j["elapsed"] * j["cpus"] / 3600
        s["used"] += j["used"] / 3600
        s["gpu"] += j["elapsed"] * j["gpus"] / 3600
        s["rss"] = max(s["rss"], j["rss"])
    w = csv.writer(sys.stdout, delimiter="\t", lineterminator="\n")
    w.writerow(["step", "tasks", "completed", "wall_h_sum", "cpu_h_alloc", "cpu_h_used", "gpu_h", "max_rss_gib"])
    tot = dict(n=0, ok=0, wall=0.0, alloc=0.0, used=0.0, gpu=0.0, rss=0.0)
    for step in sorted(steps):
        s = steps[step]
        w.writerow([step, s["n"], s["ok"], f"{s['wall'] / 3600:.2f}", f"{s['alloc']:.2f}", f"{s['used']:.2f}",
                    f"{s['gpu']:.2f}", f"{s['rss']:.1f}"])
        for k in tot:
            tot[k] = max(tot[k], s[k]) if k == "rss" else tot[k] + s[k]
    w.writerow(["TOTAL", tot["n"], tot["ok"], f"{tot['wall'] / 3600:.2f}", f"{tot['alloc']:.2f}",
                f"{tot['used']:.2f}", f"{tot['gpu']:.2f}", f"{tot['rss']:.1f}"])


if __name__ == "__main__":
    main()
