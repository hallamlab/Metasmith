#!/usr/bin/env python3
"""Core-hours per step and read type for E5 runs, from each run's nextflow traces and sacct.

  e5_cost.py <home> <shape>=<key>[,<driver job>...] ... > cost.tsv

Run it on fir. Every attempt in every logs.* directory of a run counts, failed ones included, because
the allocation was spent either way. A cache hit costs nothing and is counted apart. The driver job is
its own row: it hosts the run's local steps and holds its CPUs for the whole run.
"""

import csv
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


def sacct(jobs):
    rows = {}
    for i in range(0, len(jobs), 200):
        out = subprocess.run(
            ["sacct", "-X", "-n", "-P", "-j", ",".join(jobs[i:i + 200]),
             "-o", "JobID,AllocCPUS,ElapsedRaw,ReqMem,State"],
            capture_output=True, text=True, check=True).stdout
        for line in out.splitlines():
            jid, cpus, elapsed, mem, state = line.split("|")
            rows[jid] = (int(cpus or 0), int(elapsed or 0), mem, state.split()[0])
    return rows


def step_of(name):
    step = name.split(" (")[0].split("__", 1)[-1]
    return (step[:-len("_cached")], True) if step.endswith("_cached") else (step, False)


def main():
    home = Path(sys.argv[1])
    acc = defaultdict(lambda: {"tasks": 0, "failed": 0, "cache_hits": 0, "core_h": 0.0, "max_wall_h": 0.0, "unmatched": 0})
    drivers = {}
    traced = []
    for spec in sys.argv[2:]:
        shape, rest = spec.split("=", 1)
        key, *jobs = rest.split(",")
        drivers[shape] = jobs
        for trace in sorted((home / "runs" / key / "_metasmith").glob("logs.*/nxf_trace.tsv")):
            if trace.parent.is_symlink():
                continue
            for r in csv.DictReader(trace.open(), delimiter="\t"):
                if r["native_id"] and r["native_id"] != "-":
                    traced.append((shape, step_of(r["name"]), r["native_id"], r["status"]))
    acct = sacct(sorted({j for *_, j, _ in traced} | {j for js in drivers.values() for j in js}))
    for shape, (step, cached), jid, status in traced:
        a = acc[(shape, step)]
        if cached:
            a["cache_hits"] += 1
            continue
        a["tasks"] += 1
        a["failed"] += status != "COMPLETED"
        if jid not in acct:
            a["unmatched"] += 1
            continue
        cpus, elapsed, _, _ = acct[jid]
        a["core_h"] += cpus * elapsed / 3600
        a["max_wall_h"] = max(a["max_wall_h"], elapsed / 3600)
    for shape, jobs in drivers.items():
        for jid in jobs:
            if jid in acct:
                cpus, elapsed, _, _ = acct[jid]
                a = acc[(shape, "driver")]
                a["tasks"] += 1
                a["core_h"] += cpus * elapsed / 3600
                a["max_wall_h"] = max(a["max_wall_h"], elapsed / 3600)
    w = csv.writer(sys.stdout, delimiter="\t", lineterminator="\n")
    w.writerow(["shape", "step", "attempts", "failed", "cache_hits", "core_h", "max_wall_h", "unmatched"])
    for (shape, step), a in sorted(acc.items()):
        w.writerow([shape, step, a["tasks"], a["failed"], a["cache_hits"], f"{a['core_h']:.2f}", f"{a['max_wall_h']:.2f}", a["unmatched"]])


if __name__ == "__main__":
    main()
