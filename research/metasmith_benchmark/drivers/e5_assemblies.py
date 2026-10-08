#!/usr/bin/env python3
"""Map each assembly in E5 runs' results to its study and sample, as TSV rows for results/e5/assemblies.tsv.

Run on fir with plain python3: `e5_assemblies.py RUN...`. A run's `results/` holds hardlinks into the task
cache, and its `_metadata/index.yml` gives each instance's parents, which lead back to the imported
read_metadata whose path names the study and sample.
"""

import re
import sys
from collections import deque
from pathlib import Path

HOME = Path("/scratch/phyberos/e5/metasmith")
ASSEMBLY_DIRS = {"sequences-megahit_assembly": "megahit", "e5-opera_ms_assembly": "opera_ms",
                 "sequences-flye_assembly": "flye"}
SAMPLE = re.compile(r"/imports/e5/([^/]+)/([^/]+)/read_metadata@")


def read_index(path):
    """{instance key: [parent keys]} from a results index, parsed by indentation."""
    parents, key, in_parents = {}, None, False
    with open(path) as f:
        for line in f:
            if line.startswith("  ") and not line.startswith("   "):
                key = line.strip().rstrip(":")
                parents[key], in_parents = [], False
            elif line.startswith("    parents:"):
                in_parents = True
            elif line.startswith("      ") and in_parents:
                ref = line.strip().rsplit(": ", 1)[0]
                parents[key].append(ref.split("@", 1)[1] if "@" in ref.split("/", 1)[0] else ref)
            elif line.startswith("    "):
                in_parents = False
    return parents


def sample_of(key, parents):
    seen, todo = {key}, deque([key])
    while todo:
        k = todo.popleft()
        m = SAMPLE.search(k)
        if m:
            return m.groups()
        for p in parents.get(k, ()):
            if p not in seen:
                seen.add(p)
                todo.append(p)
    return None


def main(runs):
    print("run\tstudy\tsample\tassembler\tpath")
    for run in runs:
        results = HOME / "runs" / run / "results"
        index = results / "_metadata" / "index.yml"
        if not index.exists():
            print(f"{run}: no results index yet", file=sys.stderr)
            continue
        parents = read_index(index)
        for d, assembler in ASSEMBLY_DIRS.items():
            for f in sorted((results / d).glob("*")) if (results / d).is_dir() else ():
                found = sample_of(f"{d}/{f.name}", parents)
                assert found, f"{run}: no read_metadata above {d}/{f.name}"
                print(f"{run}\t{found[0]}\t{found[1]}\t{assembler}\t{f}")


if __name__ == "__main__":
    main(sys.argv[1:])
