"""Reconstruct product -> sample attribution from what nextflow ACTUALLY ran.

WHY THIS EXISTS
Everything metasmith publishes is content-hashed, so the sample a product
belongs to has to be derived rather than read. The obvious source is the run's
own lineage graph -- `results/_metadata/index.yml`, built from
`_metasmith/trace.jsonl` -- and both `publish_r1.py` and
`run_arbutus_campaigns.py harvest` were written against it.

*** THAT LINEAGE IS WRONG ON metasmith 0.19.1, AND WRONG SILENTLY. ***

Measured on run dcYCo2Px, 13 interleave steps, 0 of 13 agreeing with what
nextflow staged. The step that provably ran on S19_R1 + S19_R2 (its `.command.sh`
names them, and its output is the file published for S19) has a trace record
whose `consumes` decodes to S18_R1, S13_R2 and S28's read_pair -- three
different samples, none of them S19. The mis-assignment is per input slot: the
R1 slot always gets some R1, the R2 slot some R2, so nothing looks malformed.

Three artifacts agree with each other and with the filesystem:
  - `results/given.csv`                        instance_id -> path
  - the staged reads library's `_metadata/index.yml`   (same mapping, independently)
  - `.command.sh`'s `lin` block and `FILES` list, written by the code path that
    actually dispatched the task
and one artifact disagrees with all of them: `trace.jsonl`'s `consumes`, which
is what the results library is built from. So this is not ambiguity about which
record to trust -- it is one wrong record among four.

The failure mode that matters is not the loud one. `harvest()` drops any product
that does not resolve to exactly one sample, on the grounds that mislabelled
taxonomy is worse than missing taxonomy, and 12 of the 13 resolve to two samples
and would be dropped. The thirteenth resolves to exactly ONE sample -- S1 -- and
is really S2. The guard cannot see it. Every downstream file would have been
named for the wrong library, and nothing about the output would look wrong.

The computation itself is unaffected: nextflow bound the right files, so the
reads, assemblies and taxonomy are all correct. This is bookkeeping only. But
every name derived from that bookkeeping is a coin flip, which is why both
drivers now attribute from here instead.

WHAT THIS READS INSTEAD
Each nextflow task directory carries, in `.command.sh`:
  - line 2, `echo "<transform>"`, the transform that ran -- which is also how
    the binner is recovered for the three binning steps, replacing the other
    lineage walk `publish_r1.py` used to do;
  - a `lin {...}` block ending in a `FILES` list: the absolute path of every
    input the task was handed, either a given (`.../reads/S19_R1.fastq.gz`) or
    an upstream product (`.../nxf_work/e8/16ad.../1-1-1.<hash>-<hash>.fq.gz`).
and the task's outputs are simply the non-`.command*` regular files beside it.

That is a complete DAG over real paths, with the givens as roots and the sample
id still in their filenames. Walk it back and the attribution is exact, because
it is a record of the binding rather than a reconstruction of it.

Run this ON THE CLUSTER (it walks nxf_work, which is thousands of small reads
and one line of output per product) and pull the TSV back:

  python3 nxf_attribution.py <run_dir> > attribution.tsv

Columns: product, sample, transform, task_dir. `sample` is `?` when a product
resolves to no sample and `*` when it resolves to several -- both legitimate for
cross-sample steps like skani_dedup, and both a refusal rather than a guess.
Stdlib only, python3 -- fir's login node has no project environment.
"""
import re
import sys
import json
from pathlib import Path

READ_RE = re.compile(r"^(.+?)(?:_R[12]\.fastq\.gz|\.interleaved\.fq\.gz)$")
PATH_RE = re.compile(r"/[^\s\[\],\"\\]+")
SKIP = ("/lib/", "/_metasmith/task/transforms/", "/workflow.step_")


def read_task(d):
    cmd = d / ".command.sh"
    if not cmd.exists():
        return None
    try:
        head = cmd.read_text(errors="replace").split("\n", 4)
    except OSError:
        return None
    if len(head) < 3:
        return None

    m = re.match(r'\s*echo\s+"(.+?)"\s*$', head[2])
    transform = m.group(1) if m else "?"

    lin = next((l for l in head if l.lstrip().startswith('echo "lin ')), head[1])
    files = [p for p in PATH_RE.findall(lin) if not any(s in p for s in SKIP)]

    outs = [p.name for p in d.iterdir()
            if p.is_file() and not p.name.startswith(".")]
    return transform, files, outs


def build(run_dir):
    run_dir = Path(run_dir)
    work = run_dir / "nxf_work"
    if not work.is_dir():
        sys.exit(f"no nxf_work under {run_dir}")

    tasks = []
    produced_by = {}
    for d in sorted(work.glob("*/*")):
        if not d.is_dir():
            continue
        t = read_task(d)
        if not t:
            continue
        i = len(tasks)
        tasks.append((d,) + t)
        for o in t[2]:
            produced_by.setdefault(o, i)
        for o in t[2]:
            if READ_RE.match(o):
                del produced_by[o]

    memo = {}

    def walk(idx, seen):
        if idx in memo:
            return memo[idx]
        if idx in seen:
            return frozenset(), frozenset()
        seen = seen | {idx}
        _d, _t, files, _o = tasks[idx]
        samples, trans = set(), set()
        for f in files:
            base = Path(f).name
            m = READ_RE.match(base)
            if m:
                samples.add(m.group(1))
                continue
            up = produced_by.get(base)
            if up is not None and up != idx:
                s, t = walk(up, seen)
                samples |= s
                trans |= t | {tasks[up][1]}
        res = (frozenset(samples), frozenset(trans))
        if not seen - {idx}:
            memo[idx] = res
        return res

    rows = []
    for i, (d, transform, _files, outs) in enumerate(tasks):
        s, upstream = walk(i, frozenset())
        label = next(iter(s)) if len(s) == 1 else ("?" if not s else "*")
        up = ",".join(sorted(upstream))
        for o in outs:
            rows.append((o, label, transform, up, str(d)))
    return rows


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__.strip().splitlines()[0])
    rows = build(sys.argv[1])
    print("product\tsample\ttransform\tupstream\ttask_dir")
    for r in sorted(rows):
        print("\t".join(r))
    n_ok = sum(1 for r in rows if r[1] not in ("?", "*"))
    print(f"# {len(rows)} product(s), {n_ok} attributed to a single sample",
          file=sys.stderr)


if __name__ == "__main__":
    main()
