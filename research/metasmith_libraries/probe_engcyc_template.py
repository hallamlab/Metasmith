#!/usr/bin/env python3
"""Plan engcyc_metagenomics and the isolate template for each sample shape, alone and mixed.

Opens nothing and runs nothing. Each study's samples are funnelled into the template's
case for their shape (short, hybrid, long), as reads or as SRA accessions. Prints, per
study, whether it planned, its steps, its planning time and any dropped targets.
`--merge` also plans each case of the mixed study alone and prints, per transform, how
many steps the cases need apart and how many the mixed plan holds.

    python research/metasmith_libraries/probe_engcyc_template.py [--max-iter N] [--merge] [study ...]

The default budget is the one `build_templates.py` plans with.
"""
import sys
import time
from collections import Counter
from pathlib import Path

MLIB = Path(__file__).resolve().parents[2] / "src" / "metasmith_libraries"
sys.path.insert(0, str(MLIB))
import engcyc_metagenomics as E  # noqa: E402
import isolate_assembly_from_long_reads as I  # noqa: E402

SHORT_RUN, LONG_RUN = "SRR5585544", "SRR13128014"
STUDIES = {
    "short": [("short", "short_1")],
    "hybrid": [("hybrid", "hybrid_1")],
    "long": [("long", "long_1")],
    "short_acc": [("short", "short_1", {"short": SHORT_RUN})],
    "long_acc": [("long", "long_1", {"long": LONG_RUN})],
    "hybrid_acc": [("hybrid", "hybrid_1", {"short": SHORT_RUN, "long": LONG_RUN})],
    "mixed": [("short", "short_1"), ("short", "short_2"), ("hybrid", "hybrid_1"),
              ("long", "long_1")],
}
ISOLATES = {"isolate": None, "isolate_acc": LONG_RUN}


def template(name):
    if name in ISOLATES:
        return I.template_for(f"probe_engcyc_{name}", accession=ISOLATES[name], rebuild=True)
    return E.template_for(f"probe_engcyc_{name}", STUDIES[name], rebuild=True)


def solve(name, max_iter, cases=None):
    t = template(name)
    if cases is not None:
        t.cases = [c for c in t.cases if c.name in cases]
    t0 = time.time()
    task = t.Plan(max_iter=max_iter)
    return task, time.time() - t0


def counts(plan):
    return Counter(Path(s.transform._path).stem for s in plan.steps)


def report(label, task, dt, max_iter):
    p = task.plan
    print(f"[{label}] max_iter={max_iter} ok={task.ok} steps={len(p.steps)} plan={dt:.1f}s "
          f"dropped={sorted(p.dropped_targets)}", flush=True)
    print("  " + ", ".join(f"{k}x{v}" if v > 1 else k for k, v in sorted(counts(p).items())))
    for h in (p.hints or [])[:10]:
        print(f"  hint {getattr(h, 'kind', '?')}: {getattr(h, 'message', h)}"[:600])


def merge(max_iter):
    mixed, dt = solve("mixed", max_iter)
    report("mixed", mixed, dt, max_iter)
    alone = {}
    for case in dict.fromkeys(c for c, *_ in STUDIES["mixed"]):
        task, dt = solve("mixed", max_iter, cases={case})
        report(f"mixed/{case} alone", task, dt, max_iter)
        alone[case] = counts(task.plan)
    together = counts(mixed.plan)
    served = {}
    for step in mixed.plan.steps:
        served.setdefault(", ".join(sorted(step.cases)), []).append(Path(step.transform._path).stem)
    for cases, steps in sorted(served.items()):
        print(f"\nserves [{cases}]: {len(steps)} steps\n  " + ", ".join(sorted(steps)))
    names = list(alone)
    print("\ntransform | " + " | ".join(names) + " | sum alone | mixed")
    for t in sorted(set(together).union(*alone.values())):
        row = [alone[c].get(t, 0) for c in names]
        print(f"{t} | " + " | ".join(map(str, row)) + f" | {sum(row)} | {together.get(t, 0)}")
    print(f"total | " + " | ".join(str(sum(alone[c].values())) for c in names)
          + f" | {sum(sum(a.values()) for a in alone.values())} | {sum(together.values())}")


def main(argv):
    max_iter, do_merge = 512, False
    while argv[:1] and argv[0].startswith("--"):
        if argv[0] == "--max-iter":
            max_iter, argv = int(argv[1]), argv[2:]
        elif argv[0] == "--merge":
            do_merge, argv = True, argv[1:]
    for name in argv or (list(STUDIES) + list(ISOLATES)):
        task, dt = solve(name, max_iter)
        report(name, task, dt, max_iter)
    if do_merge:
        merge(max_iter)


if __name__ == "__main__":
    main(sys.argv[1:])
