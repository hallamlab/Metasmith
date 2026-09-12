#!/usr/bin/env python3
"""Collapse every narrative workflow shape onto the core task vocabulary.

Reads `tasks.yml`, `task_io.jsonl`, `app_tasks.jsonl` and
`curation/r1/shapes.jsonl`. Writes `workflows.jsonl`, one row per distinct set of
core tasks, carrying the shapes that reduce to it and the narrative copies behind
them.

Round 1 asked what distinct app sequences the public narratives contain and got
1,633. That count is an artifact of KBase's own redundancy: two narratives that
assemble, bin and annotate are the same workflow whether one ran metaSPAdes and
metabat and the other MEGAHIT and MaxBin2. This asks the question again over verbs.

Four reductions, each reported so the funnel stays legible:

1. app -> core task, and an app carrying no task at all is dropped
2. plumbing verbs are dropped -- set building, exports, viewers and format
   conversions move objects without producing new science, and they are what makes
   two runs of one pipeline look like two pipelines
3. reporting verbs are dropped -- a verb whose proposal produces a report and
   nothing else cannot change what the workflow yields, so running FastQC or not
   is not the difference between two workflows
4. the remainder is taken as a SET rather than a sequence

The fourth is the one that needs defending. The corpus records cell order, not
dependency order, so a narrative that ran QC after trimming and one that ran it
before are the same workflow written down differently. Ordering that is real --
annotate before build_model -- is recovered from the task graph rather than
asserted from the corpus, so the sequences are kept as members instead.

    catalogue.py [--top N]
"""

from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
R1 = HERE.parent / "r1"


def load():
    vocab = yaml.safe_load((HERE / "tasks.yml").read_text())
    io = {json.loads(l)["task"]: json.loads(l)
          for l in (HERE / "task_io.jsonl").read_text().splitlines() if l}
    assigned = {json.loads(l)["app_id"]: json.loads(l)
                for l in (HERE / "app_tasks.jsonl").read_text().splitlines() if l}
    shapes = [json.loads(l) for l in (R1 / "shapes.jsonl").read_text().splitlines() if l]
    return vocab, io, assigned, shapes


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--top", type=int, default=20, help="how many rows to print")
    args = ap.parse_args()

    vocab, io, assigned, shapes = load()
    spec = {t["name"]: t for t in vocab["tasks"]}
    plumbing = {name for name, t in spec.items() if t["plumbing"]}
    reporting = {name for name, t in spec.items() if t["produces"] == ["report"]}
    dropped = plumbing | reporting

    funnel = [("wired shapes from round 1", len(shapes))]
    funnel.append(("distinct app sequences", len({tuple(s["apps"]) for s in shapes})))

    verb_seqs = collections.Counter()
    for s in shapes:
        verbs = [assigned[a]["task"] for a in s["apps"] if a in assigned]
        verb_seqs[tuple(v for v in verbs if v)] += 1
    funnel.append(("as core task sequences", len(verb_seqs)))

    rows_by_key = collections.defaultdict(list)
    unknown_apps = collections.Counter()
    for s in shapes:
        seq = []
        for a in s["apps"]:
            row = assigned.get(a)
            if row is None:
                unknown_apps[a] += 1
                continue
            if row["kind"] != "task" or row["task"] in dropped:
                continue
            seq.append(row["task"])
        s["_science"] = seq
        rows_by_key[tuple(sorted(set(seq)))].append(s)

    no_plumbing = collections.Counter()
    for s in shapes:
        verbs = [assigned[a]["task"] for a in s["apps"]
                 if a in assigned and assigned[a]["kind"] == "task"
                 and assigned[a]["task"] not in plumbing]
        no_plumbing[tuple(sorted(set(verbs)))] += 1
    funnel.append((f"as producing task sequences (-{len(plumbing)} plumbing, "
                   f"-{len(reporting)} reporting)", len({tuple(s["_science"]) for s in shapes})))
    funnel.append(("as task sets, plumbing dropped only", len(no_plumbing)))
    funnel.append(("as task sets, plumbing and reporting dropped", len(rows_by_key)))

    catalogue = []
    for key, members in rows_by_key.items():
        copies = sum(m["copies"] for m in members)
        terminals = collections.Counter()
        firsts = collections.Counter()
        for m in members:
            if m["_science"]:
                terminals[m["_science"][-1]] += m["copies"]
                firsts[m["_science"][0]] += m["copies"]
        terminal = terminals.most_common(1)[0][0] if terminals else None
        first = firsts.most_common(1)[0][0] if firsts else None
        product = spec[terminal]["produces"][0] if terminal else None
        source = None
        if first:
            consumes = spec[first]["consumes"]
            source = consumes[0] if consumes else spec[first]["produces"][0]
        catalogue.append({
            "tasks": list(key),
            "n_tasks": len(key),
            "copies": copies,
            "shapes": len(members),
            "terminal_task": terminal,
            "first_task": first,
            "product": product,
            "source": source,
            "name": f"{product}_from_{source}" if product and source else None,
            "product_is_typed": bool(terminal) and not io[terminal]["untyped_canonical"],
            "all_ref_edges": all(m["all_ref_edges"] for m in members),
            "catalog_complete": all(not m["unknown_apps"] for m in members),
            "shape_ids": [m["shape_id"] for m in members][:200],
            "sequences": len({tuple(m["_science"]) for m in members}),
        })
    catalogue.sort(key=lambda r: (-r["copies"], -r["shapes"], r["tasks"]))
    for i, r in enumerate(catalogue, start=1):
        r["workflow_id"] = f"w{i:04d}"
    (HERE / "workflows.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in catalogue))

    total = sum(r["copies"] for r in catalogue)
    for label, n in funnel:
        print(f"  {label:52s} {n}")
    if unknown_apps:
        print(f"\n{len(unknown_apps)} apps in the corpus are absent from the catalog "
              f"and carry no verb, appearing {sum(unknown_apps.values())} times")

    empty = [r for r in catalogue if not r["tasks"]]
    print(f"\n{len(catalogue)} canonical workflows over {total} narrative copies")
    if empty:
        print(f"  of which {len(empty)} row is the empty set: "
              f"{empty[0]['copies']} copies whose every step is plumbing or uncatalogued")
    typed = [r for r in catalogue if r["product_is_typed"]]
    print(f"  {len(typed)} name a terminal product the canonical method actually declares "
          f"({sum(r['copies'] for r in typed)} copies)")
    names = collections.Counter(r["name"] for r in catalogue if r["name"])
    print(f"  {len(names)} distinct <product>_from_<source> names, so a name covers "
          f"{len(catalogue) / len(names):.1f} workflows on average")

    run = 0
    marks = {0.25: None, 0.5: None, 0.8: None, 0.9: None}
    for i, r in enumerate(catalogue, start=1):
        run += r["copies"]
        for frac in marks:
            if marks[frac] is None and run / total >= frac:
                marks[frac] = i
    print("\nworkflows needed to cover a share of all narrative copies:")
    for frac in sorted(marks):
        print(f"  {int(frac * 100):3d}%  {marks[frac]} workflows")

    print(f"\ntop {args.top} by copies:")
    print(f"  {'copies':>6s} {'shapes':>6s}  {'name':44s} tasks")
    for r in catalogue[:args.top]:
        print(f"  {r['copies']:6d} {r['shapes']:6d}  {str(r['name'])[:44]:44s} "
              f"{'+'.join(r['tasks'])[:70]}")


if __name__ == "__main__":
    main()
