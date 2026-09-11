#!/usr/bin/env python3
"""Round 1 of curation: reduce ~4,000 narrative DAGs to workflow candidates.

Mechanical only. This round does not judge whether a workflow is scientifically
interesting; it removes what cannot be a workflow and collapses what is the same
workflow copied many times, so that a later judging round -- a human or an agent
panel -- reads tens of candidates rather than thousands of near-duplicates.

Four filters, in order, each reported so the funnel stays legible:

1. drop narratives with no app cells at all
2. drop single-app narratives -- one step is not a workflow
3. drop unwired narratives -- app cells that share no object are not a pipeline
4. collapse exact duplicates by app sequence, then cluster by app set

The duplicate multiplicity is kept rather than discarded. A tutorial is copied by
every user who takes it, so frequency measures teaching, not practice, and the
count is the signal that says which is which.

    curate.py
"""

from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent.parent
DAGS = REPO / "data" / "kbase" / "narratives" / "dags.jsonl"


def spine(d) -> tuple:
    """The apps that actually participate in an edge, in cell order."""
    touched = {e["from"] for e in d["edges"]} | {e["to"] for e in d["edges"]}
    return tuple(d["apps"][i] for i in sorted(touched))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.parse_args()

    dags = [json.loads(l) for l in DAGS.read_text().splitlines() if l]
    funnel = [("narratives with a DAG record", len(dags))]

    has_apps = [d for d in dags if d["n_nodes"] > 0]
    funnel.append(("with at least one app cell", len(has_apps)))
    multi = [d for d in has_apps if d["n_nodes"] > 1]
    funnel.append(("with more than one app cell", len(multi)))
    wired = [d for d in multi if d["n_edges"] > 0]
    funnel.append(("with at least one edge", len(wired)))

    by_sig = collections.defaultdict(list)
    for d in wired: by_sig[d["signature"]].append(d)
    funnel.append(("distinct app sequences", len(by_sig)))

    by_shape = collections.defaultdict(list)
    for d in wired: by_shape[spine(d)].append(d)
    funnel.append(("distinct wired shapes", len(by_shape)))

    reps = []
    for shape, members in sorted(by_shape.items(), key=lambda kv: -len(kv[1])):
        # The most trustworthy copy: most reference-derived edges, then most edges,
        # then most recent. A name-only DAG may have joined unrelated cells.
        best = max(members, key=lambda d: (d["n_ref_edges"], d["n_edges"],
                                           d["modified_at"] or 0))
        reps.append({
            "shape_id": f"s{len(reps) + 1:04d}",
            "copies": len(members),
            "n_steps": len(shape),
            "apps": list(shape),
            "representative": best["ref"],
            "title": best["title"],
            "n_nodes": best["n_nodes"], "n_edges": best["n_edges"],
            "n_ref_edges": best["n_ref_edges"], "n_name_edges": best["n_name_edges"],
            "all_ref_edges": best["n_name_edges"] == 0,
            "unknown_apps": best["unknown_apps"],
            "members": [d["ref"] for d in members][:50],
        })
    (HERE / "shapes.jsonl").write_text("".join(json.dumps(r) + "\n" for r in reps))

    trusted = [r for r in reps if r["all_ref_edges"]]
    known = [r for r in reps if not r["unknown_apps"]]
    funnel.append(("shapes whose edges are all reference-derived", len(trusted)))
    funnel.append(("shapes using only apps still in the catalog", len(known)))
    prime = [r for r in reps if r["all_ref_edges"] and not r["unknown_apps"]]
    funnel.append(("shapes that are both", len(prime)))
    (HERE / "candidates.jsonl").write_text("".join(json.dumps(r) + "\n" for r in prime))

    md = ["# Curation round 1 -- mechanical\n\n",
          "Reduces every public narrative to a set of distinct workflow shapes. A shape is\n",
          "the sequence of apps that actually participate in an edge, so two narratives\n",
          "running the same pipeline on different data are one shape.\n\n",
          "## Funnel\n\n| stage | count |\n|---|---|\n"]
    for label, n in funnel: md.append(f"| {label} | {n} |\n")
    md.append("\n`shapes.jsonl` is every shape. `candidates.jsonl` is the subset a later round\n"
              "should judge: every edge derived from a workspace reference rather than a name\n"
              "match, and every app still in the catalog so a transform exists for it.\n")
    md.append("\n## The 20 most copied shapes\n\n| copies | steps | ref-edges only | apps |\n|---|---|---|---|\n")
    for r in reps[:20]:
        md.append(f"| {r['copies']} | {r['n_steps']} | {'yes' if r['all_ref_edges'] else 'no'} | "
                  f"{' -> '.join(a.split('/')[-1] for a in r['apps'][:5])}"
                  f"{' ...' if len(r['apps']) > 5 else ''} |\n")
    md.append("\n## The 15 longest candidate workflows\n\n| steps | copies | apps |\n|---|---|---|\n")
    for r in sorted(prime, key=lambda x: -x["n_steps"])[:15]:
        md.append(f"| {r['n_steps']} | {r['copies']} | "
                  f"{' -> '.join(a.split('/')[-1] for a in r['apps'][:7])}"
                  f"{' ...' if len(r['apps']) > 7 else ''} |\n")
    (HERE / "README.md").write_text("".join(md))

    for label, n in funnel: print(f"  {label:48s} {n}")
    print(f"\nwrote {len(reps)} shapes, {len(prime)} candidates")


if __name__ == "__main__":
    main()
