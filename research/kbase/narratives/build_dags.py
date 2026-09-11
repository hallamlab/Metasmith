#!/usr/bin/env python3
"""Rebuild each narrative's app cells as a dependency DAG.

An app cell names the objects it consumed in its parameters and the objects it
produced in its job result. An edge exists where one cell's product is another
cell's input. Two kinds, and they are not equally trustworthy:

* **ref** -- the producer's job result carried a concrete workspace reference and
  the consumer's parameters name it. Unambiguous.
* **name** -- the producer's output-name parameter and the consumer's input
  parameter carry the same string. Object names are reused within a workspace, so
  this can join two unrelated cells and is labelled accordingly.

Which parameter is an input and which names an output comes from the app's own
spec, via research/kbase/catalog/apps.jsonl.

    build_dags.py            # write dags.jsonl and the summary
"""

from __future__ import annotations

import argparse
import collections
import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent
CATALOG = REPO / "research" / "kbase" / "catalog"
CORPUS = REPO / "data" / "kbase" / "narratives"
CELLS = CORPUS / "cells.jsonl"
DAGS = CORPUS / "dags.jsonl"
SUMMARY = HERE / "SUMMARY.md"

WS_REF = re.compile(r"^\d+/\d+(/\d+)?$")


def catalog():
    return {a["app_id"]: a for a in
            (json.loads(l) for l in (CATALOG / "apps.jsonl").read_text().splitlines() if l)}


def _strings(v):
    if isinstance(v, str): yield v
    elif isinstance(v, dict):
        for x in v.values(): yield from _strings(x)
    elif isinstance(v, (list, tuple)):
        for x in v: yield from _strings(x)


def cell_io(cell, cat):
    """(consumed strings, produced names, produced refs) for one app cell."""
    params = cell.get("params") or {}
    spec = cat.get(cell["app_id"])
    consumed, produced = set(), set()
    if spec:
        in_ids = {e["param_id"] for e in spec["inputs"]}
        out_ids = {e["param_id"] for e in spec["outputs"]}
        for k, v in params.items():
            target = produced if k in out_ids else consumed if k in in_ids else None
            if target is None: continue
            for s in _strings(v):
                if s: target.add(s)
    else:
        # An app the catalog no longer carries. Everything it names is a
        # candidate on both sides rather than dropping the node.
        for s in _strings(params):
            if s: consumed.add(s)
    return consumed, produced, set(cell.get("result_refs") or [])


def build(narr, cat) -> dict:
    cells = [c for c in narr["cells"] if c["kind"] == "app"]
    io = [cell_io(c, cat) for c in cells]

    edges = []
    for j, (cons_j, _p, _r) in enumerate(io):
        for i in range(j):
            cons_i, prod_i, refs_i = io[i]
            by_ref = refs_i & cons_j
            by_name = prod_i & cons_j
            if by_ref:
                edges.append({"from": i, "to": j, "via": "ref", "on": sorted(by_ref)[:3]})
            elif by_name:
                edges.append({"from": i, "to": j, "via": "name", "on": sorted(by_name)[:3]})

    incoming = {e["to"] for e in edges}
    nodes = [{
        "i": n, "app_id": c["app_id"], "tag": c.get("tag"), "version": c.get("version"),
        "in_catalog": c["app_id"] in cat,
        "status": (c.get("job_status") if isinstance(c.get("job_status"), str) else None),
    } for n, c in enumerate(cells)]

    return {
        "ref": narr["ref"], "ws_id": narr["ws_id"], "obj_id": narr["obj_id"],
        "title": narr.get("title"), "creator": narr.get("creator"),
        "modified_at": narr.get("modified_at"),
        "n_nodes": len(nodes), "n_edges": len(edges),
        "n_ref_edges": sum(1 for e in edges if e["via"] == "ref"),
        "n_name_edges": sum(1 for e in edges if e["via"] == "name"),
        "roots": [n["i"] for n in nodes if n["i"] not in incoming],
        "unknown_apps": sorted({n["app_id"] for n in nodes if not n["in_catalog"]}),
        "apps": [n["app_id"] for n in nodes],
        "signature": "|".join(n["app_id"] for n in nodes),
        "nodes": nodes, "edges": edges,
    }


def connected_fraction(d) -> float:
    if d["n_nodes"] < 2: return 0.0
    touched = {e["from"] for e in d["edges"]} | {e["to"] for e in d["edges"]}
    return len(touched) / d["n_nodes"]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.parse_args()

    cat = catalog()
    narrs = [json.loads(l) for l in CELLS.read_text().splitlines() if l]
    dags = [build(n, cat) for n in narrs]
    DAGS.write_text("".join(json.dumps(d) + "\n" for d in dags))

    with_apps = [d for d in dags if d["n_nodes"] > 0]
    multi = [d for d in with_apps if d["n_nodes"] > 1]
    wired = [d for d in multi if d["n_edges"] > 0]
    sigs = collections.Counter(d["signature"] for d in wired)
    unknown = collections.Counter(a for d in dags for a in d["unknown_apps"])

    md = [
        "# Narrative DAGs\n\n",
        "One record per public narrative in `dags.jsonl`. Edges come from an app cell's\n",
        "job result or its output-name parameter meeting a later cell's input.\n\n",
        "| | |\n|---|---|\n",
        f"| narratives extracted | {len(narrs)} |\n",
        f"| with at least one app cell | {len(with_apps)} |\n",
        f"| with more than one app cell | {len(multi)} |\n",
        f"| with at least one edge | {len(wired)} |\n",
        f"| total app cells | {sum(d['n_nodes'] for d in dags)} |\n",
        f"| total edges | {sum(d['n_edges'] for d in dags)} |\n",
        f"| edges from a workspace reference | {sum(d['n_ref_edges'] for d in dags)} |\n",
        f"| edges from a name match only | {sum(d['n_name_edges'] for d in dags)} |\n",
        f"| distinct apps used | {len({a for d in dags for a in d['apps']})} |\n",
        f"| distinct wired shapes (app sequences) | {len(sigs)} |\n",
        f"| apps used but absent from the catalog | {len(unknown)} |\n",
        "\n## Most repeated wired shapes\n\n| copies | steps | app sequence |\n|---|---|---|\n",
    ]
    for sig, n in sigs.most_common(15):
        steps = sig.split("|")
        md.append(f"| {n} | {len(steps)} | {' -> '.join(s.split('/')[-1] for s in steps[:6])}"
                  f"{' ...' if len(steps) > 6 else ''} |\n")
    if unknown:
        md.append("\n## Most used apps the catalog no longer carries\n\n| uses | app |\n|---|---|\n")
        for a, n in unknown.most_common(12):
            md.append(f"| {n} | {a} |\n")
    SUMMARY.write_text("".join(md))
    print("".join(md[3:14]))
    print(f"wrote {DAGS.relative_to(REPO)} and {SUMMARY.relative_to(REPO)}")


if __name__ == "__main__":
    main()
