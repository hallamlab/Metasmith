#!/usr/bin/env python3
"""Disposition every declared KBase parameter, and size the curated config surface.

Reads `config.yml`, `catalog/params.jsonl` and `app_tasks.jsonl`. Writes
`param_map.jsonl`, one row per declared parameter: its normalised name, the core
task that declares it, its disposition, and whether the app declaring it is the
proposed canonical method for that task.

The number the round wants out of this is how much of the 1,885-knob surface a
curated library would actually carry. Two filters do the work. Most knobs are not
knobs -- they name an output, describe an input or style a report -- and most of
the survivors belong to an implementation the curation demotes.

    map_params.py [--residue]
"""

from __future__ import annotations

import argparse
import collections
import json
import re
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
CATALOG = HERE.parent.parent / "catalog"


def normaliser(cfg: dict):
    abbr = cfg["normalisation"]["abbreviations"]

    def norm(name: str) -> str:
        s = re.sub(r"(?<!^)(?=[A-Z])", "_", name or "")
        s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").lower()
        return "_".join(abbr.get(w, w) for w in s.split("_"))

    return norm


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--residue", action="store_true",
                    help="list the most declared knobs that survive both filters")
    args = ap.parse_args()

    cfg = yaml.safe_load((HERE / "config.yml").read_text())
    norm = normaliser(cfg)
    kind_of = {ft: kind for kind, spec in cfg["kinds"].items() for ft in spec["from"]}
    by_ui_class, patterns = {}, []
    for d in cfg["dispositions"]:
        for ui in d.get("ui_class") or []:
            by_ui_class[ui] = d["name"]
        for p in d.get("match") or []:
            patterns.append((d["name"], re.compile(p)))

    params = [json.loads(l) for l in (CATALOG / "params.jsonl").read_text().splitlines() if l]
    assigned = {json.loads(l)["app_id"]: json.loads(l)
                for l in (HERE / "app_tasks.jsonl").read_text().splitlines() if l}
    canonical = {r["app_id"] for r in assigned.values() if r.get("canonical")}
    common = set(cfg["common"])

    rows = []
    for p in params:
        who = assigned[p["app_id"]]
        name = norm(p["param_id"])
        if p["ui_class"] in by_ui_class:
            disposition = by_ui_class[p["ui_class"]]
        else:
            disposition = next(n for n, rx in patterns if rx.search(name))
        rows.append({
            "app_id": p["app_id"],
            "param_id": p["param_id"],
            "normalised": name,
            "task": who["task"] or who["non_task"],
            "is_task": who["kind"] == "task",
            "canonical_method": p["app_id"] in canonical,
            "ui_class": p["ui_class"],
            "kind": kind_of.get(p["field_type"]),
            "disposition": disposition,
            "optional": p["optional"],
            "advanced": p["advanced"],
            "shared": name in common,
        })

    (HERE / "param_map.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))

    order = [d["name"] for d in cfg["dispositions"]]
    counts = collections.Counter(r["disposition"] for r in rows)
    print(f"declared parameters {len(rows)}, distinct ids {len({r['param_id'] for r in rows})}, "
          f"normalised to {len({r['normalised'] for r in rows})}")
    for name in order:
        n = counts.get(name, 0)
        ids = len({r["normalised"] for r in rows if r["disposition"] == name})
        print(f"  {name:20s} {n:5d} declarations  {ids:4d} distinct names")

    knobs = [r for r in rows if r["disposition"] in ("knob", "passthrough")]
    curated = [r for r in knobs if r["canonical_method"] and r["is_task"]]
    print(f"\nknobs {len(knobs)} over {len({r['normalised'] for r in knobs})} names")
    print(f"knobs on a canonical method {len(curated)} over "
          f"{len({r['normalised'] for r in curated})} names")
    verbs = collections.Counter(r["task"] for r in curated)
    print(f"config types needed: {len(verbs)}, "
          f"median {sorted(verbs.values())[len(verbs) // 2]} fields, max {max(verbs.values())}")
    vocab = yaml.safe_load((HERE / "tasks.yml").read_text())
    plumbing = {t["name"] for t in vocab["tasks"] if t["plumbing"]}
    science = [r for r in curated if r["task"] not in plumbing]
    sverbs = collections.Counter(r["task"] for r in science)
    print(f"dropping the 7 plumbing verbs: {len(science)} knobs over "
          f"{len({r['normalised'] for r in science})} names in {len(sverbs)} config types")

    seen = collections.defaultdict(set)
    for r in knobs:
        seen[r["normalised"]].add(r["task"])
    declared_common = {k: len(seen.get(k, ())) for k in common}
    absent = sorted(k for k, n in declared_common.items() if n == 0)
    private = sorted(k for k, n in declared_common.items() if n == 1)
    if absent:
        print(f"\ncommon vocabulary naming a knob no app declares: {absent}")
    if private:
        print(f"common vocabulary naming a knob only one task declares: {private}")

    if args.residue:
        counted = collections.Counter(r["normalised"] for r in curated)
        print("\nmost declared knobs surviving both filters:")
        for name, n in counted.most_common(25):
            tasks = sorted(seen[name])
            print(f"  {n:3d}  {name:34s} {tasks[:4]}")


if __name__ == "__main__":
    main()
