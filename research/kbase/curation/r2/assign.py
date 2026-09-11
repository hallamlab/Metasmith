#!/usr/bin/env python3
"""Assign every catalog app a core task, and name the canonical method per task.

Reads `tasks.yml` and `catalog/apps.jsonl`. Writes `app_tasks.jsonl`, one row per
app: its verb or non-task disposition, the rule that assigned it, how many public
narrative copies ran it, and whether it is the proposed canonical method for its
verb.

The canonical method is the implementation scientists actually ran. Rank by
narrative copies first, then by whether KBase still marks the app active, then by
whether it declares a typed output at all. Nothing here judges which tool is
better -- the corpus decides, and `curation/r1/shapes.jsonl` is where it says so.

    assign.py [--verbose]
"""

from __future__ import annotations

import argparse
import collections
import json
import re
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
KBASE = HERE.parent.parent
CATALOG = KBASE / "catalog"
R1 = KBASE / "curation" / "r1"


def load_vocabulary() -> dict:
    return yaml.safe_load((HERE / "tasks.yml").read_text())


def usage_by_app() -> collections.Counter:
    """Narrative copies per app, over every wired shape rather than the candidates.

    A shape's copy count is how many scientists ran it, so an app used once in a
    shape copied forty times weighs forty. An app appearing twice in one shape is
    counted once: the question is how many workflows reach for it.
    """
    used = collections.Counter()
    for line in (R1 / "shapes.jsonl").read_text().splitlines():
        if not line:
            continue
        row = json.loads(line)
        for app in set(row["apps"]):
            used[app] += row["copies"]
    return used


def compile_rules(vocab: dict) -> list:
    out = []
    for entry in vocab["tasks"]:
        for pattern in entry["match"]:
            out.append(("task", entry["name"], pattern, re.compile(pattern)))
    for entry in vocab["non_tasks"]:
        for pattern in entry["match"]:
            out.append(("non_task", entry["name"], pattern, re.compile(pattern)))
    return out


def assign(app_id: str, rules: list, exceptions: dict) -> tuple:
    if app_id in exceptions:
        return "task", exceptions[app_id], "exception"
    for kind, name, pattern, rx in rules:
        if rx.search(app_id):
            return kind, name, pattern
    return "unassigned", None, None


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--verbose", action="store_true", help="list every unassigned app")
    args = ap.parse_args()

    vocab = load_vocabulary()
    rules = compile_rules(vocab)
    exceptions = vocab.get("exceptions") or {}
    non_task_names = {e["name"] for e in vocab["non_tasks"]}
    for app_id, name in exceptions.items():
        if name in non_task_names:
            raise SystemExit(f"exception {app_id} names a non_task: use the match list instead")

    apps = [json.loads(l) for l in (CATALOG / "apps.jsonl").read_text().splitlines() if l]
    used = usage_by_app()

    rows = []
    for a in apps:
        kind, name, evidence = assign(a["app_id"], rules, exceptions)
        rows.append({
            "app_id": a["app_id"],
            "kind": kind,
            "task": name if kind == "task" else None,
            "non_task": name if kind == "non_task" else None,
            "evidence": evidence,
            "active": a["active"],
            "app_type": a["app_type"],
            "typed_outputs": bool(a["outputs"]),
            "copies": used.get(a["app_id"], 0),
            "name": a["name"],
        })

    unassigned = [r for r in rows if r["kind"] == "unassigned"]
    by_task = collections.defaultdict(list)
    for r in rows:
        if r["kind"] == "task":
            by_task[r["task"]].append(r)
    for task, members in by_task.items():
        best = max(members, key=lambda r: (r["copies"], r["active"], r["typed_outputs"], r["app_id"]))
        for r in members:
            r["canonical"] = r is best

    (HERE / "app_tasks.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in sorted(rows, key=lambda r: r["app_id"])))

    declared = [e["name"] for e in vocab["tasks"]]
    print(f"apps {len(rows)} | verbs declared {len(declared)} | verbs used {len(by_task)}")
    print(f"non-task {sum(1 for r in rows if r['kind'] == 'non_task')} | "
          f"unassigned {len(unassigned)}")
    empty = [v for v in declared if v not in by_task]
    if empty:
        print(f"verbs matching no app: {empty}")
    if unassigned and args.verbose:
        for r in unassigned:
            print(f"  UNASSIGNED {r['app_id']:60s} {r['name'][:44]}")
    print()
    for task in declared:
        members = by_task.get(task, [])
        if not members:
            continue
        canon = next(r for r in members if r["canonical"])
        print(f"  {task:24s} {len(members):3d} apps  canonical={canon['app_id']} "
              f"({canon['copies']} copies)")


if __name__ == "__main__":
    main()
