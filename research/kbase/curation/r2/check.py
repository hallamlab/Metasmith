#!/usr/bin/env python3
"""Reconcile every table this round writes against the port's own denominators.

The proposal is a set of tables, not a document, and this is what makes that worth
something. Each assertion joins back to `catalog/census.json`, `catalog/apps.jsonl`,
`catalog/params.jsonl`, `catalog/types.jsonl` or `curation/r1/shapes.jsonl` -- never
to the file being checked, because a table that agrees with itself proves nothing.

Exits non-zero on the first failure, naming the join that broke.

    check.py [--quiet]
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
KBASE = HERE.parent.parent
CATALOG = KBASE / "catalog"
R1 = KBASE / "curation" / "r1"

FAILURES = []


def rows(path: Path) -> list:
    return [json.loads(l) for l in path.read_text().splitlines() if l]


def check(label: str, ok: bool, detail: str = ""):
    print(f"  {'PASS' if ok else 'FAIL'}  {label}" + (f" -- {detail}" if detail else ""))
    if not ok:
        FAILURES.append(label)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--quiet", action="store_true", help="print only failures")
    args = ap.parse_args()

    census = json.loads((CATALOG / "census.json").read_text())
    apps = rows(CATALOG / "apps.jsonl")
    params = rows(CATALOG / "params.jsonl")
    ws_types = rows(CATALOG / "types.jsonl")
    shapes = rows(R1 / "shapes.jsonl")

    vocab = yaml.safe_load((HERE / "tasks.yml").read_text())
    onto = yaml.safe_load((HERE / "types.yml").read_text())
    cfg = yaml.safe_load((HERE / "config.yml").read_text())
    app_tasks = rows(HERE / "app_tasks.jsonl")
    type_map = rows(HERE / "type_map.jsonl")
    task_io = rows(HERE / "task_io.jsonl")
    param_map = rows(HERE / "param_map.jsonl")
    workflows = rows(HERE / "workflows.jsonl")

    print("tasks")
    check("every census app carries a disposition",
          len(app_tasks) == census["apps"]["total"],
          f"{len(app_tasks)} rows against a census of {census['apps']['total']}")
    check("no app is unassigned",
          not [r for r in app_tasks if r["kind"] == "unassigned"])
    check("every app id in app_tasks is a catalog app id",
          {r["app_id"] for r in app_tasks} == {a["app_id"] for a in apps})
    verbs = {t["name"] for t in vocab["tasks"]}
    used = {r["task"] for r in app_tasks if r["kind"] == "task"}
    check("every declared verb has at least one app", used == verbs,
          f"unused: {sorted(verbs - used)}" if verbs - used else "")
    per_verb = collections.Counter(r["task"] for r in app_tasks if r["kind"] == "task")
    canon = collections.Counter(r["task"] for r in app_tasks
                                if r["kind"] == "task" and r["canonical"])
    check("exactly one canonical method per verb",
          all(canon[v] == 1 for v in verbs),
          f"{[v for v in verbs if canon[v] != 1]}")
    check("every verb in task_io is a declared verb",
          {r["task"] for r in task_io} == verbs)
    check("task_io app counts match app_tasks",
          all(r["apps"] == per_verb[r["task"]] for r in task_io))

    print("types")
    referenced = {t["ws_type"] for t in ws_types}
    mapped = {r["ws_type"] for r in type_map if r["kind"] == "type"}
    check("every referenced workspace type is mapped", referenced == mapped,
          f"{len(referenced)} referenced, {len(mapped)} mapped")
    check("the census type count agrees",
          len(mapped) == census["types"]["referenced"])
    check("every mapped type names a declared entity",
          all(r["entity"] in onto["entities"] for r in type_map if r["kind"] == "type"))
    unions = [r for r in type_map if r["kind"] == "union"]
    from_catalog = {tuple(sorted(set(p["ws_types"]))) for a in apps for p in a["inputs"]
                    if len(set(p["ws_types"])) > 1}
    check("every parameter union in the catalog has a row",
          {tuple(r["members"]) for r in unions} == from_catalog,
          f"{len(unions)} rows against {len(from_catalog)} unions")
    check("every union resolves to an entity, a family, or is recorded as heterogeneous",
          all(r["level"] in ("entity", "family", "none") for r in unions))
    survivors = [r for r in unions if r["level"] == "none"]
    plumbing = {t["name"] for t in vocab["tasks"] if t["plumbing"]}
    non_task = {e["name"] for e in vocab["non_tasks"]}
    verb_of = {r["app_id"]: (r["task"] or r["non_task"]) for r in app_tasks}
    users = {}
    for a in apps:
        for p in a["inputs"]:
            k = tuple(sorted(set(p["ws_types"])))
            if len(k) > 1:
                users.setdefault(k, set()).add(a["app_id"])
    check("every surviving union belongs only to a plumbing or non-task verb",
          all(all(verb_of[a] in plumbing | non_task for a in users[tuple(r["members"])])
              for r in survivors),
          f"{len(survivors)} survivors")

    print("config")
    check("every declared parameter carries a disposition",
          len(param_map) == len(params),
          f"{len(param_map)} rows against {len(params)} declared parameters")
    check("typed_io and object_name counts match the spec's own ui_class",
          sum(1 for r in param_map if r["disposition"] == "typed_io")
          == sum(1 for p in params if p["ui_class"] == "input")
          and sum(1 for r in param_map if r["disposition"] == "object_name")
          == sum(1 for p in params if p["ui_class"] == "output"))
    check("every param_map row names an app in the catalog",
          {r["app_id"] for r in param_map} <= {a["app_id"] for a in apps})
    seen = collections.defaultdict(set)
    for r in param_map:
        if r["disposition"] in ("knob", "passthrough"):
            seen[r["normalised"]].add(r["task"])
    shared = {k: len(seen.get(k, ())) for k in cfg["common"]}
    check("every common knob is declared by at least two tasks",
          all(n >= 2 for n in shared.values()),
          f"{sorted(k for k, n in shared.items() if n < 2)}")

    print("workflows")
    check("every round 1 shape lands in exactly one workflow",
          sum(r["shapes"] for r in workflows) == len(shapes),
          f"{sum(r['shapes'] for r in workflows)} against {len(shapes)} shapes")
    check("copies are conserved",
          sum(r["copies"] for r in workflows) == sum(s["copies"] for s in shapes),
          f"{sum(r['copies'] for r in workflows)} copies")
    check("every workflow's task set is drawn from the vocabulary",
          all(set(r["tasks"]) <= verbs for r in workflows))
    check("no workflow names a plumbing or reporting verb",
          not [r for r in workflows for t in r["tasks"]
               if t in plumbing or {t2["name"] for t2 in vocab["tasks"]
                                    if t2["produces"] == ["report"]} & {t}])
    check("workflow ids are unique and dense",
          {r["workflow_id"] for r in workflows} ==
          {f"w{i:04d}" for i in range(1, len(workflows) + 1)})

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILED: {FAILURES}")
        sys.exit(1)
    print("all checks pass")


if __name__ == "__main__":
    main()
