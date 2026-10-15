"""Emit proposals.jsonl and the parity numbers from proposals.yml.

Every count comes from curation/r2 and catalog/, never from proposals.yml. The
gate asserts the two name the same 54 verbs before anything is written.
"""

import json
from collections import Counter, defaultdict
from pathlib import Path

import yaml

HERE = Path(__file__).parent
R2 = HERE.parent / "r2"

PLUMBING_VERBS = {"build_set", "manage_samples", "import_other", "export", "view",
                  "merge_reads", "convert_reads"}
SETTLED = {"covered", "deferred", "plumbing"}


def load():
    spec = yaml.safe_load((HERE / "proposals.yml").read_text())
    task_io = {json.loads(l)["task"]: json.loads(l) for l in (R2 / "task_io.jsonl").open()}
    workflows = [json.loads(l) for l in (R2 / "workflows.jsonl").open()]
    copies = Counter()
    for line in (R2 / "app_tasks.jsonl").open():
        row = json.loads(line)
        if row["kind"] == "task":
            copies[row["task"]] += row["copies"]
    return spec, task_io, workflows, copies


def reachable(workflows, available):
    ok = [w for w in workflows if all(t in available or t in PLUMBING_VERBS for t in w["tasks"])]
    return len(ok), sum(w["copies"] for w in ok)


def check(spec, task_io):
    seen = [v["verb"] for v in spec["verbs"]]
    assert len(seen) == len(set(seen)), "a verb appears twice in proposals.yml"
    assert set(seen) == set(task_io), "proposals.yml and r2/task_io.jsonl name different verbs"
    domains = set(spec["domains"])
    for v in spec["verbs"]:
        assert v["domain"] in domains, f"{v['verb']}: unknown domain {v['domain']}"
        assert (v["status"] == "declined") == ("reason" in v), \
            f"{v['verb']}: declined needs a reason, and only declined carries one"
        assert not (v["status"] in SETTLED
                    and any(not p.get("optional") for p in v.get("proposals", []))), \
            f"{v['verb']}: a settled verb carries only optional proposals"
        for p in v.get("proposals", []):
            assert p["action"] in {"write", "enable", "adopt", "extend", "blocked"}, f"{p['name']}: unknown action"
            assert p.get("spec"), f"{p['name']}: every proposal states what it buys"
            for t in p["consumes"] + p["produces"]:
                assert "::" in t, f"{p['name']}: {t} is not a namespaced type"
    serving = yaml.safe_load((HERE / "serving.yml").read_text())["serving"]
    assert set(serving) == set(task_io), "serving.yml and task_io.jsonl name different verbs"
    troot = Path("src/metasmith_libraries/transforms")
    for v, ps in serving.items():
        for path in ps:
            assert (troot / path).exists(), f"serving.yml: {path} does not exist"
    for r in spec["refactors"]:
        assert r["domain"] in domains, f"refactor {r['name']}: unknown domain"


def main():
    spec, task_io, workflows, copies = load()
    check(spec, task_io)

    by_verb = {v["verb"]: v for v in spec["verbs"]}
    closed_by_refactor = {t for r in spec["refactors"] for t in r["closes"]}
    folded = {v["verb"] for v in spec["verbs"] if v.get("closed_by")}

    rows = []
    for v in spec["verbs"]:
        for p in v.get("proposals", []):
            rows.append({
                "transform": p["name"], "verb": v["verb"], "domain": v["domain"],
                "action": p["action"], "optional": bool(p.get("optional")), "spec": p["spec"], "consumes": p["consumes"], "produces": p["produces"],
                "narrative_copies": copies[v["verb"]],
                "canonical_app": task_io[v["verb"]]["canonical"],
                "adopt_from": v.get("adopt_from", ""),
                "note": p.get("note", v.get("note", "")),
            })
    order = list(spec["domains"])
    with (HERE / "proposals.jsonl").open("w") as f:
        for r in sorted(rows, key=lambda r: (order.index(r["domain"]), -r["narrative_copies"])):
            f.write(json.dumps(r) + "\n")

    settled = {v["verb"] for v in spec["verbs"] if v["status"] in SETTLED}
    serving = yaml.safe_load((HERE / "serving.yml").read_text())["serving"]
    proposed = {v["verb"] for v in spec["verbs"]
                if any(not p.get("optional") for p in v.get("proposals", []))}

    stages = [("today", settled),
              ("+ proposals", settled | proposed | folded),
              ("+ refactors", settled | proposed | folded | closed_by_refactor)]
    ladder = []
    for label, avail in stages:
        n, c = reachable(workflows, avail)
        ladder.append({"stage": label, "workflows": n, "copies": c})

    residual = Counter()
    final = settled | proposed | folded | closed_by_refactor
    for w in workflows:
        for t in set(w["tasks"]):
            if t not in final and t not in PLUMBING_VERBS:
                residual[t] += w["copies"]

    per_domain = defaultdict(lambda: Counter())
    for v in spec["verbs"]:
        per_domain[v["domain"]][v["status"]] += 1
    for r in rows:
        per_domain[r["domain"]]["transforms"] += 1

    # one row per catalog app: what serves it, and under which disposition
    prop_by_verb = {}
    for row in rows:
        prop_by_verb.setdefault(row["verb"], []).append(row["transform"])
    apps, app_stat = [], Counter()
    for line in (R2 / "app_tasks.jsonl").open():
        a = json.loads(line)
        verb = a["task"]
        v = by_verb.get(verb) if verb else None
        ex = serving.get(verb, []) if verb else []
        pr = prop_by_verb.get(verb, [])
        if v is None:
            served, how = [], "non_task"
        elif v["status"] == "plumbing":
            served, how = [], "plumbing"
        elif v["status"] == "deferred":
            served, how = ex, "deferred"
        elif verb in closed_by_refactor and not pr:
            served, how = ex, "refactor"
        elif v["status"] == "declined":
            served, how = ([prop_by_verb[v["closed_by"].split("/")[-1]]] if False else []), "declined"
            if v.get("closed_by"):
                served, how = [v["closed_by"]], "folded"
        elif pr and ex:
            served, how = ex + pr, "extended"
        elif pr:
            served, how = pr, "proposed"
        else:
            served, how = ex, "existing"
        app_stat[how] += 1
        apps.append({"app_id": a["app_id"], "name": a.get("name", ""), "verb": verb,
                     "non_task": a.get("non_task"), "active": a["active"],
                     "copies": a["copies"], "canonical": a.get("canonical", False),
                     "domain": v["domain"] if v else "", "how": how, "served_by": served})
    with (HERE / "app_map.jsonl").open("w") as f:
        for a in sorted(apps, key=lambda a: (-a["copies"], a["app_id"])):
            f.write(json.dumps(a) + "\n")

    summary = {
        "verbs": len(spec["verbs"]),
        "apps": len(apps), "app_disposition": dict(app_stat),
        "existing_transforms_cited": len({p for ps in serving.values() for p in ps}), "proposals": len(rows),
        "refactors": len(spec["refactors"]),
        "new_types": len(spec["new_types"]), "new_envs": len(spec["new_envs"]),
        "status": dict(Counter(v["status"] for v in spec["verbs"])),
        "workflows": len(workflows), "copies": sum(w["copies"] for w in workflows),
        "ladder": ladder, "residual": dict(residual),
        "per_domain": {d: dict(c) for d, c in per_domain.items()},
    }
    (HERE / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    tot_n, tot_c = len(workflows), summary["copies"]
    print(json.dumps(summary["status"]))
    print(f"{len(rows)} transforms, {len(spec['refactors'])} refactors, "
          f"{summary['new_types']} new types, {summary['new_envs']} new envs")
    for s in ladder:
        print(f"  {s['stage']:<14} {s['workflows']:>4}/{tot_n} workflows ({100*s['workflows']/tot_n:>3.0f}%)"
              f"  {s['copies']:>5}/{tot_c} copies ({100*s['copies']/tot_c:>3.0f}%)")
    if residual:
        print("  residual:", dict(residual))
    print()
    print()
    print(f"  {len(apps)} catalog apps ->", dict(app_stat))
    print(f"  {summary['existing_transforms_cited']} existing transforms cited, {len(rows)} proposed")
    print()
    for d in order:
        c = per_domain[d]
        print(f"  {d:<16} {c.get('transforms',0)} transforms | " +
              " ".join(f"{k}={v}" for k, v in sorted(c.items()) if k != "transforms"))


if __name__ == "__main__":
    main()
