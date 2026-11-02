#!/usr/bin/env python3
"""Emit report_data.json: every number the round's report shows, read from the tables.

The report is a published page rather than a file in the tree, so nothing it says
may be typed by hand. This reads the round's own outputs and the port's catalog and
writes the one blob the page embeds.

    report_data.py
"""

from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
KBASE = HERE.parent.parent
CATALOG = KBASE / "catalog"
R1 = KBASE / "curation" / "r1"


def rows(path: Path) -> list:
    return [json.loads(l) for l in path.read_text().splitlines() if l]


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()

    census = json.loads((CATALOG / "census.json").read_text())
    vocab = yaml.safe_load((HERE / "tasks.yml").read_text())
    onto = yaml.safe_load((HERE / "types.yml").read_text())
    cfg = yaml.safe_load((HERE / "config.yml").read_text())
    app_tasks = rows(HERE / "app_tasks.jsonl")
    task_io = {r["task"]: r for r in rows(HERE / "task_io.jsonl")}
    type_map = rows(HERE / "type_map.jsonl")
    param_map = rows(HERE / "param_map.jsonl")
    workflows = rows(HERE / "workflows.jsonl")
    shapes = rows(R1 / "shapes.jsonl")
    apps = {a["app_id"]: a for a in rows(CATALOG / "apps.jsonl")}

    spec = {t["name"]: t for t in vocab["tasks"]}
    plumbing = {n for n, t in spec.items() if t["plumbing"]}
    reporting = {n for n, t in spec.items() if t["produces"] == ["report"]}

    knobs = [r for r in param_map if r["disposition"] in ("knob", "passthrough")]
    curated_knobs = [r for r in knobs if r["canonical_method"] and r["is_task"]
                     and r["task"] not in plumbing]

    type_rows = [r for r in type_map if r["kind"] == "type"]
    unions = [r for r in type_map if r["kind"] == "union"]
    book = {"status", "provenance"}
    meanings = {(r["entity"], tuple(sorted((k, v) for k, v in r["axes"].items()
                                           if k not in book))) for r in type_rows}

    total_copies = sum(r["copies"] for r in workflows)
    curve, run = [], 0
    for i, r in enumerate(sorted(workflows, key=lambda x: -x["copies"]), start=1):
        run += r["copies"]
        curve.append([i, round(100 * run / total_copies, 3)])

    verbs = []
    for name, t in spec.items():
        io = task_io[name]
        verbs.append({
            "task": name,
            "group": t["group"],
            "means": t["means"],
            "plumbing": t["plumbing"],
            "reporting": name in reporting,
            "apps": io["apps"],
            "canonical": io["canonical"],
            "canonical_name": apps[io["canonical"]]["name"],
            "copies": io["canonical_copies"],
            "ws_types_in": io["ws_types_in"],
            "consumes": t["consumes"],
            "produces": t["produces"],
            "kbase_consumes": io["canonical_consumes"],
            "kbase_produces": io["canonical_produces"],
            "untyped": io["untyped_canonical"],
            "knobs": sum(1 for r in curated_knobs if r["task"] == name),
        })

    entity_absorb = collections.Counter(r["entity"] for r in type_rows)
    dispositions = collections.Counter(r["disposition"] for r in param_map)

    data = {
        "generated_from": "research/kbase/curation/r2",
        "census_date": census["fetched"],
        "funnels": [
            {"axis": "Apps", "before": census["apps"]["total"],
             "before_label": "apps in the catalog",
             "after": len(spec), "after_label": "core task verbs",
             "note": f"{len(spec) - len(plumbing)} of them scientific, "
                     f"{len(plumbing)} plumbing"},
            {"axis": "Types", "before": census["types"]["referenced"],
             "before_label": "workspace types",
             "after": len(entity_absorb), "after_label": "entities",
             "note": f"{len(meanings)} fully specified meanings on a "
                     f"{len(onto['axes'])}-axis grid"},
            {"axis": "Config", "before": len(param_map),
             "before_label": "declared parameters",
             "after": len(curated_knobs), "after_label": "config fields",
             "note": f"{len({r['normalised'] for r in curated_knobs})} distinct names "
                     f"in {len({r['task'] for r in curated_knobs})} config types"},
            {"axis": "Workflows", "before": len(shapes),
             "before_label": "narrative workflow shapes",
             "after": len(workflows), "after_label": "canonical workflows",
             "note": f"{len({r['name'] for r in workflows if r['name']})} distinct "
                     f"product-from-source names"},
        ],
        "verbs": verbs,
        "groups": vocab["groups"],
        "non_tasks": [{"name": e["name"], "means": e["means"],
                       "apps": sum(1 for r in app_tasks if r["non_task"] == e["name"])}
                      for e in vocab["non_tasks"]],
        "types": {
            "referenced": len(type_rows),
            "entities": len(entity_absorb),
            "meanings": len(meanings),
            "axes": len(onto["axes"]),
            "families": len(onto["families"]),
            "absorbed": entity_absorb.most_common(),
            "unmapped_entities": sorted(set(onto["entities"]) - set(entity_absorb)),
            "unions": {
                "total": len(unions),
                "entity": sum(1 for r in unions if r["level"] == "entity"),
                "family": sum(1 for r in unions if r["level"] == "family"),
                "none": sum(1 for r in unions if r["level"] == "none"),
                "survivors": [{"entities": r["entities"]} for r in unions
                              if r["level"] == "none"],
            },
            "family_means": {n: s["means"] for n, s in onto["families"].items()},
        },
        "params": {
            "declared": len(param_map),
            "distinct_ids": len({r["param_id"] for r in param_map}),
            "normalised": len({r["normalised"] for r in param_map}),
            "dispositions": [
                {"name": d["name"], "means": d["means"], "home": d["home"],
                 "declarations": dispositions.get(d["name"], 0),
                 "names": len({r["normalised"] for r in param_map
                               if r["disposition"] == d["name"]})}
                for d in cfg["dispositions"]],
            "knobs": len(knobs),
            "curated": len(curated_knobs),
            "curated_names": len({r["normalised"] for r in curated_knobs}),
            "config_types": len({r["task"] for r in curated_knobs}),
            "common": [{"name": k, "means": v["means"], "kind": v["kind"],
                        "tasks": len({r["task"] for r in knobs if r["normalised"] == k})}
                       for k, v in cfg["common"].items()],
        },
        "workflows": {
            "shapes": len(shapes),
            "copies": total_copies,
            "canonical": len(workflows),
            "names": len({r["name"] for r in workflows if r["name"]}),
            "curve": curve,
            "top": [{"name": r["name"], "tasks": r["tasks"], "copies": r["copies"],
                     "shapes": r["shapes"], "typed": r["product_is_typed"]}
                    for r in workflows[:25]],
            "by_size": sorted(collections.Counter(
                r["n_tasks"] for r in workflows).items()),
        },
        "gaps": {
            "untyped_verbs": [{"task": v["task"], "canonical": v["canonical"],
                               "apps": v["apps"], "copies": v["copies"]}
                              for v in verbs if v["untyped"]],
            "no_workspace_type": sorted(set(onto["entities"]) - set(entity_absorb)),
            "corpus_apps_absent_from_catalog": len(
                {a for s in shapes for a in s["apps"]} - set(apps)),
        },
    }
    (HERE / "report_data.json").write_text(json.dumps(data, indent=1))
    print(f"wrote report_data.json: {len(json.dumps(data))} bytes, "
          f"{len(verbs)} verbs, {len(curve)} curve points")


if __name__ == "__main__":
    main()
