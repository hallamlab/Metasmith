#!/usr/bin/env python3
"""Derive each core task's canonical signature from the apps that implement it.

`tasks.yml` proposes what a verb ought to consume and produce. This reads what its
apps actually declare, through `type_map.jsonl`, and writes `task_io.jsonl`: the
entities every implementation touches, the narrowest family covering them, and the
canonical method's own signature.

The gap between the two is the finding rather than a defect. Thirteen verbs' most
used implementation declares no typed output at all, so the chain's end is untyped
and nothing downstream can ask for it. One verb proposes a role no workspace type
carries: every taxonomic classifier reports its assignment and stores nothing.

    task_io.py [--diff]
"""

from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
CATALOG = HERE.parent.parent / "catalog"


def load():
    vocab = yaml.safe_load((HERE / "tasks.yml").read_text())
    onto = yaml.safe_load((HERE / "types.yml").read_text())
    entity_of = {}
    for line in (HERE / "type_map.jsonl").read_text().splitlines():
        row = json.loads(line)
        if row["kind"] == "type":
            entity_of[row["ws_type"]] = row["entity"]
    apps = {json.loads(l)["app_id"]: json.loads(l)
            for l in (CATALOG / "apps.jsonl").read_text().splitlines() if l}
    assigned = [json.loads(l) for l in (HERE / "app_tasks.jsonl").read_text().splitlines() if l]
    return vocab, onto, entity_of, apps, assigned


def narrowest_family(entities: set, families: dict) -> str | None:
    fits = sorted((len(pool), name) for name, pool in families.items() if entities <= pool)
    return fits[0][1] if fits else None


def role_for(entities: set, families: dict) -> str:
    """The single role a slot accepting `entities` should name."""
    if not entities:
        return "none"
    if len(entities) == 1:
        return next(iter(entities))
    return narrowest_family(entities, families) or "any"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--diff", action="store_true",
                    help="print only the verbs where the proposal and KBase disagree")
    args = ap.parse_args()

    vocab, onto, entity_of, apps, assigned = load()
    families = {name: set(spec["entities"]) for name, spec in onto["families"].items()}
    declared = {t["name"]: t for t in vocab["tasks"]}

    members = collections.defaultdict(list)
    for row in assigned:
        if row["kind"] == "task":
            members[row["task"]].append(row)

    rows = []
    for verb, spec in declared.items():
        mine = members[verb]
        canon = next(r for r in mine if r["canonical"])
        in_types, out_types = set(), set()
        for r in mine:
            a = apps[r["app_id"]]
            in_types |= {t for p in a["inputs"] for t in p["ws_types"]}
            out_types |= {t for p in a["outputs"] for t in p["ws_types"]}
        ca = apps[canon["app_id"]]
        canon_in = {t for p in ca["inputs"] for t in p["ws_types"]}
        canon_out = {t for p in ca["outputs"] for t in p["ws_types"]}

        in_ents = {entity_of[t] for t in in_types}
        out_ents = {entity_of[t] for t in out_types}
        row = {
            "task": verb,
            "group": spec["group"],
            "plumbing": spec["plumbing"],
            "apps": len(mine),
            "canonical": canon["app_id"],
            "canonical_copies": canon["copies"],
            "ws_types_in": len(in_types),
            "ws_types_out": len(out_types),
            "entities_in": sorted(in_ents),
            "entities_out": sorted(out_ents),
            "derived_consumes": role_for(in_ents, families),
            "derived_produces": role_for(out_ents - {"report"}, families),
            "declared_consumes": spec["consumes"],
            "declared_produces": spec["produces"],
            "canonical_entities_in": sorted({entity_of[t] for t in canon_in}),
            "canonical_entities_out": sorted({entity_of[t] for t in canon_out}),
            "emits_report": bool(ca["report_output"]),
        }
        rows.append(row)

    entity_names = set(onto["entities"])
    for row in rows:
        row["canonical_consumes"] = role_for(set(row["canonical_entities_in"]), families)
        row["canonical_produces"] = role_for(
            set(row["canonical_entities_out"]) - {"report"}, families)
        row["untyped_canonical"] = row["canonical_produces"] == "none"
        row["proposal_unbacked"] = sorted(
            r for r in row["declared_produces"]
            if r in entity_names and r not in {e for e in entity_of.values()})

    (HERE / "task_io.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))

    for row in rows:
        if args.diff and not (row["untyped_canonical"] or row["proposal_unbacked"]):
            continue
        note = ""
        if row["untyped_canonical"]:
            note = "  <-- canonical method declares no typed output"
        if row["proposal_unbacked"]:
            note += f"  <-- no KBase type for {row['proposal_unbacked']}"
        print(f"{row['task']:24s} {row['apps']:4d} {row['ws_types_in']:5d} {row['ws_types_out']:6d}  "
              f"{row['canonical_consumes']} -> {row['canonical_produces']}{note}")

    untyped = [r for r in rows if r["untyped_canonical"]]
    unbacked = [r for r in rows if r["proposal_unbacked"]]
    wide = [r for r in rows if r["canonical_consumes"] == "any"]
    print(f"\n{len(rows)} verbs | {len(untyped)} whose canonical method declares no typed output "
          f"| {len(unbacked)} proposing a role no workspace type carries "
          f"| {len(wide)} whose canonical input is wider than any family")
    total_in = sum(r["ws_types_in"] for r in rows)
    roles = {r["canonical_consumes"] for r in rows} | {r["canonical_produces"] for r in rows}
    print(f"slots across all verbs accept {total_in} workspace-type mentions, "
          f"stated on the grid as {len(roles - {'none'})} distinct roles")


if __name__ == "__main__":
    main()
