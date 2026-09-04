#!/usr/bin/env python3
"""Place every KBase workspace type and every parameter union on the canonical grid.

Reads `types.yml`, `type_map.yml` and `catalog/types.jsonl`. Writes
`type_map.jsonl`, one row per workspace type and one per union, and reports what
the grid collapses.

A union is a parameter that accepts several workspace types. It collapses when its
members share an entity and differ only along axes, in which case the curated
library needs no union at all -- the slot names the entity and leaves the axis
free. It survives when the members span entities, which is a genuine either/or a
type system has to carry.

    map_types.py
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
    onto = yaml.safe_load((HERE / "types.yml").read_text())
    mapping = yaml.safe_load((HERE / "type_map.yml").read_text())["types"]
    referenced = {r["ws_type"]: r for r in
                  (json.loads(l) for l in (CATALOG / "types.jsonl").read_text().splitlines() if l)}
    return onto, mapping, referenced


def defaults(onto: dict) -> dict:
    return {name: spec["default"] for name, spec in onto["axes"].items()}


def resolve(entry: dict, onto: dict) -> dict:
    """A type's full axis assignment: the entity's declared axes, filled from defaults."""
    entity = entry["entity"]
    spec = onto["entities"][entity]
    carried = set(spec["axes"]) | {"cardinality", "provenance", "status"}
    base = defaults(onto)
    out = {}
    for axis in sorted(carried):
        out[axis] = entry.get(axis, base[axis])
    for axis, value in entry.items():
        if axis == "entity":
            continue
        if axis not in carried:
            raise SystemExit(f"{entity} does not carry axis {axis!r} but a row sets it")
        allowed = onto["axes"][axis]["values"]
        if value not in allowed:
            raise SystemExit(f"axis {axis!r} has no value {value!r}")
        out[axis] = value
    return out


def unions_from_catalog() -> list:
    apps = [json.loads(l) for l in (CATALOG / "apps.jsonl").read_text().splitlines() if l]
    seen = set()
    for a in apps:
        for p in a["inputs"]:
            if len(set(p["ws_types"])) > 1:
                seen.add(tuple(sorted(set(p["ws_types"]))))
    return sorted(seen)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.parse_args()
    onto, mapping, referenced = load()

    missing = sorted(set(referenced) - set(mapping))
    extra = sorted(set(mapping) - set(referenced))
    if missing:
        raise SystemExit(f"{len(missing)} referenced types are unmapped: {missing[:6]}")
    if extra:
        raise SystemExit(f"{len(extra)} mapped types are not referenced: {extra[:6]}")

    rows, resolved = [], {}
    for ws_type, entry in mapping.items():
        axes = resolve(entry, onto)
        axes["status"] = referenced[ws_type]["status"]
        resolved[ws_type] = (entry["entity"], axes)
        rows.append({"kind": "type", "ws_type": ws_type, "entity": entry["entity"],
                     "axes": axes})

    entities = collections.Counter(e for e, _ in resolved.values())
    tuples = {(e, tuple(sorted(a.items()))) for e, a in resolved.values()}

    families = {name: set(spec["entities"]) for name, spec in onto["families"].items()}
    for name, members in families.items():
        unknown = members - set(onto["entities"])
        if unknown:
            raise SystemExit(f"family {name!r} names entities that do not exist: {sorted(unknown)}")

    level_counts = collections.Counter()
    for i, members in enumerate(unions_from_catalog(), start=1):
        ents = {resolved[m][0] for m in members}
        entity = free = family = None
        if len(ents) == 1:
            entity = next(iter(ents))
            free = sorted(axis for axis in onto["entities"][entity]["axes"] + ["cardinality", "provenance"]
                          if len({resolved[m][1].get(axis) for m in members}) > 1)
            level = "entity"
        else:
            fits = sorted((len(pool), name) for name, pool in families.items() if ents <= pool)
            family = fits[0][1] if fits else None
            level = "family" if family else "none"
        level_counts[level] += 1
        rows.append({"kind": "union", "union_id": f"accepts_{i:02d}",
                     "members": list(members), "entities": sorted(ents),
                     "level": level, "entity": entity, "family": family,
                     "free_axes": free or []})

    (HERE / "type_map.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))

    n_types = sum(1 for r in rows if r["kind"] == "type")
    book = {"status", "provenance"}
    meanings = {(e, tuple(sorted((k, v) for k, v in a.items() if k not in book)))
                for e, a in resolved.values()}
    print(f"workspace types {n_types} -> entities {len(entities)} "
          f"-> distinct meanings {len(meanings)} -> fully specified tuples {len(tuples)}")
    print(f"unions {sum(level_counts.values())} -> one entity {level_counts['entity']}, "
          f"one family {level_counts['family']}, genuinely heterogeneous {level_counts['none']}")
    print("\nentities by workspace types absorbed:")
    for entity, n in entities.most_common():
        if n > 1:
            print(f"  {n:3d}  {entity}")
    produced = {e for e, _ in resolved.values()}
    unreached = sorted(set(onto["entities"]) - produced)
    if unreached:
        print(f"\nentities no workspace type maps to: {unreached}")


if __name__ == "__main__":
    main()
