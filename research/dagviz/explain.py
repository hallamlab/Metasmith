#!/usr/bin/env python3
"""Print a dumped solver problem and plan in the names a human uses.

`payload.json` carries property indices and `rejected.json` carries endpoint
indices; neither is readable without `payload_names.json`.
"""

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
P = json.loads((HERE / "payload.json").read_text())
N = json.loads((HERE / "payload_names.json").read_text())
Q = json.loads((HERE / "rejected.json").read_text())
PROPS = N["properties"]


def ty(props) -> str:
    """The short name of a property set: the e2 type if it has one."""
    out = []
    for i in sorted(props):
        p = PROPS[i]
        try:
            d = json.loads(p)
        except ValueError:
            continue
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if k in ("e2", "method", "provides", "src", "usage", "arm"):
                out.append(f"{k}={v}")
    return ",".join(out) or f"<{sorted(props)}>"


def node(i) -> str:
    n = P["nodes"][i]
    par = "".join(f" <{ty(P['nodes'][p]['props'])}" for p in n["parents"])
    return f"n{i}[{ty(n['props'])}]{par}"


def ep(i) -> str:
    e = Q["endpoints"][i]
    par = ",".join(str(p) for p in e["parents"])
    src = e.get("source_node")
    tag = f" =n{src}" if src is not None else ""
    return f"e{i}[{ty(e['props'])}]{tag} parents={{{par}}}"


def main():
    print(f"== {len(P['given'])} given groups ==")
    for g, ids in enumerate(P["given"]):
        print(f"  group {g}: {len(ids)} nodes")
    shared = set(P["given"][0]) & set(P["given"][1]) if len(P["given"]) == 2 else set()
    print(f"  shared between the two: {len(shared)}")
    for i in sorted(shared):
        t = ty(P["nodes"][i]["props"])
        if "env=" not in t and "src=" not in t:
            print(f"    {node(i)}")

    print(f"\n== transforms ==")
    for i, t in enumerate(P["transforms"]):
        req = ", ".join(ty(P["nodes"][d]["props"]) for d in t["requires"])
        prod = ", ".join(
            ty(P["nodes"][d]["props"]) for g in t["produces"] for d in g
        )
        print(f"  t{i}: [{req}] -> [{prod}]")

    print(f"\n== plan: {len(Q['steps'])} steps, {len(Q['endpoints'])} endpoints ==")
    for i, s in enumerate(Q["steps"]):
        print(f"  step {i}  transform t{s['transform']}")
        for d, e in s["used"]:
            print(f"      uses  slot n{d}[{ty(P['nodes'][d]['props'])}]  <- {ep(e)}")
        for g in s["produced"]:
            for d, e in g:
                print(f"      makes slot n{d}[{ty(P['nodes'][d]['props'])}]  -> {ep(e)}")

    if len(sys.argv) > 1:
        i = int(sys.argv[1])
        print(f"\n== endpoint {i} lineage ==")
        seen, todo = set(), [i]
        while todo:
            x = todo.pop()
            if x in seen:
                continue
            seen.add(x)
            print("   ", ep(x))
            todo += Q["endpoints"][x]["parents"]


if __name__ == "__main__":
    main()
