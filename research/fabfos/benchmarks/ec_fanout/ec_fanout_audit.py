import re, sys, json
from collections import defaultdict
from pathlib import Path
import pandas as pd

REAC_PROP = Path("data/fabfos/originals/metanetx/4.5/reac_prop.tsv")
E4 = re.compile(r"^\d+\.\d+\.\d+\.\d+$")
MNXM = re.compile(r"\b(MNXM\d+|WATER|BIOMASS|MNXM\w+)\b")

rows = []
with open(REAC_PROP) as fh:
    for line in fh:
        if line.startswith("#"):
            continue
        p = line.rstrip("\n").split("\t")
        if len(p) < 5:
            continue
        mnxr, eq, _ref, classifs, balanced = p[0], p[1], p[2], p[3], p[4]
        if not mnxr.startswith("MNXR") or mnxr == "EMPTY":
            continue
        rows.append((mnxr, eq, classifs, balanced))
print(f"[audit] reac_prop rows: {len(rows):,}", file=sys.stderr)

def participants(eq: str):
    out = set()
    for tok in eq.replace("=", " ").split():
        if "@" in tok:
            out.add(tok.split("@", 1)[0])
    return out

parts = {mnxr: participants(eq) for mnxr, eq, _, _ in rows}

freq = defaultdict(int)
for s in parts.values():
    for m in s:
        freq[m] += 1
N = len(parts)
CURRENCY_FRAC = float(sys.argv[1]) if len(sys.argv) > 1 else 0.01
currency = {m for m, c in freq.items() if c >= CURRENCY_FRAC * N}
print(f"[audit] currency cut {CURRENCY_FRAC:.3%} of {N:,} reactions -> "
      f"{len(currency)} metabolites", file=sys.stderr)
print(f"[audit] currency set: {sorted(currency, key=lambda m: -freq[m])}", file=sys.stderr)

core = {r: (s - currency) for r, s in parts.items()}

ec_to_mnxr = defaultdict(set)
partial_only = 0
for mnxr, _eq, classifs, _b in rows:
    if not classifs:
        continue
    ecs = [e.strip() for e in classifs.split(";") if e.strip()]
    if ecs and not any(E4.match(e) for e in ecs):
        partial_only += 1
    for e in ecs:
        ec_to_mnxr[e].add(mnxr)

l4 = {e: s for e, s in ec_to_mnxr.items() if E4.match(e)}
partial = {e: s for e, s in ec_to_mnxr.items() if not E4.match(e)}
print(f"[audit] ECs in classifs: {len(ec_to_mnxr):,} "
      f"(level-4 {len(l4):,}, partial {len(partial):,})", file=sys.stderr)
print(f"[audit] reactions whose ONLY classifs are partial: {partial_only:,} "
      f"(unreachable by any lane)", file=sys.stderr)

def components(mnxrs):
    mnxrs = sorted(mnxrs)
    parent = {r: r for r in mnxrs}
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb
    by_met = defaultdict(list)
    for r in mnxrs:
        for m in core[r]:
            by_met[m].append(r)
    for rs in by_met.values():
        for r in rs[1:]:
            union(rs[0], r)
    groups = defaultdict(list)
    for r in mnxrs:
        groups[find(r)].append(r)
    return list(groups.values())

report = []
for ec, mnxrs in l4.items():
    mnxrs = {r for r in mnxrs if r in core}
    if len(mnxrs) < 2:
        continue
    empty_core = {r for r in mnxrs if not core[r]}
    comps = components(mnxrs - empty_core) if (mnxrs - empty_core) else []
    report.append({
        "ec": ec,
        "n_mnxr": len(mnxrs),
        "n_empty_core": len(empty_core),
        "n_components": len(comps),
        "comp_sizes": sorted((len(c) for c in comps), reverse=True),
        "components": [sorted(c) for c in comps],
    })

rep = pd.DataFrame(report)
rep.to_json(sys.argv[2] if len(sys.argv) > 2 else "/dev/stdout",
            orient="records", lines=True)
print(f"[audit] level-4 ECs with >=2 MNXR: {len(rep):,}", file=sys.stderr)
if len(rep):
    print(f"[audit] of those, SPLIT (>=2 components): "
          f"{(rep.n_components >= 2).sum():,}", file=sys.stderr)
