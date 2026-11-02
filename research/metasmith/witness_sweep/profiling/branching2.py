"""What one refiner iteration actually enumerates, per step.

The refiner passes `mock_produced`, which turns the generator's lineage filter
OFF (`handle_lineage = mock_produced is None`). So a step's candidate count is
the full cross product of its slots' props-compatible endpoints -- given ones
included -- with nothing pruning it. That product, summed over the plan's steps,
is the number of candidate plans one iteration scores.
"""
import json, sys
req = json.load(open(sys.argv[1])); rep = json.load(open(sys.argv[2])); label = sys.argv[3]
nodes, trs = req["nodes"], req["transforms"]
eps, steps = rep["endpoints"], rep["steps"]
given_i, target_i = req["given_index"], req["target_index"]

# Everything the state has: given endpoints and produced ones alike.
avail = sorted({e for s in steps for g in s["produced"] for _, e in g})
props = {e: set(eps[e]["props"]) for e in avail}

rows, total = [], 0
for si, s in enumerate(steps):
    if s["transform"] in (given_i, target_i): continue
    t = trs[s["transform"]]
    counts = [sum(1 for e in avail if set(nodes[d]["props"]).issubset(props[e]))
              for d in t["requires"]]
    combos = 1
    for c in counts: combos *= max(c, 1)
    rows.append((si, s["transform"], len(counts), counts, combos))
    total += combos

rows.sort(key=lambda r: -r[4])
print(f"== {label}: {len(rows)} steps, {len(avail)} endpoints available")
print(f"   candidate plans enumerated per refiner iteration: {total:,}")
print(f"   {'step':>5s} {'tr':>4s} {'slots':>5s} {'combos':>10s}  slot candidate counts")
for si, tr, n, counts, combos in rows[:8]:
    print(f"   {si:5d} {tr:4d} {n:5d} {combos:10,d}  {counts}")
print(f"   top 3 steps are {100*sum(r[4] for r in rows[:3])/max(total,1):.0f}% of the total")
