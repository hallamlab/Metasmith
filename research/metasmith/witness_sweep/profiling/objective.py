"""score_node's objective, recomputed on a returned plan from the wire alone.

The question this answers is whether a branch-and-bound cutoff could have ended
the refiner before it spent anything: the objective peaks at 0.0 (`entropy` in
`solver_math` carries no minus sign), so a plan already at 0.0 cannot be beaten
and every candidate after it is provably wasted work.
"""
import json, sys
from math import log2

req = json.load(open(sys.argv[1])); rep = json.load(open(sys.argv[2])); label = sys.argv[3]
nodes, trs = req["nodes"], req["transforms"]
eps, steps = rep["endpoints"], rep["steps"]
given_i = req["given_index"]

plan = [s for s in steps if s["transform"] != given_i]

# used_as_lineage: every endpoint some step bound to a slot that is another
# slot's declared anchor.
used_as_lineage = set()
for s in plan:
    used = dict(s["used"])
    for d, _ in s["used"]:
        for anchor in nodes[d]["parents"]:
            if anchor in used:
                used_as_lineage.add(used[anchor])

usage = {}
for s in plan:
    for d, e in s["used"]:
        if e in used_as_lineage:
            usage[e] = usage.get(e, 0) + 1

m = sum(usage.values())
e_score = sum((c / m) * log2(c / m) for c in usage.values()) if m else 0.0

# lin_distances: one per declared anchor pair actually bound by a step.
pairs = sum(1 for s in plan for d, _ in s["used"]
            for anchor in nodes[d]["parents"] if anchor in dict(s["used"]))

print(f"{label}: plan_steps={len(plan)} anchored_endpoints={len(usage)} "
      f"anchor_usages={m} anchor_pairs={pairs}")
print(f"   e_score={e_score:.6f}  (peaks at 0.0)  weighted={e_score*1000:.3f}")
print(f"   already at the ceiling: {abs(e_score) < 1e-12}")
