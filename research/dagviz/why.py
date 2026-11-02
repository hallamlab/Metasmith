import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import render_e2 as R
from metasmith.models.dag_renderer import DagMode, NodeKind

plan = R.plan_for(sys.argv[1] if len(sys.argv) > 1 else "short")
r = plan.BuildDAG(mode=DagMode.PLAIN)
preds, succs = r._neighbours()
labels = r.labels

sr = plan.BuildDAG(mode=DagMode.COLLAPSED, hide_data=True)
snodes, sedges = sr._graph()
deg = {n: 0 for n in snodes}
for a, b in sedges:
    deg[a] += 1
    deg[b] += 1

print("=== isolated steps in the steps-only view ===")
for n, d in deg.items():
    if d == 0:
        print(" ", labels.get(n).full if n in labels else n)

print()
for n in list(snodes):
    if deg[n]:
        continue
    print(f"=== {n} in the full graph ===")
    print("  inputs :", [(p, r._nodes[p].name) for p in preds[n]])
    print("  outputs:", [(s, r._nodes[s].name) for s in succs[n]])
    for s in succs[n]:
        print(f"  consumers of {s}:", succs[s] or "(none)")
