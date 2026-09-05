"""Does max_refine change the plan on any real workflow?

`found_on == 1` says the refiner returns the state it was handed. This asks the
question the other way round, on the artefact that matters: solve every shipped
template at max_refine=256 and at 0 and compare plan fingerprints.
"""
import sys, time
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))
from metasmith.agents import Template
from metasmith.models.solver_backend import Backend
from metasmith.testing.solver_verification import plan_fingerprint

assert Backend("solve") == "rust"
MLIB = ROOT / "src" / "metasmith_libraries"
rows = []
for t in sorted(Template.Discover(MLIB), key=lambda x: x.name):
    out = {}
    for refine in (256, 0):
        t0 = time.perf_counter()
        task = t.spec.Solve(max_refine=refine)
        out[refine] = (plan_fingerprint(task.plan._solver_result),
                       len(task.plan.steps), round(time.perf_counter() - t0, 3))
    same = out[256][0] == out[0][0]
    rows.append((t.name, same, out[256], out[0]))

print(f"{'template':40s} {'same plan':>9s} {'steps@256':>9s} {'steps@0':>7s} {'s@256':>7s} {'s@0':>6s}")
for name, same, a, b in rows:
    print(f"{name:40s} {str(same):>9s} {a[1]:9d} {b[1]:7d} {a[2]:7.3f} {b[2]:6.3f}")
n = sum(1 for _, s, _, _ in rows if not s)
print(f"\n{len(rows)} templates, {n} whose plan changes when the refiner is switched off")
print(f"total seconds: max_refine=256 {sum(a[2] for _,_,a,_ in rows):.2f}, "
      f"max_refine=0 {sum(b[2] for _,_,_,b in rows):.2f}")
