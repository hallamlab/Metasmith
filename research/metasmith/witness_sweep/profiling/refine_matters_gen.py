"""Same question as refine_matters.py, on the generated corpus.

The eleven templates are one family. If the refiner changes no plan there and
none here either, "it contributes nothing" is a claim about the solver rather
than about one library.
"""
import sys, time
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))
from metasmith.models.solver_backend import Backend
from metasmith.testing.solver_bench import CORPUS, STRESS_CORPUS
from metasmith.testing.solver_verification import generate_problem, plan_fingerprint

assert Backend("solve") == "rust"
rows = []
for name, seed, dials in CORPUS + STRESS_CORPUS:
    p = generate_problem(seed, dials, name=name)
    out = {}
    for refine in (256, 0):
        t0 = time.perf_counter()
        sol = p.solve(max_refine=refine)
        out[refine] = (plan_fingerprint(sol), len(sol.dependency_plan),
                       round(time.perf_counter() - t0, 3))
    rows.append((name, out[256][0] == out[0][0], out[256], out[0]))

print(f"{'case':16s} {'same':>5s} {'steps@256':>9s} {'steps@0':>7s} {'s@256':>7s} {'s@0':>6s}")
for n, same, a, b in rows:
    print(f"{n:16s} {str(same):>5s} {a[1]:9d} {b[1]:7d} {a[2]:7.3f} {b[2]:6.3f}")
print(f"\n{len(rows)} generated cases, "
      f"{sum(1 for _, s, _, _ in rows if not s)} whose plan changes with the refiner off")
