"""Is a small refiner budget indistinguishable from the shipped 256?

`max_refine=0` is not the right question: the refiner does earn its budget on
`isolate_assembly_from_long_reads`, at iteration 3. The question is whether any
case needs more than a handful of iterations.
"""
import sys, time
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))
from metasmith.agents import Template
from metasmith.models.solver_backend import Backend
from metasmith.testing.solver_bench import CORPUS, STRESS_CORPUS
from metasmith.testing.solver_verification import generate_problem, plan_fingerprint

assert Backend("solve") == "rust"
MLIB = ROOT / "src/metasmith_libraries"
SEEDS = [1, 7, 42, 99]
BUDGETS = [4, 8, 16]

diff = []
t256 = t_small = 0.0
for t in sorted(Template.Discover(MLIB), key=lambda x: x.name):
    for seed in SEEDS:
        t0 = time.perf_counter()
        ref = plan_fingerprint(t.spec.Solve(max_refine=256, seed=seed).plan._solver_result)
        t256 += time.perf_counter() - t0
        for b in BUDGETS:
            t0 = time.perf_counter()
            got = plan_fingerprint(t.spec.Solve(max_refine=b, seed=seed).plan._solver_result)
            if b == 16: t_small += time.perf_counter() - t0
            if got != ref: diff.append((t.name, seed, b))
for name, seed, dials in CORPUS + STRESS_CORPUS:
    p = generate_problem(seed, dials, name=name)
    ref = plan_fingerprint(p.solve(max_refine=256))
    for b in BUDGETS:
        if plan_fingerprint(p.solve(max_refine=b)) != ref:
            diff.append((f"gen/{name}", seed, b))

print(f"11 templates x {len(SEEDS)} seeds + 11 generated, against max_refine=256")
print(f"budgets tested: {BUDGETS}")
print(f"pairs that differ from the 256 plan: {len(diff)}")
for d in diff: print("   ", d)
print(f"\ntemplate wall clock, 44 solves: max_refine=256 {t256:.2f}s, max_refine=16 {t_small:.2f}s")
