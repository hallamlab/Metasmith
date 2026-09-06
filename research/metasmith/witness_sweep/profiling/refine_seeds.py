"""Does max_refine change any template's plan at any of several seeds?

A single-seed answer is not an answer: `refiner-returns-what-it-was-handed`
records one case that changes at seed 7 and not at 42.
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))
from metasmith.agents import Template
from metasmith.models.solver_backend import Backend
from metasmith.testing.solver_verification import plan_fingerprint

assert Backend("solve") == "rust"
MLIB = ROOT / "src/metasmith_libraries"
SEEDS = [1, 7, 42, 99]
changed = []
for t in sorted(Template.Discover(MLIB), key=lambda x: x.name):
    for seed in SEEDS:
        a = t.spec.Solve(max_refine=256, seed=seed)
        b = t.spec.Solve(max_refine=0, seed=seed)
        fa, fb = plan_fingerprint(a.plan._solver_result), plan_fingerprint(b.plan._solver_result)
        if fa != fb:
            changed.append((t.name, seed, len(a.plan.steps), len(b.plan.steps)))
print(f"{len(SEEDS)} seeds x 11 templates = {11*len(SEEDS)} pairs")
print(f"pairs whose plan changes when the refiner is switched off: {len(changed)}")
for n, s, sa, sb in changed:
    print(f"   {n}  seed={s}  steps 256->{sa}  0->{sb}")
