"""Where the refiner does earn its budget: how much of it does it need?"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))
from metasmith.agents import Template
from metasmith.testing.solver_verification import plan_fingerprint

MLIB = ROOT / "src/metasmith_libraries"
t = [x for x in Template.Discover(MLIB) if x.name == "isolate_assembly_from_long_reads"][0]
for seed in (7, 99):
    print(f"seed={seed}")
    for refine in (0, 1, 2, 4, 8, 32, 256):
        task = t.spec.Solve(max_refine=refine, seed=seed)
        r = task.plan._solver_result
        fo = getattr(r, "_refiner_iterations", None)
        print(f"   max_refine={refine:4d}  steps={len(task.plan.steps):3d}  "
              f"found_on/iters={fo}  fp={plan_fingerprint(r)[:12]}")
