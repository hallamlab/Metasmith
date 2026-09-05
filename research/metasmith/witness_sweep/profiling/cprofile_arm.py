"""cProfile one metagenomics arm on the python solver.

The engine is what ships, but it cannot be profiled here -- perf_event_paranoid
is 4 and there is no cargo on this host. The python solver answers the same
`solve_by_mcts` and is a line-for-line port, so its CALL COUNTS are the engine's
call counts. Read the counts, not the times: cProfile roughly doubles this
workload and inflates high-call-count functions.
"""
import cProfile, io, pstats, sys, time
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src/metasmith_libraries"))
sys.path.insert(0, str(ROOT / "research/metasmith/witness_sweep"))

import duplicate_work as D
from metasmith.models.solver_backend import Backend, UsePythonSolver

arm, refine = sys.argv[1], int(sys.argv[2])
with UsePythonSolver():
    assert Backend("solve") == "python"
    t0 = time.perf_counter()
    pr = cProfile.Profile()
    pr.enable()
    task = D.solve(arm, max_refine=refine)
    pr.disable()
    elapsed = time.perf_counter() - t0

s = io.StringIO()
pstats.Stats(pr, stream=s).sort_stats("tottime").print_stats(28)
print(f"### arm={arm} max_refine={refine} steps={len(task.plan.steps)} "
      f"wall_under_profiler={elapsed:.1f}s")
print(s.getvalue())
