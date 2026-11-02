"""Solve a `SolverProblem` (built by `graph_expand.py`) via metasmith, once
forced to the Python solver and once via the Rust engine, validating every
result with `check_plan` before its timing is trusted.

Every solve runs in its own subprocess under a hard wall-clock timeout. This
is not defensive boilerplate: our pure-state graphs are naturally cyclic
(blocksworld's `stack`/`unstack` are inverses of each other, so every state
has a path back toward itself), and `solve_by_mcts`'s backward distance walk
(`solver.py`, the loop building `distance_scores`/`opportunity_scores`) is a
DFS gated per-path rather than globally memoized -- on a cyclic graph its
runtime depends on set/dict iteration order, which is seed- and
process-dependent. It was observed to run in ~0.08s in one process and hang
indefinitely in another, same instance, same code. That is a real property
of the adapter being benchmarked, not a bug to paper over -- so a timeout is
recorded as a first-class outcome, not swallowed.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from metasmith.models.solver_backend import Backend
from metasmith.testing.solver_verification import SolverProblem, check_plan

#: Generous relative to the ~0.1s / ~0.01s typical solves seen on this
#: instance size -- anything that needs this long is exhibiting the
#: cyclic-graph blowup described above, not just running slowly.
DEFAULT_TIMEOUT_S = 20.0


@dataclass
class MetasmithResult:
    backend: str
    solved: bool
    checked_ok: bool
    check_detail: str
    plan_length: int
    wall_seconds: float
    timed_out: bool = False


def _solve_and_check(problem: SolverProblem, *, seed: int, max_iter: int, max_refine: int) -> MetasmithResult:
    t0 = time.perf_counter()
    sol = problem.solve(seed=seed, max_iter=max_iter, max_refine=max_refine)
    backend = Backend("solve")
    wall = time.perf_counter() - t0
    if backend != "rust":
        raise RuntimeError(f"[{problem.name}] Backend('solve')={backend!r}")
    check = check_plan(problem, sol) if sol.complete else None
    return MetasmithResult(
        backend=backend,
        solved=bool(sol.complete),
        checked_ok=bool(check.ok) if check is not None else False,
        check_detail="" if check is None or check.ok else str(check.violations),
        plan_length=len(sol.dependency_plan) if sol.complete else -1,
        wall_seconds=wall,
    )


def solve_with_metasmith_rust(
    problem: SolverProblem, *, seed: int = 1, max_iter: int = 1024, max_refine: int = 1024
) -> MetasmithResult:
    return _solve_and_check(problem, seed=seed, max_iter=max_iter, max_refine=max_refine)
