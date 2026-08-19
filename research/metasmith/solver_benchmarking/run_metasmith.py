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

import multiprocessing
import time
from dataclasses import dataclass

from metasmith.models.solver_backend import Backend, UsePythonSolver
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


def _solve_and_check(problem: SolverProblem, *, seed: int, max_iter: int, max_refine: int, expect_backend: str) -> MetasmithResult:
    t0 = time.perf_counter()
    if expect_backend == "python":
        with UsePythonSolver():
            sol = problem.solve(seed=seed, max_iter=max_iter, max_refine=max_refine)
            actual_backend = Backend("solve")
    else:
        sol = problem.solve(seed=seed, max_iter=max_iter, max_refine=max_refine)
        actual_backend = Backend("solve")
    wall = time.perf_counter() - t0
    if actual_backend != expect_backend:
        raise RuntimeError(
            f"[{problem.name}] expected backend={expect_backend!r} but "
            f"Backend('solve')={actual_backend!r} -- a claimed-Rust run would "
            "silently be a Python one"
        )
    check = check_plan(problem, sol) if sol.complete else None
    return MetasmithResult(
        backend=actual_backend,
        solved=sol.complete,
        checked_ok=bool(check) if check is not None else False,
        check_detail=str(check) if check is not None else "not solved, not checked",
        plan_length=len(sol.dependency_plan),
        wall_seconds=wall,
    )


def _worker(problem, kwargs, conn):
    try:
        conn.send(("ok", _solve_and_check(problem, **kwargs)))
    except Exception as e:  # noqa: BLE001 -- reported to the parent, not raised in the worker
        conn.send(("err", f"{type(e).__name__}: {e}"))
    finally:
        conn.close()


def _solve_with_timeout(problem: SolverProblem, *, expect_backend: str, seed: int, max_iter: int, max_refine: int, timeout_s: float) -> MetasmithResult:
    ctx = multiprocessing.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe(duplex=False)
    kwargs = dict(seed=seed, max_iter=max_iter, max_refine=max_refine, expect_backend=expect_backend)
    proc = ctx.Process(target=_worker, args=(problem, kwargs, child_conn))
    t0 = time.perf_counter()
    proc.start()
    child_conn.close()
    proc.join(timeout_s)
    wall = time.perf_counter() - t0
    if proc.is_alive():
        proc.terminate()
        proc.join(5)
        if proc.is_alive():
            proc.kill()
            proc.join()
        return MetasmithResult(
            backend=expect_backend, solved=False, checked_ok=False,
            check_detail=f"timed out after {timeout_s}s (cyclic-graph blowup in solve_by_mcts)",
            plan_length=-1, wall_seconds=wall, timed_out=True,
        )
    if parent_conn.poll():
        status, payload = parent_conn.recv()
        if status == "ok":
            return payload
        return MetasmithResult(
            backend=expect_backend, solved=False, checked_ok=False,
            check_detail=f"worker error: {payload}", plan_length=-1, wall_seconds=wall,
        )
    return MetasmithResult(
        backend=expect_backend, solved=False, checked_ok=False,
        check_detail=f"worker exited with no result (code {proc.exitcode})",
        plan_length=-1, wall_seconds=wall,
    )


def solve_with_metasmith_python(
    problem: SolverProblem, *, seed: int = 1, max_iter: int = 1024, max_refine: int = 1024, timeout_s: float = DEFAULT_TIMEOUT_S
) -> MetasmithResult:
    return _solve_with_timeout(
        problem, expect_backend="python", seed=seed, max_iter=max_iter, max_refine=max_refine, timeout_s=timeout_s
    )


def solve_with_metasmith_rust(
    problem: SolverProblem, *, seed: int = 1, max_iter: int = 1024, max_refine: int = 1024
) -> MetasmithResult:
    """No subprocess-wrapping timeout here: the cyclic-graph blowup lives
    entirely in the Python fallback's backward distance walk (confirmed absent
    from `src/workflow_solver/src/*.rs`), so the Rust path
    doesn't need the same guard -- and wrapping it in a spawned Python
    interpreter just to shell out to a Rust binary that's already its own
    subprocess was pure overhead (a full interpreter start per instance).
    """
    return _solve_and_check(
        problem, seed=seed, max_iter=max_iter, max_refine=max_refine, expect_backend="rust"
    )
