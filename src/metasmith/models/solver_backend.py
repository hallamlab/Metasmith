from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterable

from ..logging import Log

if TYPE_CHECKING:
    from .solver import Endpoint, Solution, Transform

__all__ = [
    "Solver",
    "PythonSolver",
    "RustSolver",
    "Backend",
    "UsePythonSolver",
    "ResetSolverSelection",
    "_set_solver_class",
    "_get_solver_class",
]

class Solver:
    name: str = "?"

    @classmethod
    def Available(cls) -> bool:
        raise NotImplementedError

    def Solve(
        self,
        given: list[set[Endpoint]],
        transforms: Iterable[Transform],
        target: Transform,
        seed: int=42,
        max_iter: int=256,
        max_refine: int=256,
    ) -> Solution:
        raise NotImplementedError

class PythonSolver(Solver):
    name = "python"

    @classmethod
    def Available(cls) -> bool:
        return True

    def Solve(self, given, transforms, target, seed=42, max_iter=256, max_refine=256):
        # Imported here, not at module scope: `solver.py` reaches back into this
        # module for the dispatch, so an eager import at either end is a cycle.
        from .solver import _solve_by_mcts_python
        return _solve_by_mcts_python(
            given, transforms, target,
            seed=seed, max_iter=max_iter, max_refine=max_refine,
        )

class RustSolver(Solver):
    name = "rust"

    @classmethod
    def Available(cls) -> bool:
        from .solver_engine import EngineFor
        return EngineFor("solve") is not None

    def Solve(self, given, transforms, target, seed=42, max_iter=256, max_refine=256):
        from .solver_engine import EngineError, EngineFor
        from .solver_wire import solve_via_engine
        info = EngineFor("solve")
        if info is None:
            # Pinned to rust with no usable engine. Raising rather than falling
            # back, for the same reason `solve_via_engine` does not catch: a pin
            # is a statement, and quietly serving something else makes the pin
            # a lie that reads as a slowdown.
            raise EngineError(
                "the rust solver was asked for, but no usable msm_solver is"
                " staged for this platform (./dev.sh -bel)"
            )
        return solve_via_engine(
            info, given, transforms, target,
            seed=seed, max_iter=max_iter, max_refine=max_refine,
        )

_solver_type: type[Solver]|None = None
_fallback_warned = False

def _set_solver_class(cls: type[Solver]|None) -> type[Solver]|None:
    global _solver_type
    previous = _solver_type
    _solver_type = cls
    return previous

def _get_solver_class() -> type[Solver]:
    if _solver_type is not None: return _solver_type
    if RustSolver.Available(): return RustSolver
    # Auto-detection deliberately does not memoise its answer into
    # `_solver_type`. `EngineFor` already caches the probe, so this is cheap,
    # and writing the class back would make `ResetEngineCache()` unable to
    # re-detect a binary that appeared or moved -- which is exactly what the
    # resolution tests do.
    _warn_about_the_unasked_for_fallback()
    return PythonSolver

def _warn_about_the_unasked_for_fallback():
    global _fallback_warned
    if _fallback_warned: return
    _fallback_warned = True
    from .solver_engine import ENGINE_NAME, GetEngine, packaged_engine_path, platform_slot
    path = packaged_engine_path()
    if path is None:
        why = (
            f"no [{ENGINE_NAME}] is staged for [{platform_slot()}] -- build one"
            " with [./dev.sh -bel], or [./dev.sh -be] to cross-build all four"
        )
    elif GetEngine() is None:
        why = f"the binary at [{path}] was refused at its handshake (see above)"
    else:
        why = f"the binary at [{path}] does not advertise [solve]"
    Log.Warn(
        f"solving with the python implementation because {why}. The plans are"
        " the same either way; the search is roughly 15x slower."
    )

def ResetSolverSelection():
    global _fallback_warned
    _fallback_warned = False
    _set_solver_class(None)

@contextmanager
def UsePythonSolver():
    previous = _set_solver_class(PythonSolver)
    try:
        yield
    finally:
        _set_solver_class(previous)

def Backend(capability: str="solve") -> str:
    if capability == "solve":
        return _get_solver_class().name
    if _solver_type is PythonSolver: return "python"
    from .solver_engine import EngineFor
    return "rust" if EngineFor(capability) is not None else "python"
