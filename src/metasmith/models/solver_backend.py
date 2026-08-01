"""Which solver implementation this process uses.

Deliberately separate from `solver_engine.py`. That module answers *is this
binary trustworthy* -- where it lives, what it said about itself, whether its
versions match. This one answers *which implementation runs*, and it is the only
one of the two that anything outside the solver should import.

The Rust engine is the default, not an opportunistic upgrade. `solve_by_mcts`
runs on it whenever a usable binary is staged, with nothing set and nothing
passed; the Python solver is a *reversion*, reachable only by saying so. That
distinction is why an unasked-for fallback warns: the two implementations
produce the same plans and differ by roughly fifteen times in wall clock, so a
checkout that forgot to stage a binary is correct and slow, which is precisely
the kind of defect that never gets noticed.

Selection is an object rather than an environment read because the callers are
on core execution paths. A planning call has to be able to state which
implementation it wants and put the previous choice back afterwards -- a
differential test needs a reference, and a test whose subject is the Python
implementation stops testing anything the moment the search runs elsewhere.
An environment variable cannot be scoped, cannot be restored, and is invisible
at the call site.
"""

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
    """One implementation of `solve_by_mcts`, named and askable.

    `Solve` takes exactly `solve_by_mcts`'s arguments in exactly its order --
    several callers pass `seed`/`max_iter`/`max_refine` positionally, and the
    drop-in claim is that the signature does not move.
    """

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
    """The original search, in `models/solver.py`. Always available."""

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
    """`msm_solver`, over a subprocess and a JSON envelope."""

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

#: The pin. `None` means *decide on first use*, which is the shipping default.
_solver_type: type[Solver]|None = None
_fallback_warned = False

def _set_solver_class(cls: type[Solver]|None) -> type[Solver]|None:
    """Pin the implementation; `None` restores automatic detection.

    Returns the previous value so a caller can put it back -- which is what
    `UsePythonSolver` does, and what any scoped override should do.
    """
    global _solver_type
    previous = _solver_type
    _solver_type = cls
    return previous

def _get_solver_class() -> type[Solver]:
    """The implementation the next solve will use."""
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
    """Once per process, naming the cause and the fix.

    Once, not per solve: the GUI re-plans on every edit, and what matters is
    noticing at all rather than being told repeatedly. A pin never reaches here,
    because an asked-for fallback is a choice and not a surprise.
    """
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
    """Unpin, and let the fallback warn again. For tests."""
    global _fallback_warned
    _fallback_warned = False
    _set_solver_class(None)

@contextmanager
def UsePythonSolver():
    """Pin the python solver for the duration of the block.

    Two callers need this and they need it for opposite reasons. A differential
    test needs a *reference* -- comparing the engine against itself is a green
    run that proves nothing, and that is the easiest mistake to make here because
    both sides go through the same `solve()`. And a test whose subject is the
    python implementation -- CPython's set layout, say, or how many times a
    particular branch fires -- stops testing anything at all the moment the
    search runs somewhere else.

    Not thread-safe, and was not before: the pin is process-global. Planning is
    already serialised behind the GUI's own lock, and this is stated rather than
    defended against.
    """
    previous = _set_solver_class(PythonSolver)
    try:
        yield
    finally:
        _set_solver_class(previous)

def Backend(capability: str="solve") -> str:
    """`"rust"` or `"python"` -- what will actually run. For tests and reporting.

    A pin decides `solve` outright. Other capabilities are not pinnable: `rng`
    exists for the trace harness, which is handed an `EngineInfo` directly, so
    the honest answer there is whatever the probe found -- except under a python
    pin, which is a statement about this whole process.
    """
    if capability == "solve":
        return _get_solver_class().name
    if _solver_type is PythonSolver: return "python"
    from .solver_engine import EngineFor
    return "rust" if EngineFor(capability) is not None else "python"
