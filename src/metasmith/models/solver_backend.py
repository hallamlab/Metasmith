from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from .solver import Endpoint, Solution, Transform

__all__ = [
    "REFINER_BUDGET",
    "Backend",
    "solve_with_engine",
]

#: Refiner iterations, the shipped default.
#:
#: 256 was never what made the refiner work. Its winner is found by iteration 3
#: on every case that has one, and its frontier exhausts by 16 even when 256 is
#: asked for -- 256 is only what let it run away on a plan whose state space does
#: not exhaust. Re-derived after the lineage repair rather than carried forward:
#: over 44 case-seed pairs, budgets of 4, 8 and 16 give plan fingerprints
#: byte-identical to 256, and two pairs differ from a budget of 0, so 0 is not
#: safe.
#:
#: **It has to be the only default.** Wiring it into the solver alone left it
#: unreachable: every caller from `solve_by_mcts` upward kept its own literal 256
#: and forwarded it, so the budget that shipped stayed 256 and nothing said so.
#: Every layer above now forwards `None` and `solve_by_mcts` resolves it here.
REFINER_BUDGET = 8


def solve_with_engine(
    given: list[set[Endpoint]],
    transforms: Iterable[Transform],
    target: Transform,
    seed: int=42,
    max_iter: int=256,
    max_refine: int=REFINER_BUDGET,
) -> Solution:
    from .solver_engine import EngineError, EngineFor
    from .solver_wire import solve_via_engine
    info = EngineFor("solve")
    if info is None:
        raise EngineError(f"no usable solver engine: {_why_not()}")
    return solve_via_engine(
        info, given, transforms, target,
        seed=seed, max_iter=max_iter, max_refine=max_refine,
    )


def Backend(capability: str="solve") -> str:
    """Which implementation answers `capability`: "rust", or "none" if nothing does."""
    from .solver_engine import EngineFor
    return "rust" if EngineFor(capability) is not None else "none"


def _why_not() -> str:
    """The whole diagnosis, because there is no longer a slower path to fall back to.

    This used to be a warning beside a working solve. Now it is the text of a hard
    failure, so it has to name which of the three things went wrong -- nothing
    staged, staged but refused at the handshake, or staged and not offering
    `solve` -- and the exec-bit case specifically, because a wheel and an sdist
    normalise the mode differently and that has silently rerouted every plan in
    every worktree once already.
    """
    from .solver_engine import ENGINE_NAME, GetEngine, packaged_engine_path, platform_slot
    path = packaged_engine_path()
    if path is None:
        return (
            f"no [{ENGINE_NAME}] is staged for [{platform_slot()}] -- build one"
            " with [./dev/metasmith.sh -bel], or [./dev/metasmith.sh -be] to"
            " cross-build all four"
        )
    if GetEngine() is None:
        return (
            f"the binary at [{path}] was refused at its handshake (see above)."
            " If that was a permission error, the file lost its executable bit"
            " somewhere between the build and here -- check [ls -l] on it"
        )
    return f"the binary at [{path}] does not advertise [solve]"
