"""The preamble's backward walk: what it must finish, and what it must not lose.

`solve_by_mcts` scores every transform by how far it sits from the target before
a single node is expanded. The walk that does it used to enumerate simple paths,
which is combinatorial the moment cycles overlap — on a transform universe where
every action has an inverse, it does not finish. The replacement expands each
transform once.

Both halves of that are load-bearing, and they pull against each other:

- **Bounded.** A guard that readmits a transform puts the blow-up back. The
  first test is a shape the old walk could not get through.
- **Complete.** The obvious way to bound it — remember which transforms have
  been seen by their `key` — is wrong, because `Transform.key` is printed from
  properties alone. Duplicate transforms share it, and so do two transforms
  differing only in a lineage constraint, and those have different producer
  edges. Membership in the distance table is not decoration: it builds
  `relavent_transforms` and it answers "is there a path from the givens at all",
  so a transform that loses its entry leaves the search without a word. The
  second test catches that, and it needs a problem carrying duplicates to catch
  anything at all.
"""

from __future__ import annotations

import time
from collections import Counter

from metasmith.models.solver_backend import UsePythonSolver
from metasmith.testing.solver_verification import GeneratorDials, generate_problem

#: Every extra transform's requirement points forward, so the producer graph is
#: dense mutual inverses. At 35 transforms the old walk needs over a minute; at
#: 21 it needs 9ms, which is the curve rather than the constant.
_DENSE_CYCLES = GeneratorDials(
    n_types=12, n_extra_transforms=24, cycle_density=1.0, max_requirements=3
)

#: Five exact clones, so `Transform.key` is shared by transforms the walk has to
#: keep apart.
_DUPLICATES = GeneratorDials(
    n_types=7, n_extra_transforms=3, n_duplicate_transforms=5, lineage_density=0.5
)


def test_a_densely_cyclic_universe_does_not_stall_the_preamble():
    """Termination, not speed — the predecessor did not finish this at all.

    `max_iter=1` and `max_refine=0` leave the search doing nothing, so what is
    timed is the preamble. The bound is deliberately loose: the failure mode
    being guarded is a walk that never returns, and a slow machine is not it.
    """
    problem = generate_problem(0, _DENSE_CYCLES, name="dense")
    assert len(problem.transforms) >= 30, "the shape lost its teeth"
    started = time.perf_counter()
    problem.solve(max_iter=1, max_refine=0)
    elapsed = time.perf_counter() - started
    assert elapsed < 20.0, (
        f"the preamble took {elapsed:.1f}s on {len(problem.transforms)} transforms"
        " -- something readmits a transform to the backward walk, and the cost of"
        " that is combinatorial rather than linear"
    )


def test_transforms_sharing_a_key_each_keep_their_own_distance():
    """The discriminator is duplicate keys *present* in the table at all.

    A key-scoped memo records the first transform reached under each key and
    skips every later one, so it cannot produce this table: every key in it
    would be unique. Pinned to the Python solver because it is the only side
    that reports its preamble — the engine keys its distance table by arena
    index, which is the same identity, and the two are held together by the
    differential sweep rather than from here.
    """
    problem = generate_problem(11, _DUPLICATES, name="dupes")
    with UsePythonSolver():
        solution = problem.solve()
    distances = (solution._heuristics or {}).get("distance_scores")
    assert distances, "the python solver stopped reporting distance_scores"

    by_key = Counter(tr.key for tr in distances)
    shared = sorted(k for k, n in by_key.items() if n > 1)
    assert shared, (
        "every key in the distance table is unique, so either the memo went back"
        " to keying on `Transform.key` or this problem no longer reaches two"
        " transforms that share one -- check the second before believing the"
        " first, and raise n_duplicate_transforms if so"
    )
