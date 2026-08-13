"""The two scenarios that only ever lived in `main/workflow_solver/solver_test.ipynb`.

Everything in `branching_test.py` was already absorbed into `test_solver.py`
verbatim, so there is nothing to deduplicate from it. The notebook is the other
half, and these two cells never made the crossing:

* **the dense reciprocal web** -- four bidirectional transform pairs, against
  the two in `test_solver.py`'s cyclic case, and no lineage constraint on the
  target;
* **the M×N join** -- search-space scaling, as opposed to
  `test_solver_scaling_cyanoverse.py`, which scales the *data* (21k items
  through one transform) and says nothing about how the search copes with
  alternatives.

Together they place the boundary of the cyclic-search failure pinned in
`test_solver.py`: cycles alone are fine, and a lineage constraint alone is
fine. It is a lineage constraint whose satisfaction *requires walking a cycle*
that defeats the search.
"""

from __future__ import annotations

import pytest

from metasmith.models.solver import Endpoint, Transform
from metasmith.testing.solver_verification import (
    SolverProblem,
    check_plan,
    plan_fingerprint,
)


def _reciprocal_web() -> SolverProblem:
    """assembly↔bins, bins↔tax, bins↔contigs, contigs↔orfs, orfs→annotation."""
    transforms = []
    for a, b in [
        ("assembly", "bins"), ("bins", "tax"),
        ("bins", "contigs"), ("contigs", "orfs"),
    ]:
        for x, y in ((a, b), (b, a)):
            t = Transform()
            t.AddRequirement(properties={x})
            t.AddProduct(properties={y})
            transforms.append(t)
    t = Transform()
    t.AddRequirement(properties={"orfs"})
    t.AddProduct(properties={"annotation"})
    transforms.append(t)

    target = Transform()
    target.AddRequirement(properties={"annotation"})
    return SolverProblem(
        given=[{Endpoint(properties={"assembly"})}],
        transforms=transforms,
        target=target,
        name="reciprocal-web",
    )


def test_a_dense_reciprocal_web_solves_to_the_short_route():
    """Eight cycle-forming transforms, and the search still takes the direct path.

    assembly → bins → contigs → orfs → annotation is four applications plus the
    given step and the target. Every one of those types also has a transform
    pointing back the way it came, so the search has a reciprocal edge available
    at each hop and declines all of them.
    """
    problem = _reciprocal_web()
    solution = problem.solve()
    assert solution.complete
    verdict = check_plan(problem, solution)
    assert verdict.ok, verdict.violations
    assert len(solution.dependency_plan) == 6


def test_the_reciprocal_web_is_stable_across_seeds():
    """No seed talks it into a detour -- the shape here is not luck."""
    prints = {plan_fingerprint(_reciprocal_web().solve(seed=s)) for s in (42, 7, 1234)}
    assert len(prints) == 1, "the route through the web depends on the seed"


def _mxn_join(m: int, n: int) -> SolverProblem:
    """`m` alternative routes of `n` steps, joined under a lineage constraint.

    Every route reaches the same `left`, so the search has `m` interchangeable
    ways to satisfy the join and `n` hops to walk down whichever it picks. The
    join's second slot must descend from its first, which is what stops the two
    bindings from being chosen independently.
    """
    transforms = []
    for i in range(m):
        prev = "seed"
        for j in range(n):
            t = Transform()
            t.AddRequirement(properties={prev})
            prev = f"s{i}_{j}"
            t.AddProduct(properties={prev})
            transforms.append(t)
        t = Transform()
        t.AddRequirement(properties={prev})
        t.AddProduct(properties={"left"})
        transforms.append(t)

    t = Transform()
    t.AddRequirement(properties={"left"})
    t.AddProduct(properties={"right"})
    transforms.append(t)

    join = Transform()
    anchor = join.AddRequirement(properties={"left"})
    join.AddRequirement(properties={"right"}, parents={anchor})
    join.AddProduct(properties={"joined"})
    transforms.append(join)

    target = Transform()
    target.AddRequirement(properties={"joined"})
    return SolverProblem(
        given=[{Endpoint(properties={"seed"})}],
        transforms=transforms,
        target=target,
        name=f"join-{m}x{n}",
    )


@pytest.mark.parametrize("m,n", [(2, 2), (4, 2), (4, 4), (8, 4), (16, 4)])
def test_the_join_takes_one_route_however_many_are_offered(m: int, n: int):
    """The notebook ran this to 256×256; the shape, not the ceiling, is the point.

    Plan length must track `n` (the chosen route's depth) and ignore `m` (how
    many routes were on offer) -- a search that widened with `m` would be
    exploring alternatives it has no reason to compare.
    """
    problem = _mxn_join(m, n)
    solution = problem.solve()
    assert solution.complete
    verdict = check_plan(problem, solution)
    assert verdict.ok, verdict.violations
    # given + n chain steps + left + right + join + target
    assert len(solution.dependency_plan) == n + 5


def test_widening_the_search_does_not_lengthen_the_plan():
    """`m` is search breadth, not plan depth. Stated once, directly."""
    lengths = {m: len(_mxn_join(m, 4).solve().dependency_plan) for m in (2, 8, 32)}
    assert len(set(lengths.values())) == 1, lengths
