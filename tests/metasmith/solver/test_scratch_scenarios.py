from __future__ import annotations

import pytest

from metasmith.models.solver import Endpoint, Transform
from metasmith.testing.solver_verification import (
    SolverProblem,
    check_plan,
    plan_fingerprint,
)


def _reciprocal_web() -> SolverProblem:
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
    problem = _reciprocal_web()
    solution = problem.solve()
    assert solution.complete
    verdict = check_plan(problem, solution)
    assert verdict.ok, verdict.violations
    assert len(solution.dependency_plan) == 6


def test_the_reciprocal_web_is_stable_across_seeds():
    prints = {plan_fingerprint(_reciprocal_web().solve(seed=s)) for s in (42, 7, 1234)}
    assert len(prints) == 1, "the route through the web depends on the seed"


def _mxn_join(m: int, n: int) -> SolverProblem:
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
    problem = _mxn_join(m, n)
    solution = problem.solve()
    assert solution.complete
    verdict = check_plan(problem, solution)
    assert verdict.ok, verdict.violations
    assert len(solution.dependency_plan) == n + 5


def test_widening_the_search_does_not_lengthen_the_plan():
    lengths = {m: len(_mxn_join(m, 4).solve().dependency_plan) for m in (2, 8, 32)}
    assert len(set(lengths.values())) == 1, lengths
