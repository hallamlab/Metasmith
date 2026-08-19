from __future__ import annotations

import pytest

from metasmith.testing.solver_verification import (
    GeneratorDials,
    check_plan,
    generate_problem,
)

_SINK = GeneratorDials(
    n_types=9, n_given=2, n_given_groups=2, n_extra_transforms=6,
    cycle_density=0.4, lineage_density=0.7, n_duplicate_transforms=2,
    product_group_density=0.5, target_lineage=1.0, max_requirements=3,
)

FORMERLY_UNSOUND = [
    (f"sink-{s}", s, _SINK) for s in (6503, 6807, 8575, 9087, 9375, 9927)
]

UNREACHED = ("sink-9391", 9391, _SINK)


@pytest.mark.parametrize(
    "name,seed,dials", FORMERLY_UNSOUND, ids=[c[0] for c in FORMERLY_UNSOUND]
)
def test_a_cyclic_transform_graph_yields_a_runnable_plan(name, seed, dials):
    problem = generate_problem(seed, dials, name=name)
    verdict = check_plan(problem, problem.solve())
    assert verdict.ok, verdict.violations


def test_the_instance_the_search_no_longer_reaches_says_so():
    name, seed, dials = UNREACHED
    problem = generate_problem(seed, dials, name=name)
    solution = problem.solve()
    assert not solution.complete, (
        "sink-9391 reports a complete solve -- if the search genuinely reaches it"
        " again, move it back into FORMERLY_UNSOUND rather than relaxing this"
    )
    assert not any(s.transform is problem.target for s in solution.dependency_plan)


def test_an_unsatisfiable_target_is_reported_as_unsolved():
    from metasmith.models.solver import Endpoint, Transform
    from metasmith.testing.solver_verification import SolverProblem

    transforms = []
    for w in ("w0", "w1"):
        tr = Transform()
        tr.AddRequirement(properties={"root"})
        tr.AddProduct(properties={"mid", w})
        transforms.append(tr)
    target = Transform()
    anchor = target.AddRequirement(properties={"mid", "w0"})
    target.AddRequirement(properties={"mid", "w1"}, parents={anchor})
    problem = SolverProblem(
        given=[{Endpoint(properties={"root"})}], transforms=transforms, target=target
    )

    solution = problem.solve()
    assert not solution.complete
    assert not any(s.transform is target for s in solution.dependency_plan)
    verdict = check_plan(problem, solution)
    assert not verdict.ok
    assert any("target transform" in v for v in verdict.violations), verdict.violations
