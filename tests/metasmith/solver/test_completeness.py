from __future__ import annotations

from metasmith.testing.solver_verification import (
    GeneratorDials,
    check_plan,
    generate_problem,
)


def test_a_multi_sample_solve_is_not_reported_as_a_failure():
    for seed in range(8):
        problem = generate_problem(
            seed,
            GeneratorDials(n_types=7, n_given=2, n_given_groups=3, n_extra_transforms=4),
        )
        solution = problem.solve()
        verdict = check_plan(problem, solution)
        assert verdict.ok, verdict.violations
        assert solution.complete, (
            "a sound, target-reaching, multi-sample plan reported as incomplete"
        )


def test_a_solve_that_got_nowhere_is_reported_as_a_failure():
    from metasmith.models.solver import Endpoint, Transform
    from metasmith.testing.solver_verification import SolverProblem

    producer = Transform()
    producer.AddRequirement(properties={"root"})
    producer.AddProduct(properties={"mid"})
    target = Transform()
    anchor = target.AddRequirement(properties={"mid", "absent"})
    target.AddRequirement(properties={"mid"}, parents={anchor})
    problem = SolverProblem(
        given=[{Endpoint(properties={"root"})}], transforms=[producer], target=target
    )

    solution = problem.solve()
    assert not solution.complete
    assert not check_plan(problem, solution).ok
