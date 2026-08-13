"""What `Solution.complete` means, now that it means anything.

It was hardcoded `True`, which made `WorkflowPlan.Generate`'s
`not result.complete` guard dead code. Propagating the search's own flag
instead turned out to be wrong in the other direction: that flag is set only
when every timeline resolves on a single pass, and a multi-sample search
normally exits by running its frontier down while holding a perfectly good
merged plan. Every one of 1250 generated multi-given instances left that way.

So the flag now says `solved_state is not None` -- did the search ever merge a
solved timeline in. The two failure shapes it must keep apart:

* nothing solved: `state` is an arbitrary unfinished timeline, and the plan is
  as long as the iteration budget. `complete` must be False.
* something solved: a merged plan exists and the checker passes it.
  `complete` must be True, however the loop exited.

Soundness is a separate question with a separate answer -- `check_plan`.
`complete` is about whether the search got anywhere, not whether what it got
can run; `test_known_unsound.py` holds the cases where those two disagree.
"""

from __future__ import annotations

from metasmith.testing.solver_verification import (
    GeneratorDials,
    check_plan,
    generate_problem,
)


def test_a_multi_sample_solve_is_not_reported_as_a_failure():
    """The false negative that a naive propagation of the search flag creates.

    These are exactly the problems real workflows are: several samples, one
    plan merged across them. Reporting them incomplete would have emptied every
    multi-sample plan and replaced it with diagnostic hints.
    """
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
    """The true negative, from the other side of the same discriminator."""
    from metasmith.models.solver import Endpoint, Transform
    from metasmith.testing.solver_verification import SolverProblem

    # `mid` is reachable, but the target wants two of them in a lineage that
    # the single producer cannot supply.
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
