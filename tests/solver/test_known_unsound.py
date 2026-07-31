"""Plans the solver returns that cannot run. Pinned, not fixed.

Found by sweeping 10,000 generated problems through `check_plan`: 59 of them
came back as a plan containing at least one step whose input **no step in the
plan produces**. `Solution.complete` says `True` for every one of them.

The concentration is entirely in problems whose transform graph carries a
cycle — a transform that consumes a type and also produces it, directly or
through a second product group. That is the same shape the solver's
path-dependent loop rejection exists to catch, and which measurement showed
never fires: zero hits across the four shipped templates, the pre-existing
solver tests, and the original scratch scenarios.

These are `xfail(strict=True)` on purpose. When the solver stops returning
unrunnable plans here, the suite fails loudly and these get promoted to
ordinary assertions rather than quietly continuing to pass for the wrong
reason.
"""

from __future__ import annotations

import pytest

from metasmith.testing.solver_verification import (
    GeneratorDials,
    check_plan,
    generate_problem,
)

#: (name, seed, dials) — verified by hand for `cyclic-217`: its step 4 reads a
#: `t3` and its step 7 reads a `t2` and a `t3` that nothing writes, and the
#: target's own path runs through both.
KNOWN_UNSOUND = [
    ("cyclic-217", 217, GeneratorDials(n_types=7, n_extra_transforms=5, cycle_density=0.8)),
    ("cyclic-449", 449, GeneratorDials(n_types=7, n_extra_transforms=5, cycle_density=0.8)),
    ("pgroups-556", 556, GeneratorDials(n_types=7, n_extra_transforms=4, product_group_density=0.9)),
    ("pgroups-716", 716, GeneratorDials(n_types=7, n_extra_transforms=4, product_group_density=0.9)),
]


@pytest.mark.xfail(
    strict=True,
    reason="solver returns a plan with inputs no step produces when the "
    "transform graph carries a cycle",
)
@pytest.mark.parametrize(
    "name,seed,dials", KNOWN_UNSOUND, ids=[c[0] for c in KNOWN_UNSOUND]
)
def test_a_cyclic_transform_graph_still_yields_a_runnable_plan(name, seed, dials):
    problem = generate_problem(seed, dials, name=name)
    verdict = check_plan(problem, problem.solve())
    assert verdict.ok, verdict.violations


def test_the_solver_reports_complete_even_when_it_did_not_complete():
    """`Solution.complete` is not a completeness signal.

    `solve_by_mcts` hardcodes `complete=True` on its success return and
    discards `MctsResult.complete`. `WorkflowPlan.Generate` guards on
    `not result.complete or not result.dependency_plan`, so the first half of
    that guard never fires and the whole check rests on the plan being *empty*
    — a partial, non-empty plan walks straight through it.

    Pinned as the current behaviour, not endorsed. The target here is
    genuinely unsatisfiable: `w1` and `w0` are siblings off one root, so `w1`
    cannot be descended from `w0`.
    """
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
    assert solution.complete, "if this flipped to False, delete this test"
    assert solution.dependency_plan, "non-empty, so Generate's guard does not fire"
    assert not any(s.transform is target for s in solution.dependency_plan), (
        "the plan claims completion without applying the target"
    )
    verdict = check_plan(problem, solution)
    assert not verdict.ok
    assert any("target transform" in v for v in verdict.violations), verdict.violations
