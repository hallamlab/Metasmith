"""Plans the solver returns that cannot run. Pinned, not fixed.

Found by sweeping 10,000 generated problems through `check_plan`: some come back
as a plan containing at least one step whose input **no step in the plan
produces**. `Solution.complete` says `True` for every one of them.

They concentrate in problems whose transform graph carries a cycle — a
transform that consumes a type and also produces it, directly or through a
second product group. That is the same shape the solver's path-dependent loop
rejection exists to catch. It does catch some of them, and
`test_refiner_validity.py` traces what happens to the rest: the refiner accepts
a state whose production graph is cyclic, and `rectify` then rewrites endpoints
in an order that cannot be topological, converting the cycle into an input no
step produces. Those are the mechanism; these are the outcome.

**Which problems land here is a property of the random stream, not only of the
solver.** Swapping numpy's stream for the ChaCha8 contract in T4 changed the
count from 59 to 5 and changed the membership completely: all 59 of the old set
come back sound under the new stream, and all 5 of the new set were sound under
the old one. Nothing about the defect was touched. So an XPASS here has two
possible causes and they must be told apart — either the laundering was fixed,
or the anchors went stale because the stream moved. Check
`test_refiner_validity.py` first; if the mechanism is still live, re-anchor
these from a fresh sweep rather than promoting them.

These are `xfail(strict=True)` on purpose: a silent pass is exactly the failure
mode that would let a fix-by-accident be mistaken for a fix.
"""

from __future__ import annotations

import pytest

from metasmith.testing.solver_verification import (
    GeneratorDials,
    check_plan,
    generate_problem,
)

#: The `sink` profile: two given groups, a cycle dial, dense lineage, duplicate
#: transforms and product groups all at once. Under the current stream it is the
#: only profile that still produces unrunnable plans — `cyclic` and `pgroups`,
#: which supplied 39 of the previous 59, are now clean.
_SINK = GeneratorDials(
    n_types=9, n_given=2, n_given_groups=2, n_extra_transforms=6,
    cycle_density=0.4, lineage_density=0.7, n_duplicate_transforms=2,
    product_group_density=0.5, target_lineage=1.0, max_requirements=3,
)

#: (name, seed, dials) — every unsound case in a 10,000-problem sweep.
KNOWN_UNSOUND = [(f"sink-{s}", s, _SINK) for s in (4047, 6623, 8335, 8983, 9391)]


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


def test_an_unsatisfiable_target_is_reported_as_unsolved():
    """`Solution.complete` now carries the search's own verdict.

    It used to be hardcoded `True`, which made the first half of
    `WorkflowPlan.Generate`'s `not result.complete or not
    result.dependency_plan` guard dead: a partial, non-empty plan walked
    straight through it and became a workflow. The target here is genuinely
    unsatisfiable -- `w1` and `w0` are siblings off one root, so `w1` cannot be
    descended from `w0` -- and the honest answer is the one asserted.
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
    assert not solution.complete
    assert not any(s.transform is target for s in solution.dependency_plan)
    verdict = check_plan(problem, solution)
    assert not verdict.ok
    assert any("target transform" in v for v in verdict.violations), verdict.violations
