"""The plans that used to come back unrunnable. Fixed, and kept as the pin.

For most of this project's history these seven were `xfail(strict=True)`: the
solver returned a plan containing a step whose input **no step in the plan
produces**, with `Solution.complete` saying `True` for every one. They
concentrate in problems whose transform graph carries a cycle, and
`test_refiner_validity.py` traced why — the refiner accepted a state whose
production graph was cyclic, and `rectify` then rewrote endpoints in an order
that cannot be topological, converting the cycle into an unproduced input and
erasing every trace that a cycle had been involved.

That is fixed. `_is_valid` now asks for *schedulability* — every step runnable
with all of its inputs available, starting from the givens — which fails on a
cycle and on an unproduced input and on nothing else. The seven flipped to sound
together, a 10,000-problem sweep went from 7 unrunnable plans to 0, and no
fingerprint anywhere moved.

They stay here, as assertions rather than expectations, because they are the
hardest instances anyone has found: the `sink` profile with every dial up, and
these seven the residue of sweeping ten thousand problems for the shape. A
regression in the refiner's validity check shows up here first.

**Which problems land in a set like this is a property of how the solver
decides, not only of what it decides**, and that is worth remembering if these
ever need re-deriving. Swapping numpy's stream for ChaCha8 (T4) took the count
from 59 to 5 and replaced the membership outright; stating the iteration order
the solver used to take from CPython's hash tables (T5a) took it from 5 to 7 and
replaced it again; replacing the backward distance walk (T9) left six of the
seven solving and took `sink-9391` out of reach of the search entirely. None
touched the defect. So a *failure* here is a real regression, but a failure
after a change to a decision rule should be checked against a fresh sweep before
it is believed to be one.
"""

from __future__ import annotations

import pytest

from metasmith.testing.solver_verification import (
    GeneratorDials,
    check_plan,
    generate_problem,
)

#: The `sink` profile: two given groups, a cycle dial, dense lineage, duplicate
#: transforms and product groups all at once. It was the last profile still
#: producing unrunnable plans — `cyclic` and `pgroups`, which supplied 39 of the
#: 59 found in T1, had been clean since T4.
_SINK = GeneratorDials(
    n_types=9, n_given=2, n_given_groups=2, n_extra_transforms=6,
    cycle_density=0.4, lineage_density=0.7, n_duplicate_transforms=2,
    product_group_density=0.5, target_lineage=1.0, max_requirements=3,
)

#: (name, seed, dials) — the unsound cases from the last 10,000-problem sweep
#: that the search still finds an answer for.
FORMERLY_UNSOUND = [
    (f"sink-{s}", s, _SINK) for s in (6503, 6807, 8575, 9087, 9375, 9927)
]

#: The seventh. T9's distance walk is a single-pass BFS rather than a longest
#: simple path, which is what makes it terminate on a densely cyclic universe,
#: and the depth signal it gives the search is correspondingly flatter. On the
#: two cycle-dense generator profiles that costs solves — 18 to 63 unsolved in
#: 2,400 — and this is one of them.
UNREACHED = ("sink-9391", 9391, _SINK)


@pytest.mark.parametrize(
    "name,seed,dials", FORMERLY_UNSOUND, ids=[c[0] for c in FORMERLY_UNSOUND]
)
def test_a_cyclic_transform_graph_yields_a_runnable_plan(name, seed, dials):
    problem = generate_problem(seed, dials, name=name)
    verdict = check_plan(problem, problem.solve())
    assert verdict.ok, verdict.violations


def test_the_instance_the_search_no_longer_reaches_says_so():
    """A refusal is an acceptable answer here; a confident wrong one is not.

    `sink-9391` is the hardest instance this project has generated and the
    search stopped finding a plan for it. That is the failure mode this codebase
    accepts — no answer in preference to a bad one — so what is pinned is the
    *honesty* of it: `complete` is False, and the partial plan carries no
    application of the target. The regression to fear is not that this starts
    solving again, which would be welcome; it is that it starts returning a plan
    while still not having one.
    """
    name, seed, dials = UNREACHED
    problem = generate_problem(seed, dials, name=name)
    solution = problem.solve()
    assert not solution.complete, (
        "sink-9391 reports a complete solve -- if the search genuinely reaches it"
        " again, move it back into FORMERLY_UNSOUND rather than relaxing this"
    )
    assert not any(s.transform is problem.target for s in solution.dependency_plan)


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
