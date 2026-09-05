from __future__ import annotations

import sys

import pytest

pytestmark = pytest.mark.python_solver


@pytest.fixture(autouse=True)
def _python_solver():
    from metasmith.models.solver_backend import UsePythonSolver
    with UsePythonSolver():
        yield

from metasmith.models import solver as solver_module
from metasmith.models.solver import Application, Endpoint, Transform
from metasmith.testing.solver_verification import (
    GeneratorDials,
    SolverProblem,
    check_plan,
    generate_problem,
)

ANCHOR = "sink-9396"
ANCHOR_CASE = (
    9396,
    GeneratorDials(
        n_types=9, n_given=2, n_given_groups=2, n_extra_transforms=6,
        cycle_density=0.4, lineage_density=0.7, n_duplicate_transforms=2,
        product_group_density=0.5, target_lineage=1.0, max_requirements=3,
    ),
)


def _production_cycle(steps) -> bool:
    producer = {}
    for i, a in enumerate(steps):
        for group in a.produced:
            for e in group.values():
                producer[id(e)] = i
    edges = {
        i: {producer[id(e)] for e in a.used.values() if id(e) in producer}
        for i, a in enumerate(steps)
    }
    colour: dict[int, int] = {}

    def walk(n: int) -> bool:
        colour[n] = 1
        for m in edges[n]:
            if colour.get(m) == 1:
                return True
            if colour.get(m) is None and walk(m):
                return True
        colour[n] = 2
        return False

    return any(walk(n) for n in edges if colour.get(n) is None)


def _trace_stages(problem: SolverProblem) -> dict[str, list]:
    seen: dict[str, list] = {"refine_in": [], "rectify_in": []}

    def tracer(frame, event, arg):
        if event != "call" or not frame.f_code.co_filename.endswith("solver.py"):
            return None
        name = frame.f_code.co_name
        if name == "refine_mcts":
            seen["refine_in"].append(list(frame.f_locals["initial_solution"]))
        elif name == "rectify":
            seen["rectify_in"].append(list(frame.f_locals["solution"]))
        return None

    old = sys.gettrace()
    sys.settrace(tracer)
    try:
        problem.solve()
    finally:
        sys.settrace(old)
    return seen


def _trace_validations(problem: SolverProblem) -> list[tuple[bool, bool]]:
    out: list[tuple[bool, bool]] = []

    def local(frame, event, arg):
        if event == "return":
            state = frame.f_locals.get("state")
            if state is not None:
                out.append((bool(state.valid), _production_cycle(list(state.steps))))
        return local

    def tracer(frame, event, arg):
        if (
            event == "call"
            and frame.f_code.co_name == "validate_node"
            and frame.f_code.co_filename.endswith("solver.py")
        ):
            return local
        return None

    old = sys.gettrace()
    sys.settrace(tracer)
    try:
        problem.solve()
    finally:
        sys.settrace(old)
    return out


def test_the_search_hands_the_refiner_a_sound_plan():
    seed, dials = ANCHOR_CASE
    stages = _trace_stages(generate_problem(seed, dials, name=ANCHOR))
    assert stages["refine_in"], "refine_mcts never ran"
    for steps in stages["refine_in"]:
        produced = {id(e) for a in steps for g in a.produced for e in g.values()}
        orphans = [
            sorted(e.properties)
            for a in steps for e in a.used.values() if id(e) not in produced
        ]
        assert not orphans, f"the search itself emitted orphan inputs: {orphans}"
        assert not _production_cycle(steps)


def test_refinement_does_not_introduce_a_cycle():
    seed, dials = ANCHOR_CASE
    stages = _trace_stages(generate_problem(seed, dials, name=ANCHOR))
    assert stages["rectify_in"], "rectify never ran"
    assert not any(_production_cycle(steps) for steps in stages["rectify_in"])


def test_no_cyclic_state_reaches_the_refiner_and_none_would_be_accepted():
    """The prune prevents what this used to catch, so the premise moved.

    This asserted that `sink-9396` still handed the refiner at least one cyclic
    candidate, so that the rejection below was exercised by something real. Since
    the lineage prune runs on the refiner path, no cyclic candidate is generated
    at all: **0 of 6,256** validated states across the eleven templates at three
    seeds, and none from this generator either.

    That assertion is therefore gone rather than re-anchored -- a sweep for a new
    anchor is a search for something the prune has removed. What remains is the
    half that still means something: if a cyclic state ever does reach the
    validator it must be rejected, and something must stay valid for the refiner
    to choose between. `_is_valid`'s `# looped` branch is now defence in depth
    rather than a live path, and `test_the_loop_rejection_branch_is_reachable`
    is what keeps it from being deleted as dead.
    """
    seed, dials = ANCHOR_CASE
    verdicts = _trace_validations(generate_problem(seed, dials, name=ANCHOR))
    assert verdicts, "validate_node never ran"
    cyclic = [valid for valid, has_cycle in verdicts if has_cycle]
    assert not any(cyclic), (
        f"{sum(cyclic)} of {len(cyclic)} cyclic states were accepted as valid"
    )
    assert any(valid for valid, _ in verdicts), (
        "every state was rejected, so the refiner has nothing to choose between"
    )


def test_rectify_is_never_asked_to_order_a_cycle():
    seed, dials = ANCHOR_CASE
    problem = generate_problem(seed, dials, name=ANCHOR)
    stages = _trace_stages(problem)
    assert not any(_production_cycle(steps) for steps in stages["rectify_in"]), (
        "a cyclic state reached rectify -- link 2 has regressed"
    )
    solution = problem.solve()
    assert not _production_cycle(solution.dependency_plan)
    verdict = check_plan(problem, solution)
    assert verdict.ok, verdict.violations


def test_the_loop_rejection_branch_is_reachable():
    marker = [
        i + 1
        for i, line in enumerate(
            open(solver_module.__file__, encoding="utf-8").read().splitlines()
        )
        if line.rstrip().endswith("# looped")
    ]
    assert len(marker) == 1, (
        f"expected exactly one `# looped` marker in solver.py, found {marker}"
    )
    looped_line = marker[0]

    exits: list[int] = []
    last: dict[int, int] = {}

    def local(frame, event, arg):
        if event == "line":
            last[id(frame)] = frame.f_lineno
        elif event == "return":
            exits.append(last.pop(id(frame), -1))
        return local

    def tracer(frame, event, arg):
        if (
            event == "call"
            and frame.f_code.co_name == "_is_valid"
            and frame.f_code.co_filename.endswith("solver.py")
        ):
            return local
        return None

    seed, dials = ANCHOR_CASE
    problem = generate_problem(seed, dials, name=ANCHOR)
    old = sys.gettrace()
    sys.settrace(tracer)
    try:
        problem.solve()
    finally:
        sys.settrace(old)

    assert exits, "_is_valid never ran"
    assert looped_line in exits, (
        "no `_is_valid` call returned from the loop-rejection line; the branch "
        "is unreachable on a problem that provably reaches it, so either the "
        "walk or this attribution has changed"
    )


def test_two_applications_can_share_one_signature():
    tr = Transform()
    dep = tr.AddRequirement(properties={"a"})
    tr.AddProduct(properties={"b"})
    ep = Endpoint(properties={"a"})

    twins = [
        Application(initial_timeline=0, transform=tr, used={dep: ep}, produced=[{}])
        for _ in range(2)
    ]
    assert twins[0] is not twins[1]
    assert twins[0].Signature() == twins[1].Signature()

    steps = list(twins)
    kept = [s for s in steps if s.Signature() != twins[0].Signature()]
    assert kept == [], (
        "signature-keyed removal drops both twins -- if this ever keeps one, "
        "expand_node has been fixed and the note above is stale"
    )


def test_duplicate_transforms_still_yield_a_runnable_plan():
    problem = generate_problem(
        5, GeneratorDials(n_types=8, n_extra_transforms=4, n_duplicate_transforms=4)
    )
    keys = [tr.key for tr in problem.transforms]
    assert len(keys) != len(set(keys)), "the dial stopped producing duplicates"
    verdict = check_plan(problem, problem.solve())
    assert verdict.ok, verdict.violations
