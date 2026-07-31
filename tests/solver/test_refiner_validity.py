"""How the refiner decides a state is valid, and where that goes wrong.

`refine_mcts` is handed a sound plan and may return an unsound one. This file
pins the mechanism, one link at a time, because the `xfail`s in
`test_known_unsound.py` only pin the *outcome* and an outcome can be fixed by
accident -- or, as T4 showed, by a change of random stream. The chain, measured:

1. the search hands `refine_mcts` a plan that is acyclic and fully produced;
2. the refiner rebinds an input to an endpoint produced by a later step,
   creating a cycle, and `validate_node` calls that state valid;
3. `rectify` rewrites endpoints in `get_order` order, which cannot be
   topological on a cyclic graph, so a consumer is rewritten before its
   producer and keeps an endpoint object the producer no longer emits.

Step 3 is what converts a cycle into the violation the checker reports: an
input no step produces. The plan comes out acyclic, so nothing downstream can
tell it was ever a cycle.

The tracing here hooks *function names*, never line numbers, except in
`test_the_loop_rejection_branch_is_reachable`, which finds its line by the
marker comment in the source.

The anchor problem is chosen, not arbitrary: it has to exhibit all four links at
once, and **which problems do is a property of how the solver breaks ties**, not
of the defect. `cyclic-217` was the anchor until T4 swapped numpy's stream for
the ChaCha8 contract; `sink-6623` until T5a stated the iteration order the
solver had been taking from CPython's hash tables. Each change left the
mechanism below untouched and moved which problems fall into it.

`sink-9391` is the current anchor because it is the one case that has survived
both, and it exercises the chain hardest — the loop-rejection branch fires 2259
times in the one solve. Re-anchoring is expected maintenance whenever a decision
rule changes; re-deriving it from a fresh `check_plan` sweep is the way, and an
anchor that stops exhibiting the chain is not evidence of a fix.
"""

from __future__ import annotations

import sys

import pytest

from metasmith.models import solver as solver_module
from metasmith.models.solver import Application, Endpoint, Transform
from metasmith.testing.solver_verification import (
    GeneratorDials,
    SolverProblem,
    check_plan,
    generate_problem,
)

#: The anchor: the search hands the refiner a sound plan, a cyclic state reaches
#: `rectify`, the returned plan is unrunnable, and the loop-rejection branch
#: fires 2259 times in the one solve. All four links, one problem.
ANCHOR = "sink-9391"
ANCHOR_CASE = (
    9391,
    GeneratorDials(
        n_types=9, n_given=2, n_given_groups=2, n_extra_transforms=6,
        cycle_density=0.4, lineage_density=0.7, n_duplicate_transforms=2,
        product_group_density=0.5, target_lineage=1.0, max_requirements=3,
    ),
)


def _production_cycle(steps) -> bool:
    """Does the production graph over these applications carry a cycle?"""
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
    """Snapshot the step list entering `refine_mcts` and entering `rectify`."""
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


def test_the_search_hands_the_refiner_a_sound_plan():
    """The unsoundness is not the search's. Establishes where to look."""
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


@pytest.mark.xfail(
    strict=True,
    reason="the refiner accepts a state whose production graph is cyclic",
)
def test_refinement_does_not_introduce_a_cycle():
    """The link that actually breaks.

    `validate_node`'s forward walk starts at the given application and follows
    *consumers* of each produced endpoint, so an application is reached as soon
    as **one** of its inputs is available -- its other inputs are never checked
    for being produced at all. Only the target's direct inputs get that test.
    A cycle sitting off that walk is therefore invisible, and the endpoints
    involved are still provisional at this point (`expand_node` passes
    `mock_produced=step.produced`, "rectify later"), so identity has not yet
    settled into the shape the cycle would be visible in.
    """
    seed, dials = ANCHOR_CASE
    stages = _trace_stages(generate_problem(seed, dials, name=ANCHOR))
    assert stages["rectify_in"], "rectify never ran"
    assert not any(_production_cycle(steps) for steps in stages["rectify_in"])


def test_rectify_launders_a_cycle_into_missing_inputs():
    """The consequence, pinned so a fix upstream cannot hide here.

    `rectify` unifies endpoint objects in `get_order` order. On a cyclic graph
    no order is topological, so some consumer is rewritten before its producer
    and keeps an endpoint the producer then replaces. The cycle disappears and
    an unproduced input appears in its place -- a plan that looks well-formed
    and cannot run.
    """
    seed, dials = ANCHOR_CASE
    problem = generate_problem(seed, dials, name=ANCHOR)
    stages = _trace_stages(problem)
    assert any(_production_cycle(steps) for steps in stages["rectify_in"]), (
        "no cyclic state reached rectify -- if the refiner was fixed, this test "
        "and the xfail above should both go"
    )
    solution = problem.solve()
    assert not _production_cycle(solution.dependency_plan), "cycle survived rectify"
    violations = check_plan(problem, solution).violations
    assert any("no step produces" in v for v in violations), violations


def test_the_loop_rejection_branch_is_reachable():
    """`_is_valid`'s `return False # looped` does fire -- just never in anger.

    Measurement across the four shipped templates, the tests in this axis and
    the original scratch scenarios found zero hits, which left it open whether
    the branch was dead code. It is not: this problem drives it 177 times in
    one solve. What it is, is *incomplete* -- see the xfail above, where states
    it should have caught get through anyway. Both facts have to be carried
    into the Rust port; a port that drops the branch as unreachable would be
    wrong, and one that reimplements it faithfully inherits the hole.
    """
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
    """Signature collisions are semantics, not hash accidents.

    `Application.Signature()` is transform key plus dependency bindings, so two
    distinct applications of the *same* transform over the *same* endpoints
    collide by construction -- and the solver relies on that to dedup. It also
    trips over it: `expand_node` removes the step it is replacing with
    `[s for s in state.steps if s.Signature() != step.Signature()]`, which drops
    **both** members of a colliding pair.

    Kept as a pin for the Rust port, where `ApplicationId` and
    `ApplicationSig` are meant to be separate types precisely so the compiler
    refuses the confusion this expression makes.
    """
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
    """The collision above, under a real solve rather than by construction."""
    problem = generate_problem(
        5, GeneratorDials(n_types=8, n_extra_transforms=4, n_duplicate_transforms=4)
    )
    keys = [tr.key for tr in problem.transforms]
    assert len(keys) != len(set(keys)), "the dial stopped producing duplicates"
    verdict = check_plan(problem, problem.solve())
    assert verdict.ok, verdict.violations
