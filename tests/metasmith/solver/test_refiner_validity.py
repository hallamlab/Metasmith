"""How the refiner decides a state is valid, and the chain that used to break.

`refine_mcts` is handed a sound plan and, for most of this project's history,
could return an unsound one. This file pins the mechanism link by link, because
`test_known_unsound.py` pins only the *outcome*, and an outcome can move for
reasons that have nothing to do with the defect — T4 showed exactly that. The
chain, as it was:

1. the search hands `refine_mcts` a plan that is acyclic and fully produced;
2. the refiner rebinds an input to an endpoint produced by a later step,
   creating a cycle, and `validate_node` calls that state valid;
3. `rectify` rewrites endpoints in `get_order` order, which cannot be
   topological on a cyclic graph, so a consumer is rewritten before its
   producer and keeps an endpoint object the producer no longer emits.

Step 3 converted a cycle into the violation the checker reported — an input no
step produces — and the plan came out acyclic, so nothing downstream could tell
a cycle had been involved.

**Link 2 is fixed**, which is the only repairable point in the chain. Link 3 is
not a defect: `get_order` is a BFS by depth from the givens and it is correct;
asked to order a cycle it flattens the members onto one depth because there is
no answer to give. The fix is that a cyclic state no longer reaches it.
`_is_valid` asks for schedulability — every step runnable with *all* of its
inputs available — which is exactly the promise that `get_order` finds a total
order. The old check walked forward over *consumers* and reached a step as soon
as **one** input was available, so a cycle off the side of the walk was
invisible.

The tests below assert the chain is broken and keep tracing the same links, so a
regression says which one came back.

**Opt-in** (`--python-solver`). Tracing is how this file localises a break, and
there is nothing to trace in the engine; the outcome it protects is pinned
implementation-agnostically in `test_known_unsound.py`, which does run by
default. Reach for this when that one goes red and the reason is not obvious.

The anchor problem is chosen, not arbitrary: it has to exercise the whole chain
at once, and **which problems do is a property of how the solver breaks ties**,
not of the defect. `cyclic-217` was the anchor until T4 swapped numpy's stream
for the ChaCha8 contract; `sink-6623` until T5a stated the iteration order the
solver had been taking from CPython's hash tables; `sink-9391` until T9 replaced
the backward distance walk and the search stopped finding a plan for it at all —
an instance the search never solves reaches the refiner not at all, and traces
nothing. Each change left the mechanism untouched and moved which problems fell
into it, which is the thing to check first when this file goes red.

`sink-9396` is the current anchor, picked by sweeping the sink profile for a seed
that clears every link above in one solve. It puts the refiner under comparable
pressure to its predecessor: of the 2,744 states it validates, 1,764 are cyclic,
and 745 of the rejections come from the loop branch.
"""

from __future__ import annotations

import sys

import pytest

pytestmark = pytest.mark.python_solver


@pytest.fixture(autouse=True)
def _python_solver():
    """Pin the search to the python implementation for this whole file --
    this file counts how often particular branches of the *python* refiner fire,
    by patching them; with the search running elsewhere it would count zero and
    assert nothing.
    """
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

#: The anchor: the search hands the refiner a sound plan, cyclic states are put
#: in front of the refiner and rejected, and the loop-rejection branch fires 745
#: times in the one solve. All four links, one problem.
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


def _trace_validations(problem: SolverProblem) -> list[tuple[bool, bool]]:
    """`(state.valid, production graph is cyclic)` for every validated state.

    `validate_node` is a closure inside `refine_mcts`, so there is nothing to
    monkeypatch; the verdict is read off the frame as it returns. Hooks the
    function name, never a line number.
    """
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


def test_refinement_does_not_introduce_a_cycle():
    """The link that used to break, and the one the fix is in.

    The old `validate_node` walked forward from the given application following
    *consumers* of each produced endpoint, so a step was reached as soon as
    **one** of its inputs was available and its other inputs were never checked
    for being produced at all -- only the target's direct inputs got that test.
    A cycle sitting off that walk was invisible. Schedulability cannot miss it:
    a step is only ever reached once every one of its inputs is available, and
    the members of a cycle are each waiting on another, so none is.
    """
    seed, dials = ANCHOR_CASE
    stages = _trace_stages(generate_problem(seed, dials, name=ANCHOR))
    assert stages["rectify_in"], "rectify never ran"
    assert not any(_production_cycle(steps) for steps in stages["rectify_in"])


def test_the_anchor_still_puts_cyclic_states_in_front_of_the_refiner():
    """The test above is only meaningful if there is something to reject.

    If the refiner stopped *generating* cyclic states -- a change in tie-breaking
    would do it -- then "no cyclic state reached rectify" would pass for the
    wrong reason and the fix would be untested. So this asserts the pressure is
    still there: the anchor's refiner produces cyclic candidates, and they are
    rejected rather than absent.
    """
    seed, dials = ANCHOR_CASE
    verdicts = _trace_validations(generate_problem(seed, dials, name=ANCHOR))
    assert verdicts, "validate_node never ran"
    cyclic = [valid for valid, has_cycle in verdicts if has_cycle]
    assert cyclic, (
        f"{ANCHOR} no longer produces a single cyclic candidate, so nothing here "
        "exercises the rejection -- re-anchor from a fresh sweep"
    )
    assert not any(cyclic), (
        f"{sum(cyclic)} of {len(cyclic)} cyclic states were accepted as valid"
    )
    assert any(valid for valid, _ in verdicts), (
        "every state was rejected, so the refiner has nothing to choose between "
        "and the rejection above proves less than it looks like it does"
    )


def test_rectify_is_never_asked_to_order_a_cycle():
    """The consequence, inverted.

    `rectify` unifies endpoint objects in `get_order` order, and `get_order` is
    a BFS by depth from the givens. On a cyclic graph the members of the cycle
    are never reached, so they all land on one flat depth and some consumer is
    processed before its producer, keeping an endpoint the producer then
    replaces -- a cycle laundered into an unproduced input, in a plan that looks
    well-formed and cannot run.

    That is not a defect in `get_order`; there is no order for it to find. The
    guarantee is upstream, and this is where its absence would surface.
    """
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
    """`_is_valid`'s `return False # looped` fires, and now fires in anger.

    When the rejection was a signature-repeat test along a forward walk, this
    test existed to settle whether the branch was dead code at all: it fires
    nowhere in the four shipped templates, nowhere in the pre-existing tests,
    and nowhere in the original scratch scenarios. It took a generated cyclic
    instance to reach it, and it was incomplete when it did.

    The branch is now the schedulability rejection -- no step became runnable,
    so the state has a cycle or an input nothing produces -- and it is both
    reachable *and* complete. Keeping the test costs nothing and would catch a
    rewrite that made it unreachable, which is how a validity check silently
    stops checking.
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
