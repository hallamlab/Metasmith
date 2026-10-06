from __future__ import annotations

import copy

import pytest

from metasmith.models.solver import Endpoint, Transform, solve_by_mcts
from metasmith.models.solver_engine import SOLVER_WIRE_VERSION, CallEngine, EngineFor
from metasmith.models.solver_wire import decode_plan, encode_problem
from metasmith.testing.solver_spec import check_spec
from metasmith.testing.witness_check import witness_check

needs_engine = pytest.mark.skipif(
    EngineFor("check") is None, reason="no staged msm_solver advertising 'check'"
)


def _overlapping_fork():
    fork = Transform()
    fork.AddRequirement(properties={"start"})
    a = fork.AddProduct(properties={"A"})
    fork.AddProduct(properties={"B"})
    fork.NewProductGroup()
    fork.AddProduct(a)
    fork.AddProduct(properties={"C"})

    from_b = Transform()
    from_b.AddRequirement(properties={"A"})
    from_b.AddRequirement(properties={"B"})
    from_b.AddProduct(properties={"done"})

    from_c = Transform()
    from_c.AddRequirement(properties={"A"})
    from_c.AddRequirement(properties={"C"})
    from_c.AddProduct(properties={"done"})

    target = Transform()
    target.AddRequirement(properties={"done"})
    given = [{Endpoint(properties={"start"})}]
    return given, [fork, from_b, from_c], target


def _encode(given, transforms, target, **kw):
    return encode_problem(
        given, transforms, target,
        seed=42, max_iter=256, max_refine=8, wire_version=SOLVER_WIRE_VERSION, **kw,
    )


def _fork_step(sol, fork):
    steps = [a for a in sol.dependency_plan if a.transform is fork]
    assert len(steps) == 1
    return steps[0]


@needs_engine
@pytest.mark.parametrize("group,via", [(0, "B"), (1, "C")])
def test_a_fork_step_carries_the_group_its_solve_is_for(group, via):
    given, transforms, target = _overlapping_fork()
    fork = transforms[0]
    sol = solve_by_mcts(given, transforms, target, fork_groups=[(fork, group)])
    assert sol.complete
    step = _fork_step(sol, fork)
    assert step.group == group
    assert len(step.produced) == 1
    assert {next(iter(d.properties)) for d in step.produced[0]} == {"A", via}


@needs_engine
def test_a_fork_with_no_choice_takes_its_first_group():
    given, transforms, target = _overlapping_fork()
    sol = solve_by_mcts(given, transforms, target)
    assert _fork_step(sol, transforms[0]).group == 0


@needs_engine
def test_a_guide_that_reaches_the_target_needs_no_search():
    given, transforms, target = _overlapping_fork()
    fork = transforms[0]
    first = solve_by_mcts(given, transforms, target, fork_groups=[(fork, 1)])
    assert first.complete
    guide = [a.transform for a in first.dependency_plan if a.transform in transforms] + [target]
    again = solve_by_mcts(given, transforms, target, fork_groups=[(fork, 1)], guide=guide)
    assert again.complete
    assert again._iterations == 1
    assert [a.transform.key for a in again.dependency_plan] == [a.transform.key for a in first.dependency_plan]


@needs_engine
def test_a_guide_from_the_other_outcome_still_reaches_this_one():
    given, transforms, target = _overlapping_fork()
    fork = transforms[0]
    first = solve_by_mcts(given, transforms, target, fork_groups=[(fork, 0)])
    guide = [a.transform for a in first.dependency_plan if a.transform in transforms] + [target]
    other = solve_by_mcts(given, transforms, target, fork_groups=[(fork, 1)], guide=guide)
    assert other.complete
    assert _fork_step(other, fork).group == 1


@needs_engine
def test_a_choice_naming_a_group_that_does_not_exist_is_refused():
    given, transforms, target = _overlapping_fork()
    with pytest.raises(Exception, match="fork group 5"):
        solve_by_mcts(given, transforms, target, fork_groups=[(transforms[0], 5)])


def _solved_fork_pair(group):
    given, transforms, target = _overlapping_fork()
    fork = transforms[0]
    enc = _encode(given, transforms, target, fork_groups=[(fork, group)])
    reply = CallEngine(EngineFor("solve"), "solve", enc.payload)
    assert decode_plan(enc, reply).complete
    i = next(k for k, s in enumerate(reply["steps"]) if s["transform"] == enc.transforms.index(fork))
    return enc.payload, reply, i


def _both_groups(q, r, i):
    t = q["transforms"][r["steps"][i]["transform"]]
    other = [d for d in t["produces"][1] if d not in t["produces"][0]]
    r["endpoints"].append({"props": [], "parents": [], "source_node": None})
    r["steps"][i]["produced"].append([[d, len(r["endpoints"]) - 1] for d in other])


def _partial_group(q, r, i):
    r["steps"][i]["produced"][0].pop()


def _undeclared_group(q, r, i):
    t = q["transforms"][r["steps"][i]["transform"]]
    union = sorted({d for g in t["produces"] for d in g})
    r["endpoints"].append({"props": [], "parents": [], "source_node": None})
    have = {d for d, _ in r["steps"][i]["produced"][0]}
    for d in union:
        if d not in have:
            r["steps"][i]["produced"][0].append([d, len(r["endpoints"]) - 1])


@needs_engine
@pytest.mark.parametrize("group", [0, 1])
def test_the_witness_accepts_either_outcome_of_a_fork(group):
    q, r, _ = _solved_fork_pair(group)
    assert witness_check(q, r).ok
    assert check_spec(q, r).ok


@needs_engine
@pytest.mark.parametrize("mutate", [_both_groups, _partial_group, _undeclared_group])
def test_the_witness_rejects_a_fork_step_that_is_not_one_declared_group(mutate):
    q, r, i = _solved_fork_pair(0)
    q, r = copy.deepcopy(q), copy.deepcopy(r)
    mutate(q, r, i)
    engine = witness_check(q, r)
    assert "shape" in engine.clauses, engine.clauses
    reference = check_spec(q, r)
    assert not reference.ok
    assert any(v.startswith("shape/") for v in reference.violations), reference.violations
