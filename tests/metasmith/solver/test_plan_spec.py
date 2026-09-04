from __future__ import annotations

import copy

import pytest

from metasmith.models.solver_backend import Backend
from metasmith.models.solver_engine import SOLVER_WIRE_VERSION, CallEngine, EngineFor
from metasmith.models.solver_wire import encode_problem
from metasmith.testing.solver_bench import CORPUS, STRESS_CORPUS, _libraries_root
from metasmith.testing.solver_spec import CLAUSES, check_spec
from metasmith.testing.solver_verification import generate_problem, problem_of_plan

ALL_CASES = [(n, s, d) for n, s, d in CORPUS + STRESS_CORPUS]


@pytest.fixture(scope="module")
def engine():
    info = EngineFor("solve")
    if info is None:
        pytest.skip("no msm_solver advertising `solve` (src/workflow_solver/dev.sh -b1)")
    return info


def _wire(engine, problem):
    encoded = encode_problem(
        [set(g) for g in problem.given],
        list(problem.transforms),
        problem.target,
        seed=42, max_iter=256, max_refine=256, wire_version=SOLVER_WIRE_VERSION,
    )
    return encoded.payload, CallEngine(engine, "solve", encoded.payload)


def _invariants(request: dict, reply: dict) -> dict[str, int]:
    given_tr = request["given_index"]
    plan = [s for s in reply["steps"] if s["transform"] != given_tr]
    given = [s for s in reply["steps"] if s["transform"] == given_tr]
    producers: dict[int, set[int]] = {}
    for i, s in enumerate(plan):
        for group in s["produced"]:
            for _, e in group:
                producers.setdefault(e, set()).add(i)
    presented = {e for s in given for group in s["produced"] for _, e in group}
    return {
        "two_producers": sum(1 for v in producers.values() if len(v) > 1),
        "orphan_consumed": sum(
            1 for s in plan for _, e in s["used"]
            if e not in producers and e not in presented
        ),
        "givens_missing_source": sum(
            1 for e in presented if reply["endpoints"][e]["source_node"] is None
        ),
    }


@pytest.mark.parametrize("name,seed,dials", ALL_CASES, ids=[c[0] for c in ALL_CASES])
def test_the_engine_returns_a_plan_the_specification_accepts(engine, name, seed, dials):
    assert Backend("solve") == "rust"
    request, reply = _wire(engine, generate_problem(seed, dials, name=name))
    if not reply["complete"]:
        pytest.skip("the search gave up; soundness is not the question")
    verdict = check_spec(request, reply)
    assert verdict.ok, f"{name}: {verdict}"


@pytest.mark.parametrize("name,seed,dials", ALL_CASES, ids=[c[0] for c in ALL_CASES])
def test_one_endpoint_has_one_producer_and_every_given_names_its_node(
    engine, name, seed, dials
):
    """The three things `rectify`'s repair was for, stated apart from the clauses.

    Each was violated before it: products of two steps collapsed into one
    endpoint, a consumer held an endpoint no step emitted, and `source_node` was
    absent on every given the plan used.
    """
    request, reply = _wire(engine, generate_problem(seed, dials, name=name))
    if not reply["complete"]:
        pytest.skip("the search gave up")
    assert _invariants(request, reply) == {
        "two_producers": 0, "orphan_consumed": 0, "givens_missing_source": 0,
    }


def test_the_shipped_templates_satisfy_the_specification(engine):
    root = _libraries_root()
    if root is None:
        pytest.skip("the standard library is not compiled — run `dev/libraries.sh -bm`")
    from metasmith.agents import Template

    seen = 0
    for template in Template.Discover(root):
        problem = problem_of_plan(template.spec.Solve().plan, name=template.name)
        if problem is None:
            continue
        request, reply = _wire(engine, problem)
        verdict = check_spec(request, reply)
        assert verdict.ok, f"{template.name}: {verdict}"
        assert _invariants(request, reply) == {
            "two_producers": 0, "orphan_consumed": 0, "givens_missing_source": 0,
        }, template.name
        seen += 1
    assert seen >= 4, f"only {seen} templates adjudicated"


# --------------------------------------------------------------------------
# decoys: one per clause, each derived from a reply the checker just accepted
# --------------------------------------------------------------------------


def _first_plan_step(request: dict, reply: dict) -> int:
    for i, s in enumerate(reply["steps"]):
        if s["transform"] != request["given_index"] and s["used"]:
            return i
    raise AssertionError("no non-given step consumes anything")


def _d_indexed(q, r):
    r["steps"][_first_plan_step(q, r)]["used"][0][0] = len(q["nodes"]) + 1000


def _d_shape(q, r):
    del r["steps"][_first_plan_step(q, r)]["used"][0]


def _d_conformance(q, r):
    i = _first_plan_step(q, r)
    slot = r["steps"][i]["used"][0][0]
    r["endpoints"].append({"props": [], "parents": [], "source_node": None})
    r["steps"][i]["used"][0] = [slot, len(r["endpoints"]) - 1]


def _d_emission(q, r):
    i = _first_plan_step(q, r)
    for group in r["steps"][i]["produced"]:
        for k, (slot, _) in enumerate(group):
            if q["nodes"][slot]["props"]:
                r["endpoints"].append({"props": [], "parents": [], "source_node": None})
                group[k] = [slot, len(r["endpoints"]) - 1]
                return


def _d_derived(q, r):
    # One extra parent the step did not confer. It has to be an index the
    # endpoint does not already carry, and below the endpoint's own -- a higher
    # one would trip `indexed` instead and prove nothing about this clause.
    for i, s in enumerate(r["steps"]):
        if s["transform"] == q["given_index"]:
            continue
        for group in s["produced"]:
            for _, e in group:
                carried = set(r["endpoints"][e]["parents"])
                spare = next((x for x in range(e) if x not in carried), None)
                if spare is None:
                    continue
                r["endpoints"][e]["parents"] = sorted(carried | {spare})
                return


def _d_uniqueProducer(q, r):
    i = _first_plan_step(q, r)
    _, e = r["steps"][i]["produced"][0][0]
    for j, s in enumerate(r["steps"]):
        if j != i and s["transform"] != q["given_index"] and s["produced"] and s["produced"][0]:
            s["produced"][0][0] = [s["produced"][0][0][0], e]
            return


def _d_provenance(q, r):
    i = _first_plan_step(q, r)
    slot, e = r["steps"][i]["used"][0]
    r["endpoints"].append(copy.deepcopy(r["endpoints"][e]))
    r["steps"][i]["used"][0] = [slot, len(r["endpoints"]) - 1]


def _d_givens(q, r):
    # A property id that exists but that this given does not carry. Reaching past
    # the interned table instead would trip `indexed` and prove nothing here.
    for s in r["steps"]:
        if s["transform"] != q["given_index"]:
            continue
        _, e = s["produced"][0][0]
        carried = set(r["endpoints"][e]["props"])
        spare = next(x for x in range(q["n_properties"]) if x not in carried)
        r["endpoints"][e]["props"] = sorted(carried | {spare})
        return


def _d_schedulable(q, r):
    plan = [i for i, s in enumerate(r["steps"]) if s["transform"] != q["given_index"]]
    for j in plan:
        for _, e in r["steps"][j]["used"]:
            for i in plan:
                if i < j and any(b == e for g in r["steps"][i]["produced"] for _, b in g):
                    r["steps"][i], r["steps"][j] = r["steps"][j], r["steps"][i]
                    return


def _d_target(q, r):
    for i, s in enumerate(r["steps"]):
        if s["transform"] == q["target_index"]:
            del r["steps"][i]
            return


DECOYS = {c: globals()[f"_d_{c}"] for c in CLAUSES}


@pytest.mark.parametrize("clause", sorted(DECOYS))
def test_each_clause_rejects_a_plan_built_to_break_it(engine, clause):
    """A decoy caught by the wrong clause proves nothing about this one."""
    hosted = 0
    for name, seed, dials in ALL_CASES:
        request, reply = _wire(engine, generate_problem(seed, dials, name=name))
        if not (reply["complete"] and check_spec(request, reply).ok):
            continue
        # Derived from a reply the checker has just accepted, so a rejection
        # cannot be blamed on the plan merely being malformed.
        bad_q, bad_r = copy.deepcopy(request), copy.deepcopy(reply)
        try:
            DECOYS[clause](bad_q, bad_r)
        except (AssertionError, IndexError, StopIteration):
            continue
        if bad_r == reply and bad_q == request:
            continue
        hosted += 1
        got = check_spec(bad_q, bad_r)
        assert got.violated(clause), (
            f"{clause} decoy on {name} was not caught by {clause}; "
            f"got {got.violations or 'nothing at all'}"
        )
    assert hosted, f"no corpus case can host a {clause} decoy, so it is untested"
