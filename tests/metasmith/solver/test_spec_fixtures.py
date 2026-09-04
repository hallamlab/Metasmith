"""Cases the corpus cannot produce, built by hand because no clause is tested by a
plan that never breaks it.

Three of the ten conditions fire on nothing the solver returns: the lineage half
of `conformance`, the transitive case of the ancestor walk, and
`uniqueProducer`. The decoys in `test_plan_spec` exercise the clauses; these
fixtures exercise the *semantics* -- each is a plan a person could plausibly
believe in, and the point of each is which answer the specification gives.

Every case is checked against both statements of the specification: the python
reference and the engine's extracted witness.
"""

from __future__ import annotations

import copy

import pytest

from metasmith.models.solver import Endpoint, Transform
from metasmith.models.solver_engine import SOLVER_WIRE_VERSION, CallEngine, EngineFor
from metasmith.models.solver_wire import encode_problem
from metasmith.testing.solver_spec import check_spec
from metasmith.testing.witness_check import witness_check


@pytest.fixture(scope="module")
def engine():
    info = EngineFor("solve")
    if info is None:
        pytest.skip("no msm_solver advertising `solve` (src/workflow_solver/dev.sh -b1)")
    return info


def _solve(engine, given, transforms, target):
    encoded = encode_problem(
        given, transforms, target,
        seed=42, max_iter=256, max_refine=256, wire_version=SOLVER_WIRE_VERSION,
    )
    return encoded.payload, CallEngine(engine, "solve", encoded.payload)


def _clauses(verdict) -> set[str]:
    return {v.split("/")[0].split("[")[0] for v in verdict.violations}


def _both_agree(request, reply):
    """The reference and the engine's witness must give the same verdict."""
    reference = check_spec(request, reply)
    engine_says = witness_check(request, reply)
    assert reference.ok == engine_says.ok, (
        f"reference {reference.violations} vs engine {engine_says.clauses}"
    )
    return reference


# ---------------------------------------------------------------------------
# a three-level given chain: the ancestor walk has to be transitive
# ---------------------------------------------------------------------------


def _deep_chain():
    run = Endpoint(properties={"run"})
    sid = Endpoint(properties={"sid"}, parents={run})
    reads = Endpoint(properties={"reads"}, parents={sid})

    trim = Transform()
    trim.AddRequirement(properties={"reads"})
    trim.AddProduct(properties={"trimmed"})

    # The `trimmed` slot is anchored at the RUN -- two hops above the reads its
    # binding was derived from. The lineage a step confers is one hop, so only a
    # transitive walk finds the grandparent again.
    asm = Transform()
    anchor = asm.AddRequirement(properties={"run"})
    asm.AddRequirement(properties={"trimmed"}, parents={anchor})
    asm.AddProduct(properties={"asm"})

    target = Transform()
    target.AddRequirement(properties={"asm"})
    return [{run, sid, reads}], [trim, asm], target


def test_an_anchor_two_hops_up_is_still_satisfied(engine):
    request, reply = _solve(engine, *_deep_chain())
    assert reply["complete"]
    assert _both_agree(request, reply).ok


def test_the_deep_anchor_actually_needs_the_transitive_walk(engine):
    """Otherwise the clause above passes for the wrong reason.

    A direct-parents-only ancestor test fires on every plan the corpus contains,
    so nothing there distinguishes it from the transitive one. This asserts the
    fixture reaches its anchor at a depth a one-hop test would miss.
    """
    request, reply = _solve(engine, *_deep_chain())
    nodes, eps = request["nodes"], reply["endpoints"]
    given_tr = request["given_index"]
    depths = []
    for step in reply["steps"]:
        if step["transform"] == given_tr:
            continue
        used = dict(step["used"])
        for slot, e in step["used"]:
            for anchor in nodes[slot]["parents"]:
                f = used.get(anchor)
                if f is None:
                    continue
                seen, frontier, hops = {e}, [e], 0
                while frontier and f not in seen:
                    hops += 1
                    frontier = [
                        p for x in frontier for p in eps[x]["parents"] if p not in seen
                    ]
                    seen.update(frontier)
                depths.append(hops)
    assert depths, "the fixture declares no anchors, so it tests nothing"
    assert max(depths) > 1, (
        f"every anchor resolved within one hop ({depths}); this fixture no longer "
        "exercises the transitive case and something else must"
    )


# ---------------------------------------------------------------------------
# two files that look alike: the case identity exists for
# ---------------------------------------------------------------------------


def _lookalikes():
    x = Endpoint(properties={"x"})

    def maker(tag):
        t = Transform()
        t.AddRequirement(properties={"x"})
        t.AddProduct(properties={"y"})
        t.AddProduct(properties={tag})
        return t

    k = Transform()
    k.AddRequirement(properties={"y"})
    k.AddProduct(properties={"z"})

    h = Transform()
    anchor = h.AddRequirement(properties={"y"})
    h.AddRequirement(properties={"z"}, parents={anchor})
    h.AddProduct(properties={"out"})

    target = Transform()
    for p in ("out", "f", "g"):
        target.AddRequirement(properties={p})
    return [{x}], [maker("f"), maker("g"), k, h], target


def _cross_the_anchor(request: dict, reply: dict) -> bool:
    """Rebind one anchor to a different endpoint of the same type.

    The step is left internally consistent -- its products' declared lineage is
    recomputed from the new inputs -- so `derived` does not fire and the only
    thing wrong with the plan is the crossover itself.
    """
    nodes, given_tr, eps = request["nodes"], request["given_index"], reply["endpoints"]
    for step in reply["steps"]:
        if step["transform"] == given_tr:
            continue
        used = dict(step["used"])
        for slot, _ in step["used"]:
            for anchor in nodes[slot]["parents"]:
                f = used.get(anchor)
                if f is None:
                    continue
                for alt in range(len(eps)):
                    if alt == f:
                        continue
                    if sorted(set(eps[alt]["props"])) != sorted(set(eps[f]["props"])):
                        continue
                    for bi, (sl, _) in enumerate(step["used"]):
                        if sl == anchor:
                            step["used"][bi] = [sl, alt]
                    consumed = [e for _, e in step["used"]]
                    confers = sorted(
                        set(consumed) | {p for e in consumed for p in eps[e]["parents"]}
                    )
                    for group in step["produced"]:
                        for _, pe in group:
                            eps[pe]["parents"] = confers
                    return True
    return False


def test_two_transforms_making_the_same_type_are_two_files(engine):
    """The plan is sound as returned: F's `y` and G's `y` are distinct endpoints."""
    request, reply = _solve(engine, *_lookalikes())
    assert reply["complete"]
    assert _both_agree(request, reply).ok


def test_anchoring_to_one_lookalike_and_consuming_the_other_is_rejected(engine):
    """This is what identity buys, and it is the whole argument for it.

    Both `y` endpoints have the same properties and the same lineage. A relation
    that compares them structurally cannot tell them apart, so it accepts a step
    that anchors to the file one transform made while consuming something derived
    from the file another transform made.
    """
    request, reply = _solve(engine, *_lookalikes())
    bad = copy.deepcopy(reply)
    assert _cross_the_anchor(request, bad), "the fixture could not host a crossover"
    verdict = _both_agree(request, bad)
    assert not verdict.ok
    assert _clauses(verdict) == {"conformance"}, (
        f"the crossover should be a lineage failure and nothing else, got {verdict.violations}"
    )


# ---------------------------------------------------------------------------
# two samples: anchors are the mechanism, groups are not
# ---------------------------------------------------------------------------


def _two_samples(with_anchor: bool):
    def sample(tag):
        meta = Endpoint(properties={"meta", f"s:{tag}"})
        reads = Endpoint(properties={"reads", f"s:{tag}"}, parents={meta})
        return meta, reads

    meta_a, reads_a = sample("a")
    meta_b, reads_b = sample("b")

    t = Transform()
    m = t.AddRequirement(properties={"meta"})
    if with_anchor:
        t.AddRequirement(properties={"reads"}, parents={m})
    else:
        t.AddRequirement(properties={"reads"})
    t.AddProduct(properties={"bam"})

    target = Transform()
    target.AddRequirement(properties={"bam"})
    return [{meta_a, reads_a}, {meta_b, reads_b}], [t], target


def test_a_declared_anchor_keeps_two_samples_apart(engine):
    """With the anchor, the plan is sound and each step stays within one sample."""
    request, reply = _solve(engine, *_two_samples(with_anchor=True))
    assert reply["complete"]
    assert _both_agree(request, reply).ok


def test_without_an_anchor_mixing_two_samples_violates_nothing(engine):
    """And this is deliberate, not a gap the specification failed to close.

    The groups are one per sample, but nothing in a plan is confined to one of
    them: a multi-sample workflow spans all of them by design. A transform that
    declares no anchor between its `meta` and its `reads` has not asked for them
    to come from the same sample, and a specification that refused anyway would
    be refusing on the author's behalf.
    """
    request, reply = _solve(engine, *_two_samples(with_anchor=False))
    assert reply["complete"]
    assert _both_agree(request, reply).ok


# ---------------------------------------------------------------------------
# one endpoint, two producers
# ---------------------------------------------------------------------------


def test_two_steps_emitting_one_endpoint_is_rejected(engine):
    """`rectify` returned exactly this shape until it was repaired: its endpoint
    map was keyed by signature, so two transforms' structurally equal outputs
    collapsed into one endpoint with two producers. Compiled, that is two
    processes writing one file.
    """
    request, reply = _solve(engine, *_lookalikes())
    bad = copy.deepcopy(reply)
    given_tr = request["given_index"]
    plan = [i for i, s in enumerate(bad["steps"]) if s["transform"] != given_tr]
    donor, victim = None, None
    for i in plan:
        if bad["steps"][i]["produced"] and bad["steps"][i]["produced"][0]:
            if donor is None:
                donor = bad["steps"][i]["produced"][0][0][1]
            elif victim is None:
                victim = i
                break
    assert donor is not None and victim is not None
    slot = bad["steps"][victim]["produced"][0][0][0]
    bad["steps"][victim]["produced"][0][0] = [slot, donor]

    verdict = _both_agree(request, bad)
    assert not verdict.ok
    assert "uniqueProducer" in _clauses(verdict), verdict.violations
