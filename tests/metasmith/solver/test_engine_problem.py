"""The engine reads a problem the same way the solver does.

Before the port can be trusted to *search*, it has to be trusted to have read
the same problem, and that is separable: everything `solve_by_mcts` derives
before its first decision is a pure function of the inputs. So the two sides are
compared on those derived maps directly, over a corpus, with no randomness
involved at all.

Doing it here rather than inferring it from a differing plan is the whole point.
`demand2product` disagreeing by one entry and a plan disagreeing by one step look
identical from the outside; only one of them says where to look.

What is compared is what Python exposes on `Solution._heuristics` -- the three
demand/production maps, plus the distance and opportunity scores that steer the
search. The maps are compared *after* pruning, because that is the state the
search actually reads.
"""

from __future__ import annotations

import pytest

from metasmith.models.solver_backend import Backend, UsePythonSolver
from metasmith.models.solver_engine import (
    SOLVER_WIRE_VERSION,
    CallEngine,
    GetEngine,
    ResetEngineCache,
    packaged_engine_path,
)
from metasmith.models.solver_wire import encode_problem
from metasmith.testing.solver_bench import CORPUS
from metasmith.testing.solver_verification import GeneratorDials, generate_problem


@pytest.fixture(scope="module")
def engine():
    # Through `GetEngine`, not `probe_engine(packaged_engine_path())` -- see the
    # `rust_engine` fixture in test_solver_engine.py for why those differ in a
    # source checkout, and what it cost to learn.
    path = packaged_engine_path()
    if path is None:
        pytest.skip("no msm_solver staged for this platform (./dev.sh -be)")
    ResetEngineCache()
    info = GetEngine()
    if info is None:
        pytest.fail(f"a binary is staged at [{path}] but could not be resolved or failed its handshake")
    return info


#: A slice wide enough to cross the dials that matter here: cycles (which the
#: distance walk's path guard has to survive), duplicate transforms (which share
#: a structural key and so block each other in that walk), lineage (which makes
#: dependencies with the same properties into *different* nodes), and product
#: groups and multi-given (which change the shape of the synthesized transform).
_PROFILES = [
    ("plain", GeneratorDials(n_types=6, n_extra_transforms=3)),
    ("cyclic", GeneratorDials(n_types=7, n_extra_transforms=5, cycle_density=0.8)),
    ("lineage", GeneratorDials(
        n_types=7, n_extra_transforms=5, lineage_density=0.9, target_lineage=1.0)),
    ("dupes", GeneratorDials(n_types=7, n_extra_transforms=3, n_duplicate_transforms=5)),
    ("pgroups", GeneratorDials(n_types=7, n_extra_transforms=4, product_group_density=0.9)),
    ("multi", GeneratorDials(n_types=7, n_given=2, n_given_groups=3, n_extra_transforms=4)),
    ("sink", GeneratorDials(
        n_types=9, n_given=2, n_given_groups=2, n_extra_transforms=6, cycle_density=0.4,
        lineage_density=0.7, n_duplicate_transforms=2, product_group_density=0.5,
        target_lineage=0.8)),
]


def _cases():
    for name, seed, dials in CORPUS:
        yield f"corpus/{name}", generate_problem(seed, dials, name=name)
    for name, dials in _PROFILES:
        for seed in range(12):
            yield f"{name}-{seed}", generate_problem(seed, dials, name=name)


def _describe_python(problem, encoded):
    """Python's derived maps, re-keyed to the indices the payload used.

    Forced onto the python path, and not as a formality: once the engine
    advertises `solve`, `problem.solve()` *is* the engine, and this file would
    quietly start comparing the engine against itself. A green run that proves
    nothing is the worst outcome available here.
    """
    with UsePythonSolver():
        assert Backend("solve") == "python", "the reference side must be python"
        solution = problem.solve()
    h = solution._heuristics

    node_index = {n: i for i, n in enumerate(encoded.nodes)}
    tr_index = {id(t): i for i, t in enumerate(encoded.transforms)}
    given_index = encoded.payload["given_index"]

    # `solve_by_mcts` synthesizes its own `given` transform, so the object in
    # these maps is not the one the encoder built. It is the only object that
    # can be unknown, and it is asserted to be the only one rather than assumed:
    # a second unknown transform would mean the encoder and the solver disagree
    # about what the problem contains, which is exactly what this file exists to
    # find.
    unknown: set[int] = set()
    def _tr(t) -> int:
        i = tr_index.get(id(t))
        if i is not None: return i
        unknown.add(id(t))
        return given_index

    def _pairs(m, value):
        return sorted((node_index[k], value(v)) for k, v in m.items())

    # The two demand maps are compared *as sequences*: Python freezes them into
    # rank order at construction and `_find_endpoints` appends candidates in the
    # order it walks them, so a map with the right members in the wrong order is
    # a different plan. `product2consumer`'s values are a `set` in Python and are
    # only ever unioned into another set, so those are compared as sets.
    out = {
        "demand2product": _pairs(
            h["demand2product"], lambda v: [node_index[x] for x in v]),
        "demand2producer": _pairs(h["demand2producer"], lambda v: [_tr(x) for x in v]),
        "product2consumer": _pairs(h["product2consumer"], lambda v: sorted(_tr(x) for x in v)),
        "distance": sorted((_tr(k), v) for k, v in h["distance_scores"].items()),
        "opportunity": sorted((_tr(k), v) for k, v in h["opportunity_scores"].items()),
        "no_path_possible": bool(h.get("no_path_possible", False)),
    }
    assert len(unknown) <= 1, "more than one transform was not in the payload"
    return out


def _describe_engine(engine, encoded):
    reply = CallEngine(engine, "describe", encoded.payload)
    return {
        "demand2product": [(k, v) for k, v in reply["demand2product"]],
        "demand2producer": [(k, v) for k, v in reply["demand2producer"]],
        "product2consumer": [(k, sorted(v)) for k, v in reply["product2consumer"]],
        "distance": [(k, v) for k, v in reply["distance"]],
        "opportunity": [(k, v) for k, v in reply["opportunity"]],
        "no_path_possible": reply["no_path_possible"],
    }


@pytest.mark.python_solver  # the reference side is a python solve
@pytest.mark.parametrize("name,problem", list(_cases()), ids=lambda x: x if isinstance(x, str) else "")
def test_the_engine_derives_what_the_solver_derives(engine, name, problem):
    encoded = encode_problem(
        problem.given, problem.transforms, problem.target,
        seed=42, max_iter=256, max_refine=256, wire_version=SOLVER_WIRE_VERSION,
    )
    mine = _describe_python(problem, encoded)
    theirs = _describe_engine(engine, encoded)
    # Compared key by key so a failure names the map rather than dumping all six.
    for key in mine:
        assert theirs[key] == mine[key], f"{name}: {key} disagrees"


def test_a_demand_with_several_producers_lists_them_in_rank_order(engine):
    """The arena index really is the T5a rank, checked where it can be seen.

    Python freezes `demand2producer` into `_transform_rank` order at
    construction. If the engine's arena were built in any other order -- the
    loader's convenience, say -- the members would still all be there and only
    the sequence would differ, which is the failure this asserts against: the
    lists must be *ascending*, since the engine's index is supposed to be that
    same rank.

    The corpus is checked for this property rather than trusted to have it. A
    single-producer problem would satisfy an ordering assertion vacuously.
    """
    problem = generate_problem(2, GeneratorDials(
        n_types=9, n_extra_transforms=7, n_duplicate_transforms=3, lineage_density=0.6))
    encoded = encode_problem(
        problem.given, problem.transforms, problem.target,
        seed=42, max_iter=256, max_refine=256, wire_version=SOLVER_WIRE_VERSION,
    )
    mine = _describe_python(problem, encoded)
    theirs = _describe_engine(engine, encoded)
    assert theirs["demand2producer"] == mine["demand2producer"]
    multi = [v for _, v in mine["demand2producer"] if len(v) > 1]
    assert multi, "this problem no longer has a dependency with several producers"
    for v in multi:
        assert v == sorted(v), "producers are not in ascending rank order"


def test_the_engine_reads_the_shipped_templates(engine):
    """The four real libraries, which is where the property table gets big.

    Generated problems have a handful of properties, so their bitsets are one
    word and the striding is never exercised. A shipped template carries
    hundreds, which is the first time a property index above 63 exists at all --
    and an off-by-one in the word/bit split would pass every test above.

    Skipped rather than failed without the sibling checkout, as elsewhere: the
    generated corpus above still runs, and it is the part that runs in CI.
    """
    from metasmith.testing.solver_bench import _libraries_root
    from metasmith.testing.solver_verification import problem_of_plan

    root = _libraries_root()
    if root is None:
        pytest.skip("the standard library is not compiled — run `dev/libraries.sh -bm`")
    from metasmith.agents import Template

    seen = 0
    for template in Template.Discover(root):
        task = template.spec.Solve()
        problem = problem_of_plan(task.plan, name=template.name)
        if problem is None: continue
        encoded = encode_problem(
            problem.given, problem.transforms, problem.target,
            seed=42, max_iter=256, max_refine=256, wire_version=SOLVER_WIRE_VERSION,
        )
        assert encoded.payload["n_properties"] > 64, \
            f"{template.name} has too few properties to be testing the striding"
        mine = _describe_python(problem, encoded)
        theirs = _describe_engine(engine, encoded)
        for key in mine:
            assert theirs[key] == mine[key], f"{template.name}: {key} disagrees"
        seen += 1
    assert seen == 4, f"expected the four shipped templates, adjudicated {seen}"
