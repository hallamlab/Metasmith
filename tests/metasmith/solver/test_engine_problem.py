from __future__ import annotations

import pytest

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
    path = packaged_engine_path()
    if path is None:
        pytest.skip("no msm_solver staged for this platform (./dev.sh -be)")
    ResetEngineCache()
    info = GetEngine()
    if info is None:
        pytest.fail(f"a binary is staged at [{path}] but could not be resolved or failed its handshake")
    return info


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


def test_a_demand_with_several_producers_lists_them_in_rank_order(engine):
    problem = generate_problem(2, GeneratorDials(
        n_types=9, n_extra_transforms=7, n_duplicate_transforms=3, lineage_density=0.6))
    encoded = encode_problem(
        problem.given, problem.transforms, problem.target,
        seed=42, max_iter=256, max_refine=256, wire_version=SOLVER_WIRE_VERSION,
    )
    described = _describe_engine(engine, encoded)
    multi = [v for _, v in described["demand2producer"] if len(v) > 1]
    assert multi, "this problem no longer has a dependency with several producers"
    for v in multi:
        assert v == sorted(v), "producers are not in ascending rank order"


def test_the_engine_reads_the_shipped_templates(engine):
    from metasmith.testing.solver_bench import _libraries_root
    from metasmith.testing.solver_verification import problem_of_plan

    root = _libraries_root()
    if root is None:
        pytest.skip("the standard library is not compiled — run `dev/libraries.sh -bm`")
    from metasmith.agents import Template

    seen = shipped = 0
    for template in Template.Discover(root):
        shipped += 1
        task = template.spec.Solve()
        problem = problem_of_plan(task.plan, name=template.name)
        if problem is None: continue
        encoded = encode_problem(
            problem.given, problem.transforms, problem.target,
            seed=42, max_iter=256, max_refine=256, wire_version=SOLVER_WIRE_VERSION,
        )
        assert encoded.payload["n_properties"] > 64, \
            f"{template.name} has too few properties to be testing the striding"
        described = _describe_engine(engine, encoded)
        # A template that solves has a path to its target, and the striding bug
        # this guards -- a property id past the first word of a dense bit set --
        # shows up as an empty derivation rather than as an error.
        assert not described["no_path_possible"], f"{template.name}: no path possible"
        assert described["demand2producer"], f"{template.name}: nothing produces anything"
        n_tr = len(encoded.transforms)
        for k, producers in described["demand2producer"]:
            for i in producers:
                assert 0 <= i <= n_tr, f"{template.name}: producer {i} of {n_tr}"
        seen += 1
    # Every shipped template, not a count. The count was 4 when this was written and
    # is 10 since the scopes converged, and a template landing is not a reason for
    # this to fail. What it guards is a vacuous pass: an uncompiled library
    # discovers nothing, and a template that stops yielding a problem stops being
    # adjudicated without failing anything else.
    assert shipped >= 4, f"only {shipped} templates discovered; the library looks uncompiled"
    assert seen == shipped, f"adjudicated {seen} of {shipped} shipped templates"
