from __future__ import annotations

import pytest

pytestmark = pytest.mark.python_solver

from metasmith.models.solver_backend import Backend, UsePythonSolver
from metasmith.models.solver_engine import (
    SOLVER_WIRE_VERSION,
    CallEngine,
    packaged_engine_path,
    probe_engine,
)
from metasmith.models.solver_wire import decode_plan, encode_problem
from metasmith.testing.solver_bench import CORPUS
from metasmith.testing.solver_verification import (
    GeneratorDials,
    check_plan,
    generate_problem,
    plan_fingerprint,
    plan_shape,
)


@pytest.fixture(scope="module")
def engine():
    path = packaged_engine_path()
    if path is None:
        pytest.skip("no msm_solver staged for this platform (./dev.sh -be)")
    info = probe_engine(path)
    if info is None:
        pytest.fail(f"a binary is staged at [{path}] but failed its handshake")
    return info


_PROFILES = [
    ("plain", GeneratorDials(n_types=6, n_extra_transforms=3)),
    ("cyclic", GeneratorDials(n_types=7, n_extra_transforms=5, cycle_density=0.8)),
    ("lineage", GeneratorDials(
        n_types=7, n_extra_transforms=5, lineage_density=0.9, target_lineage=1.0)),
    ("dupes", GeneratorDials(n_types=7, n_extra_transforms=3, n_duplicate_transforms=5)),
    ("pgroups", GeneratorDials(n_types=7, n_extra_transforms=4, product_group_density=0.9)),
    ("multi", GeneratorDials(n_types=7, n_given=2, n_given_groups=3, n_extra_transforms=4)),
    ("tiny", GeneratorDials(n_types=4, n_extra_transforms=2, lineage_density=0.6,
                            target_lineage=0.6)),
    ("sink", GeneratorDials(
        n_types=9, n_given=2, n_given_groups=2, n_extra_transforms=6, cycle_density=0.4,
        lineage_density=0.7, n_duplicate_transforms=2, product_group_density=0.5,
        target_lineage=0.8)),
]


def _cases():
    for name, seed, dials in CORPUS:
        yield f"corpus/{name}", seed, dials, name
    for name, dials in _PROFILES:
        for seed in range(10):
            yield f"{name}-{seed}", seed, dials, name


def _both(engine, problem, *, seed=42, max_iter=256, max_refine=256):
    encoded = encode_problem(
        problem.given, problem.transforms, problem.target,
        seed=seed, max_iter=max_iter, max_refine=max_refine,
        wire_version=SOLVER_WIRE_VERSION,
    )
    theirs = decode_plan(encoded, CallEngine(engine, "solve", encoded.payload))
    with UsePythonSolver():
        assert Backend("solve") == "python", "the reference side must be python"
        mine = problem.solve(seed=seed, max_iter=max_iter, max_refine=max_refine)
    return mine, theirs


def _sequence(plan):
    return [
        (s.transform.key, sorted((d.key, e.key) for d, e in s.used.items()))
        for s in plan.dependency_plan
    ]


@pytest.mark.parametrize("name,seed,dials,label", list(_cases()), ids=lambda x: x if isinstance(x, str) else "")
def test_the_engine_finds_the_same_plan(engine, name, seed, dials, label):
    problem = generate_problem(seed, dials, name=label)
    mine, theirs = _both(engine, problem)
    assert theirs.complete == mine.complete, f"{name}: disagreed about completeness"
    assert plan_fingerprint(theirs) == plan_fingerprint(mine), f"{name}: different topology"
    assert plan_shape(theirs) == plan_shape(mine), f"{name}: same fingerprint, different shape"
    assert _sequence(theirs) == _sequence(mine), f"{name}: same plan, different order"


@pytest.mark.parametrize("name,seed,dials,label", list(_cases())[:16], ids=lambda x: x if isinstance(x, str) else "")
def test_the_engines_plan_is_sound_on_its_own_terms(engine, name, seed, dials, label):
    problem = generate_problem(seed, dials, name=label)
    _, theirs = _both(engine, problem)
    if not theirs.complete:
        pytest.skip(f"{name} has no solution for either side to be judged on")
    verdict = check_plan(problem, theirs)
    with UsePythonSolver():
        mine = problem.solve()
    assert verdict.ok == check_plan(problem, mine).ok, f"{name}: {verdict}"


@pytest.mark.parametrize("seed", [1, 7, 99, 2**31])
def test_the_two_agree_across_seeds_not_just_the_default(engine, seed):
    problem = generate_problem(3, GeneratorDials(
        n_types=8, n_extra_transforms=6, lineage_density=0.5, n_duplicate_transforms=2))
    mine, theirs = _both(engine, problem, seed=seed)
    assert plan_fingerprint(theirs) == plan_fingerprint(mine)
    assert _sequence(theirs) == _sequence(mine)


def test_a_problem_with_no_route_is_refused_the_same_way(engine):
    problem = generate_problem(0, GeneratorDials(n_types=5, n_extra_transforms=0))
    problem.transforms = []
    mine, theirs = _both(engine, problem)
    assert mine.complete is False and theirs.complete is False
    assert theirs.dependency_plan == [] and mine.dependency_plan == []


def test_the_engine_solves_the_shipped_templates(engine):
    from metasmith.testing.solver_bench import _libraries_root
    from metasmith.testing.solver_verification import problem_of_plan

    root = _libraries_root()
    if root is None:
        pytest.skip("the standard library is not compiled — run `dev/libraries.sh -bm`")
    from metasmith.agents import Template

    # Counted against what is actually shipped rather than against a literal: the
    # library grows, and a stale literal here fails for the one reason that says
    # nothing about the two implementations agreeing.
    templates = list(Template.Discover(root))
    assert len(templates) >= 4, f"only {len(templates)} templates discovered"
    seen = 0
    for template in templates:
        problem = problem_of_plan(template.spec.Solve().plan, name=template.name)
        if problem is None: continue
        mine, theirs = _both(engine, problem)
        assert theirs.complete == mine.complete, template.name
        assert plan_fingerprint(theirs) == plan_fingerprint(mine), template.name
        assert _sequence(theirs) == _sequence(mine), f"{template.name}: different order"
        seen += 1
    assert seen == len(templates), (
        f"adjudicated {seen} of {len(templates)} shipped templates -- the rest carry "
        "no solver inputs, so nothing compared the two implementations on them"
    )
