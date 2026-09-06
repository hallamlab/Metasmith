from __future__ import annotations

import pytest

from metasmith.models.solver_engine import (
    SOLVER_WIRE_VERSION,
    CallEngine,
    packaged_engine_path,
    probe_engine,
)
from metasmith.models.solver_wire import decode_plan, encode_problem
from metasmith.testing.solver_bench import CORPUS, SWEEP_PROFILES
from metasmith.testing.solver_verification import (
    GeneratorDials,
    check_plan,
    generate_problem,
)


@pytest.fixture(scope="module")
def engine():
    path = packaged_engine_path()
    if path is None:
        pytest.skip("no msm_solver staged for this platform (./dev/metasmith.sh -be)")
    info = probe_engine(path)
    if info is None:
        pytest.fail(f"a binary is staged at [{path}] but failed its handshake")
    return info


def _cases():
    for name, seed, dials in CORPUS:
        yield f"corpus/{name}", seed, dials, name
    for name, dials in SWEEP_PROFILES:
        for seed in range(10):
            yield f"{name}-{seed}", seed, dials, name


def _solve(engine, problem, *, seed=42, max_iter=256, max_refine=256):
    encoded = encode_problem(
        problem.given, problem.transforms, problem.target,
        seed=seed, max_iter=max_iter, max_refine=max_refine,
        wire_version=SOLVER_WIRE_VERSION,
    )
    return decode_plan(encoded, CallEngine(engine, "solve", encoded.payload))


@pytest.mark.parametrize(
    "name,seed,dials,label", list(_cases()),
    ids=lambda x: x if isinstance(x, str) else "",
)
def test_the_engine_returns_a_sound_plan(engine, name, seed, dials, label):
    problem = generate_problem(seed, dials, name=label)
    plan = _solve(engine, problem)
    if not plan.complete:
        pytest.skip(f"{name}: the search returned no complete plan to judge")
    verdict = check_plan(problem, plan)
    assert verdict.ok, f"{name}: {verdict.violations}"


@pytest.mark.parametrize("seed", [1, 7, 99, 2**31])
def test_soundness_holds_across_seeds_not_just_the_default(engine, seed):
    problem = generate_problem(3, GeneratorDials(
        n_types=8, n_extra_transforms=6, lineage_density=0.5, n_duplicate_transforms=2))
    plan = _solve(engine, problem, seed=seed)
    if not plan.complete:
        pytest.skip("no complete plan at this seed")
    verdict = check_plan(problem, plan)
    assert verdict.ok, verdict.violations


def test_a_problem_with_no_route_is_refused_rather_than_answered(engine):
    problem = generate_problem(0, GeneratorDials(n_types=5, n_extra_transforms=0))
    problem.transforms = []
    plan = _solve(engine, problem)
    assert plan.complete is False
    assert plan.dependency_plan == []


def test_the_engine_solves_the_shipped_templates(engine):
    from metasmith.testing.solver_bench import _libraries_root
    from metasmith.testing.solver_verification import problem_of_plan

    root = _libraries_root()
    if root is None:
        pytest.skip("the standard library is not compiled — run `dev/libraries.sh -bm`")
    from metasmith.agents import Template

    # Counted against what is actually shipped rather than against a literal: the
    # library grows, and a stale literal here fails for the one reason that says
    # nothing about the plans.
    templates = list(Template.Discover(root))
    assert len(templates) >= 4, f"only {len(templates)} templates discovered"
    seen = 0
    for template in templates:
        problem = problem_of_plan(template.spec.Solve().plan, name=template.name)
        if problem is None: continue
        plan = _solve(engine, problem)
        assert plan.complete, template.name
        verdict = check_plan(problem, plan)
        assert verdict.ok, f"{template.name}: {verdict.violations}"
        seen += 1
    assert seen == len(templates), (
        f"adjudicated {seen} of {len(templates)} shipped templates -- the rest carry "
        "no solver inputs, so nothing judged the plans on them"
    )
