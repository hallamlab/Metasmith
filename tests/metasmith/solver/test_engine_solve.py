"""The engine finds the same plan the solver finds.

The gate the port exists to pass. Both sides read the same problem, draw from the
same stream, and must land on the same plan -- not an equally good one, *the*
same one. Anything weaker would leave a whole class of divergence invisible: a
port that reproduces the search's *distribution* while making different decisions
is a port nobody can debug.

Two comparisons, and they answer different questions.

`plan_fingerprint` is the shipped criterion -- topological equivalence, which is
what the plan file promised and what actually matters to a caller. But it is a
canonical form, so it can hide a real disagreement behind a symmetry.
`plan_shape` is compared alongside it, and the step order is compared
separately, because two plans can be topologically equivalent and still be
different sequences.

The `describe` gate in `test_engine_problem.py` runs first for a reason: if the
problem was read differently, everything here fails and none of it says why.

**Opt-in** (`--python-solver`). Every test here goes through `_both`, which
forces a python solve to have something to compare against, and the python
solver is on its way out. What still holds the engine to account without it is
`check_plan`, which shares no code with either implementation -- see
`test_known_unsound.py` and the corpus pin.
"""

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
    # The reference is forced onto the python path. Without this the engine
    # would be compared against itself the moment it advertises `solve`, and
    # every assertion below would pass for the wrong reason.
    with UsePythonSolver():
        assert Backend("solve") == "python", "the reference side must be python"
        mine = problem.solve(seed=seed, max_iter=max_iter, max_refine=max_refine)
    return mine, theirs


def _sequence(plan):
    """The plan as an ordered list of (transform, sorted input slots).

    Coarser than a fingerprint on purpose -- it is about *order*, which the
    fingerprint's canonical form deliberately forgets.
    """
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
    """The checker adjudicates the engine's plan directly.

    Agreeing with the Python solver is the point of the test above, and it is
    also its blind spot: two implementations can agree on a plan that is wrong.
    The semantic checker shares no code with either of them.
    """
    problem = generate_problem(seed, dials, name=label)
    _, theirs = _both(engine, problem)
    if not theirs.complete:
        pytest.skip(f"{name} has no solution for either side to be judged on")
    verdict = check_plan(problem, theirs)
    # Some generated problems are unsound under *both* implementations -- the
    # refiner/rectify laundering, pinned in `test_known_unsound.py`. The claim
    # here is parity of verdict, not soundness the Python solver does not have.
    with UsePythonSolver():
        mine = problem.solve()
    assert verdict.ok == check_plan(problem, mine).ok, f"{name}: {verdict}"


@pytest.mark.parametrize("seed", [1, 7, 99, 2**31])
def test_the_two_agree_across_seeds_not_just_the_default(engine, seed):
    """A single seed proves the two implementations agree once.

    The stream is where a port diverges quietly -- a rule that consumes one word
    too many is right until it is not -- so the same problem is re-solved under
    seeds that send the search down entirely different paths.
    """
    problem = generate_problem(3, GeneratorDials(
        n_types=8, n_extra_transforms=6, lineage_density=0.5, n_duplicate_transforms=2))
    mine, theirs = _both(engine, problem, seed=seed)
    assert plan_fingerprint(theirs) == plan_fingerprint(mine)
    assert _sequence(theirs) == _sequence(mine)


def test_a_problem_with_no_route_is_refused_the_same_way(engine):
    """The bail-out path, which returns before the search starts."""
    problem = generate_problem(0, GeneratorDials(n_types=5, n_extra_transforms=0))
    problem.transforms = []  # nothing can produce the target's inputs
    mine, theirs = _both(engine, problem)
    assert mine.complete is False and theirs.complete is False
    assert theirs.dependency_plan == [] and mine.dependency_plan == []


def test_the_engine_solves_the_shipped_templates(engine):
    """The four real libraries, which is what the port is for.

    Generated problems are small and their refiners barely run. A template is
    where the search is actually expensive -- `metagenomics_from_paired_reads` is
    29 steps and puts 19,683 states through the refiner's validity check -- and
    it is also where the property table is large enough for the type bitsets to
    stride.
    """
    from metasmith.testing.solver_bench import _libraries_root
    from metasmith.testing.solver_verification import problem_of_plan

    root = _libraries_root()
    if root is None:
        pytest.skip("the standard library is not compiled — run `dev/libraries.sh -b`")
    from metasmith.agents import Template

    seen = 0
    for template in Template.Discover(root):
        problem = problem_of_plan(template.spec.Solve().plan, name=template.name)
        if problem is None: continue
        mine, theirs = _both(engine, problem)
        assert theirs.complete == mine.complete, template.name
        assert plan_fingerprint(theirs) == plan_fingerprint(mine), template.name
        assert _sequence(theirs) == _sequence(mine), f"{template.name}: different order"
        seen += 1
    assert seen == 4, f"expected the four shipped templates, adjudicated {seen}"
