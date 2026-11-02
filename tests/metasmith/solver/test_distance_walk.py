from __future__ import annotations

import pytest
import time
from collections import Counter

from metasmith.models.solver_engine import EngineFor
from metasmith.testing.solver_verification import GeneratorDials, generate_problem


@pytest.fixture(scope="module")
def engine():
    info = EngineFor("solve")
    if info is None:
        pytest.skip("no msm_solver advertising `solve` (./dev/metasmith.sh -bel)")
    return info

_DENSE_CYCLES = GeneratorDials(
    n_types=12, n_extra_transforms=24, cycle_density=1.0, max_requirements=3
)

_DUPLICATES = GeneratorDials(
    n_types=7, n_extra_transforms=3, n_duplicate_transforms=5, lineage_density=0.5
)


def test_a_densely_cyclic_universe_does_not_stall_the_preamble():
    problem = generate_problem(0, _DENSE_CYCLES, name="dense")
    assert len(problem.transforms) >= 30, "the shape lost its teeth"
    started = time.perf_counter()
    problem.solve(max_iter=1, max_refine=0)
    elapsed = time.perf_counter() - started
    assert elapsed < 20.0, (
        f"the preamble took {elapsed:.1f}s on {len(problem.transforms)} transforms"
        " -- something readmits a transform to the backward walk, and the cost of"
        " that is combinatorial rather than linear"
    )


def test_transforms_sharing_a_key_each_keep_their_own_distance(engine):
    from metasmith.models.solver_engine import CallEngine, SOLVER_WIRE_VERSION
    from metasmith.models.solver_wire import encode_problem

    problem = generate_problem(11, _DUPLICATES, name="dupes")
    encoded = encode_problem(
        problem.given, problem.transforms, problem.target,
        seed=42, max_iter=256, max_refine=0, wire_version=SOLVER_WIRE_VERSION,
    )
    reply = CallEngine(engine, "describe", encoded.payload)
    distances = dict(reply["distance"])
    assert distances, "the engine stopped reporting distances"

    by_key = Counter(encoded.transforms[i].key for i in distances)
    shared = sorted(k for k, n in by_key.items() if n > 1)
    assert shared, (
        "every key in the distance table is unique, so either the memo went back"
        " to keying on `Transform.key` or this problem no longer reaches two"
        " transforms that share one -- check the second before believing the"
        " first, and raise n_duplicate_transforms if so"
    )
