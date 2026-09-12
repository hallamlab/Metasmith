from __future__ import annotations

import json
from pathlib import Path

import pytest

from metasmith.testing.solver_bench import CORPUS
from metasmith.testing.solver_verification import (
    check_plan,
    generate_problem,
    plan_fingerprint,
)

_PIN = Path(__file__).parent / "fingerprints.json"


def _pinned() -> dict[str, dict]:
    return json.loads(_PIN.read_text(encoding="utf-8"))["cases"]


@pytest.mark.parametrize("name,seed,dials", CORPUS, ids=[c[0] for c in CORPUS])
def test_corpus_case_still_yields_the_recorded_plan(name, seed, dials):
    pinned = _pinned().get(f"gen/{name}")
    assert pinned is not None, (
        f"{name} is in CORPUS but not in fingerprints.json -- regenerate the pin"
    )
    problem = generate_problem(seed, dials, name=name)
    solution = problem.solve()
    verdict = check_plan(problem, solution)
    assert verdict.ok, f"[{name}] {verdict.violations}"
    assert plan_fingerprint(solution) == pinned["fingerprint"], (
        f"[{name}] the solver now returns a different plan. If that is intended, "
        "regenerate tests/solver/fingerprints.json and say so in the commit."
    )


def test_the_solve_seed_no_longer_reaches_the_plan():
    """PUCT ranks; it does not sample, so the seed is inert on every corpus case.

    This test used to demand the corpus keep at least one rng-*sensitive* case, so
    that a PRNG swap could not pass unnoticed. That premise died with the weighted
    rule: selection's only remaining draw is `bounded_int` over the top-k set, k is
    1, and a one-element choice consumes nothing. A swapped PRNG is caught by
    `test_rng_contract.py`, which drives the stream directly through `rng-trace`
    rather than hoping a plan happens to depend on it.

    What is worth pinning instead is the inertness itself: a change that puts
    sampling back on the selection path makes every plan seed-dependent again, and
    that should be a decision rather than a discovery.
    """
    for name, seed, dials in CORPUS:
        prints = {
            plan_fingerprint(generate_problem(seed, dials).solve(seed=s))
            for s in (42, 7, 1234)
        }
        assert len(prints) == 1, (
            f"{name} moved with the solve seed -- something reintroduced a draw"
            " into node selection"
        )


def test_the_pin_covers_the_whole_corpus():
    assert set(_pinned()) == {f"gen/{name}" for name, _, _ in CORPUS}
