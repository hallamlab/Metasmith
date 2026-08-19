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


def test_the_corpus_spans_both_sides_of_rng_sensitivity():
    stable, sensitive = [], []
    for name, seed, dials in CORPUS:
        prints = {
            plan_fingerprint(generate_problem(seed, dials).solve(seed=s))
            for s in (42, 7, 1234)
        }
        (stable if len(prints) == 1 else sensitive).append(name)
    assert stable, "no rng-stable case left -- nothing pins a change as non-noise"
    assert sensitive, "no rng-sensitive case left -- a PRNG swap would go unseen"


def test_the_pin_covers_the_whole_corpus():
    assert set(_pinned()) == {f"gen/{name}" for name, _, _ in CORPUS}
