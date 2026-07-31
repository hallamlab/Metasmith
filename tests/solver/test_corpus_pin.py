"""The generated corpus, pinned to a recorded fingerprint.

This is the gate every performance change to the solver has to walk past. The
fingerprint is topological, so a change that reorders steps or reshuffles which
`Endpoint` object carries a value passes untouched; a change that alters *which
plan* comes back does not.

Updating `fingerprints.json` is therefore a deliberate act, not housekeeping.
The only change expected to move these legitimately is swapping the PRNG: a
different random stream finds a different, equally valid plan, and parity there
is argued over distributions rather than digests.

Regenerate with::

    python -m metasmith.testing.solver_bench --no-templates --pin tests/solver/fingerprints.json
"""

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
    """Some cases must move with the seed, and some must not.

    A case whose plan is the same under every seed is the stronger parity pin:
    a fingerprint change there cannot be blamed on random-stream noise. A case
    that *does* move with the seed is what makes the corpus able to notice a
    PRNG swap at all. The corpus is only useful if it holds both, so this
    asserts the spread rather than any particular case's behaviour.
    """
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
    """A case quietly dropped from the pin is a gate that stopped gating."""
    assert set(_pinned()) == {f"gen/{name}" for name, _, _ in CORPUS}
