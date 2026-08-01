"""No plan may depend on how CPython lays out a set.

The solver iterates sets in several places where the order reaches the answer:
it decides which application is appended to the frontier first, and the
selection rules break ties by index. CPython gives that iteration in hash-table
slot order, which is a function of hash *values* and of the insertion and
resize history -- deterministic for one interpreter, and portable to nothing.
Before this axis existed, four of the eight corpus fingerprints moved under a
change of hash values alone.

The probe below is the reason this can be asserted rather than argued.
`Node.__hash__` returns `self.hash` and `Node.__eq__` compares `self.hash`, so
XORing a salt into `__hash__` -- and *only* `__hash__` -- leaves every
signature, key and equality relation exactly as it was while completely
rearranging every set and dict the solver builds. Nothing about the problem
changes; only the container layout does. A plan that moves under that is a plan
the Rust port cannot reproduce, because the port has no CPython set to imitate.

This is a hard prerequisite for the port, not a style preference. The
alternative to stating the order is reimplementing CPython's set probing
sequence in Rust and pinning it to an interpreter version.
"""

from __future__ import annotations

import pytest

@pytest.fixture(autouse=True)
def _python_solver():
    """Pin the search to the python implementation for this whole file --
    CPython's set layout is this file's whole subject; there is none to salt in
    the engine, and a green run there would mean nothing at all.
    """
    from metasmith.models.solver_backend import UsePythonSolver
    with UsePythonSolver():
        yield

from metasmith.models import solver as solver_module
from metasmith.testing.solver_bench import CORPUS
from metasmith.testing.solver_verification import (
    check_plan,
    generate_problem,
    plan_fingerprint,
)

#: Arbitrary and varied on purpose -- a single salt could be unlucky enough to
#: leave the slot order of these particular problems alone.
SALTS = [0x5BF03635, 0xDEADBEEF, 0x1, 0x7FFFFFFF]

#: `CORPUS` only. Each case is solved once per salt, and the stress instances
#: run for seconds each, which would put this file alone above two minutes on a
#: `fast` axis. They were checked out of band and hold; `kitchen-sink` is the
#: sensitive one here and it costs 15ms.
CASES = CORPUS


def _salt_hashes(monkeypatch, salt: int) -> None:
    """Rearrange every container without changing a single signature."""
    monkeypatch.setattr(
        solver_module.Node, "__hash__", lambda self: self.hash ^ salt, raising=False
    )
    monkeypatch.setattr(
        solver_module.Transform, "__hash__", lambda self: self.hash ^ salt, raising=False
    )

    def _application_hash(self):
        if self._hash is None:
            self._hash, _ = solver_module.KeyGenerator.FromStr(self.Signature())
        return self._hash ^ salt

    monkeypatch.setattr(
        solver_module.Application, "__hash__", _application_hash, raising=False
    )


@pytest.mark.parametrize("name,seed,dials", CASES, ids=[c[0] for c in CASES])
def test_the_plan_survives_a_rearranged_hash_table(name, seed, dials, monkeypatch):
    reference = plan_fingerprint(generate_problem(seed, dials, name=name).solve())
    for salt in SALTS:
        with monkeypatch.context() as m:
            _salt_hashes(m, salt)
            problem = generate_problem(seed, dials, name=name)
            solution = problem.solve()
            assert plan_fingerprint(solution) == reference, (
                f"{name} solves differently at salt 0x{salt:x}: some iteration "
                "order in the solver is still CPython's rather than stated"
            )
            # a salted run that produced an equivalent-but-broken plan would
            # slip past the fingerprint, which compares shape and not soundness
            assert check_plan(problem, solution).ok


def test_the_probe_actually_rearranges_containers(monkeypatch):
    """Guard the guard.

    Everything above is a pass if the salt happens to perturb nothing, and a
    silently inert probe is worse than no probe. So check the mechanism head
    on: the same endpoints, inserted in the same order, must come back out of a
    `set` in a *different* order once the hashes are salted -- while remaining
    equal, same-keyed and same-signatured, which is what makes the perturbation
    legitimate rather than a different problem.
    """
    endpoints = [
        solver_module.Endpoint(properties={f"type_{i}", "shared"}) for i in range(24)
    ]
    signatures = [e.Signature() for e in endpoints]
    plain = [e.key for e in set(endpoints)]

    rearranged = []
    for salt in SALTS:
        with monkeypatch.context() as m:
            _salt_hashes(m, salt)
            rearranged.append([e.key for e in set(endpoints)])
            assert [e.Signature() for e in endpoints] == signatures, (
                "the salt changed a signature, so it is perturbing the problem "
                "and not just the container -- the probe is invalid"
            )

    assert any(order != plain for order in rearranged), (
        "salting the hashes left set iteration order untouched; the probe no "
        "longer perturbs anything and the assertions above prove nothing"
    )
    assert all(sorted(order) == sorted(plain) for order in rearranged)
