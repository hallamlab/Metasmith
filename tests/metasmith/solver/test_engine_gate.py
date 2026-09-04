"""The gate that takes the search out of the trusted computing base.

`msm_solver solve` adjudicates the bytes it is about to emit against the
specification, using code that shares nothing with the search, and refuses to
hand back a plan that fails. These pin the two halves of that contract: what it
refuses, and what it must NOT refuse.

The gate condition is `complete → sound`, never `sound`. A search whose frontier
runs out returns a non-empty plan with no target step, which is an honest "no
answer" rather than a wrong one. A gate that demanded every reply pass would turn
that into a hard error while looking like it had found something.
"""

from __future__ import annotations

import json
import subprocess

import pytest

from metasmith.models.solver_backend import Backend
from metasmith.models.solver_engine import SOLVER_WIRE_VERSION, CallEngine, EngineFor
from metasmith.models.solver_wire import encode_problem
from metasmith.testing.solver_bench import CORPUS
from metasmith.testing.solver_spec import check_spec
from metasmith.testing.witness_check import apply_decoy
from metasmith.testing.solver_verification import generate_problem

_staged = EngineFor("check") is not None
needs_engine = pytest.mark.skipif(
    not _staged, reason="no staged msm_solver advertising the 'check' capability"
)


def _run_check(request: dict, reply: dict) -> subprocess.CompletedProcess:
    info = EngineFor("check")
    assert info is not None
    return subprocess.run(
        [str(info.path), "check"],
        input=json.dumps({"request": request, "reply": reply}),
        capture_output=True,
        text=True,
    )


def _solve(name, seed, dials):
    problem = generate_problem(seed, dials, name=name)
    encoded = encode_problem(
        [set(g) for g in problem.given], list(problem.transforms), problem.target,
        seed=42, max_iter=256, max_refine=256, wire_version=SOLVER_WIRE_VERSION,
    )
    return encoded.payload, CallEngine(EngineFor("solve"), "solve", encoded.payload)


@needs_engine
def test_an_accepted_plan_exits_zero_and_a_rejected_one_exits_two():
    """The exit code is the contract for a shell caller, so it is worth pinning.

    A rejected plan exiting non-zero is not an error -- it is the answer -- which
    is why the python harness treats 2 as a verdict rather than a crash.
    """
    assert Backend("solve") == "rust"
    name, seed, dials = CORPUS[0]
    request, reply = _solve(name, seed, dials)
    assert reply["complete"]

    good = _run_check(request, reply)
    assert good.returncode == 0, good.stderr
    assert json.loads(good.stdout)["ok"] is True

    bad = apply_decoy("provenance", request, reply)
    assert bad is not None, "the case could not host the decoy"
    rejected = _run_check(*bad)
    assert rejected.returncode == 2, (
        f"a rejected plan must exit 2, got {rejected.returncode}: {rejected.stderr}"
    )
    verdict = json.loads(rejected.stdout)
    assert verdict["ok"] is False
    assert "provenance" in {v["clause"] for v in verdict["violations"]}


@needs_engine
def test_the_gate_refuses_to_emit_a_plan_it_cannot_adjudicate():
    """`solve` and `check` must agree, or the gate is not the checker.

    Every plan the engine hands back has already passed its own gate, so the
    independent `check` invocation on the same bytes has to accept it too. A
    disagreement here means the gate is running something other than the witness.
    """
    assert Backend("solve") == "rust"
    for name, seed, dials in CORPUS:
        request, reply = _solve(name, seed, dials)
        if not reply["complete"]:
            continue
        result = _run_check(request, reply)
        assert result.returncode == 0, f"{name}: {result.stderr}"
        assert check_spec(request, reply).ok, name


@needs_engine
def test_an_incomplete_reply_is_returned_rather_than_refused():
    """The half of the contract that is easy to break while looking correct.

    A gate asserting `sound` instead of `complete -> sound` passes every test
    about wrong plans and silently converts "no plan found" into a crash.
    """
    assert Backend("solve") == "rust"
    from metasmith.models.solver import Endpoint, Transform

    # Nothing produces `{unreachable}`, so the search runs out of frontier.
    stuck = Transform()
    stuck.AddRequirement(properties={"unreachable"})
    stuck.AddProduct(properties={"out"})
    target = Transform()
    target.AddRequirement(properties={"out"})

    encoded = encode_problem(
        [{Endpoint(properties={"seed"})}], [stuck], target,
        seed=42, max_iter=256, max_refine=256, wire_version=SOLVER_WIRE_VERSION,
    )
    reply = CallEngine(EngineFor("solve"), "solve", encoded.payload)
    assert reply["complete"] is False, "the fixture is solvable and pins nothing"
