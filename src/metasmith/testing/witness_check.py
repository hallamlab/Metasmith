"""Driving `msm_solver check` over real solves, and over decoys built from them.

Two questions, and they are different. Does the witness accept what the shipping
solver actually produces? And does it reject a plan that is wrong, *by the clause
that plan was built to break*? A witness that only ever answers the first is a
witness nobody has tested.

The corpus cannot answer the second. Five of the conditions are unfalsifiable on
real solves -- no shipped plan uses a product slot declaring lineage, no decoded
payload names the same slot twice, and the reflexive escape in the lineage test
fires on nothing. The decoys are the only witnesses those clauses will ever get,
which is why `mutate` is not an optional extra here.

**CAUTION** Everything here is vacuous unless the Rust engine is actually the one
running. `solve_by_mcts` dispatches through `solver_backend` and never reads a
Python-side policy or checker, so on a host with no staged binary these cases
exercise the Python solver and prove nothing about the port. `require_rust`
asserts rather than warns, for that reason.
"""

from __future__ import annotations

import copy
import json
import subprocess
from dataclasses import dataclass
from typing import Any

from ..models.solver_backend import Backend
from ..models.solver_engine import CallEngine, EngineFor
from ..models.solver_wire import encode_problem
from .solver_spec import CLAUSES

__all__ = [
    "require_rust",
    "witness_check",
    "solve_and_check",
    "DECOYS",
    "apply_decoy",
]


def require_rust() -> None:
    """Refuse to run at all unless the engine is the thing under test."""
    got = Backend("solve")
    if got != "rust":
        raise AssertionError(
            f"solve backend is {got!r}, not 'rust' -- stage a binary with "
            "`src/workflow_solver/dev.sh -bl` or these results mean nothing"
        )
    info = EngineFor("check")
    if info is None:
        raise AssertionError(
            "the staged engine does not advertise the 'check' capability; "
            "it predates the witness"
        )


@dataclass
class WitnessVerdict:
    ok: bool
    complete: bool
    clauses: list[str]
    raw: dict[str, Any]

    def violated(self, clause: str) -> bool:
        return clause in self.clauses


def witness_check(request: dict, reply: dict) -> WitnessVerdict:
    """Adjudicate a `(request, reply)` pair with the staged engine."""
    info = EngineFor("check")
    assert info is not None, "no engine advertising 'check'"
    # A rejected plan exits non-zero on purpose, so a shell caller can gate on
    # it. That is not an error here -- it is the answer -- so the subprocess is
    # driven directly rather than through CallEngine, which raises on it.
    proc = subprocess.run(
        [str(info.path), "check"],
        input=json.dumps({"request": request, "reply": reply}),
        capture_output=True,
        text=True,
    )
    if proc.returncode not in (0, 2):
        raise RuntimeError(f"msm_solver check failed ({proc.returncode}): {proc.stderr}")
    out = json.loads(proc.stdout)
    return WitnessVerdict(
        ok=out["ok"],
        complete=out["complete"],
        clauses=sorted({v["clause"] for v in out["violations"]}),
        raw=out,
    )


def solve_and_check(
    problem, *, seed: int = 42, max_iter: int = 256, max_refine: int = 256
) -> tuple[dict, dict, WitnessVerdict]:
    """Solve on the engine and adjudicate the exact bytes it returned.

    Returns the request and reply as well, because the decoys are built by
    mutating a reply the witness has already accepted. Hand-writing one instead
    risks a rejection for being malformed rather than for being unsound, which
    would prove nothing about the clause under test.
    """
    from ..models.solver_engine import SOLVER_WIRE_VERSION

    encoded = encode_problem(
        [set(g) for g in problem.given],
        list(problem.transforms),
        problem.target,
        seed=seed,
        max_iter=max_iter,
        max_refine=max_refine,
        wire_version=SOLVER_WIRE_VERSION,
    )
    payload = encoded.payload
    info = EngineFor("solve")
    assert info is not None
    reply = CallEngine(info, "solve", payload)
    return payload, reply, witness_check(payload, reply)


# ---------------------------------------------------------------------------
# decoys: one per clause, each derived from a reply the checker just accepted
# ---------------------------------------------------------------------------
#
# Shared, so the engine's witness and `solver_spec`'s python reference are
# adjudicating the SAME mutated plans. Two copies of these drifted apart once
# already, and a decoy that differs between the two turns a real disagreement
# into a diff nobody can read.


def _first_plan_step(request: dict, reply: dict) -> int:
    for i, s in enumerate(reply["steps"]):
        if s["transform"] != request["given_index"] and s["used"]:
            return i
    raise AssertionError("no non-given step consumes anything")


def _d_indexed(q, r):
    r["steps"][_first_plan_step(q, r)]["used"][0][0] = len(q["nodes"]) + 1000


def _d_shape(q, r):
    del r["steps"][_first_plan_step(q, r)]["used"][0]


def _d_conformance(q, r):
    i = _first_plan_step(q, r)
    slot = r["steps"][i]["used"][0][0]
    r["endpoints"].append({"props": [], "parents": [], "source_node": None})
    r["steps"][i]["used"][0] = [slot, len(r["endpoints"]) - 1]


def _d_emission(q, r):
    i = _first_plan_step(q, r)
    for group in r["steps"][i]["produced"]:
        for k, (slot, _) in enumerate(group):
            if q["nodes"][slot]["props"]:
                r["endpoints"].append({"props": [], "parents": [], "source_node": None})
                group[k] = [slot, len(r["endpoints"]) - 1]
                return


def _d_derived(q, r):
    # One extra parent the step did not confer. It has to be an index the
    # endpoint does not already carry, and below the endpoint's own -- a higher
    # one would trip `indexed` instead and prove nothing about this clause.
    for i, s in enumerate(r["steps"]):
        if s["transform"] == q["given_index"]:
            continue
        for group in s["produced"]:
            for _, e in group:
                carried = set(r["endpoints"][e]["parents"])
                spare = next((x for x in range(e) if x not in carried), None)
                if spare is None:
                    continue
                r["endpoints"][e]["parents"] = sorted(carried | {spare})
                return


def _d_uniqueProducer(q, r):
    i = _first_plan_step(q, r)
    _, e = r["steps"][i]["produced"][0][0]
    for j, s in enumerate(r["steps"]):
        if j != i and s["transform"] != q["given_index"] and s["produced"] and s["produced"][0]:
            s["produced"][0][0] = [s["produced"][0][0][0], e]
            return


def _d_provenance(q, r):
    i = _first_plan_step(q, r)
    slot, e = r["steps"][i]["used"][0]
    r["endpoints"].append(copy.deepcopy(r["endpoints"][e]))
    r["steps"][i]["used"][0] = [slot, len(r["endpoints"]) - 1]


def _d_givens(q, r):
    # A property id that exists but that this given does not carry. Reaching past
    # the interned table instead would trip `indexed` and prove nothing here.
    for s in r["steps"]:
        if s["transform"] != q["given_index"]:
            continue
        _, e = s["produced"][0][0]
        carried = set(r["endpoints"][e]["props"])
        spare = next(x for x in range(q["n_properties"]) if x not in carried)
        r["endpoints"][e]["props"] = sorted(carried | {spare})
        return


def _d_schedulable(q, r):
    plan = [i for i, s in enumerate(r["steps"]) if s["transform"] != q["given_index"]]
    for j in plan:
        for _, e in r["steps"][j]["used"]:
            for i in plan:
                if i < j and any(b == e for g in r["steps"][i]["produced"] for _, b in g):
                    r["steps"][i], r["steps"][j] = r["steps"][j], r["steps"][i]
                    return


def _d_target(q, r):
    for i, s in enumerate(r["steps"]):
        if s["transform"] == q["target_index"]:
            del r["steps"][i]
            return


#: clause name -> the mutation. Several mutations legitimately trip more than one
#: clause, so a test asserts the named clause is *among* those violated.
DECOYS: dict[str, Any] = {c: globals()[f"_d_{c}"] for c in CLAUSES}


def apply_decoy(clause: str, request: dict, reply: dict) -> tuple[dict, dict] | None:
    """Return mutated copies, or None when this case cannot host the decoy."""
    q, r = copy.deepcopy(request), copy.deepcopy(reply)
    try:
        DECOYS[clause](q, r)
    except (AssertionError, IndexError, StopIteration):
        return None
    if q == request and r == reply:
        return None
    return q, r
