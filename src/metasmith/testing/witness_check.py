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
# decoys: one per clause, each derived from an accepted reply
# ---------------------------------------------------------------------------


def _first_nongiven_step(request: dict, reply: dict) -> int | None:
    for i, s in enumerate(reply["steps"]):
        if s["transform"] != request["given_index"] and s["used"]:
            return i
    return None


def _decoy_conformance(request: dict, reply: dict) -> bool:
    """Bind a slot to an endpoint that does not carry its properties."""
    i = _first_nongiven_step(request, reply)
    if i is None:
        return False
    slot, _ = reply["steps"][i]["used"][0]
    # An endpoint carrying no properties at all cannot satisfy a slot that
    # demands any, and every plan has at least one such slot.
    if not request["nodes"][slot]["props"]:
        return False
    reply["endpoints"].append({"props": [], "parents": [], "source_node": None})
    reply["steps"][i]["used"][0] = [slot, len(reply["endpoints"]) - 1]
    return True


def _decoy_provenance(request: dict, reply: dict) -> bool:
    """Consume an endpoint no step ever emitted."""
    i = _first_nongiven_step(request, reply)
    if i is None:
        return False
    slot, ep = reply["steps"][i]["used"][0]
    clone = copy.deepcopy(reply["endpoints"][ep])
    reply["endpoints"].append(clone)
    reply["steps"][i]["used"][0] = [slot, len(reply["endpoints"]) - 1]
    return True


def _decoy_schedulable(request: dict, reply: dict) -> bool:
    """Put a consumer before its producer."""
    for j, s in enumerate(reply["steps"]):
        if not s["used"]:
            continue
        ep = s["used"][0][1]
        for i, t in enumerate(reply["steps"]):
            if i < j and any(b[1] == ep for g in t["produced"] for b in g):
                reply["steps"][i], reply["steps"][j] = reply["steps"][j], reply["steps"][i]
                return True
    return False


def _decoy_boundary(request: dict, reply: dict) -> bool:
    """Delete the target step."""
    for i, s in enumerate(reply["steps"]):
        if s["transform"] == request["target_index"]:
            del reply["steps"][i]
            return True
    return False


def _decoy_rooted(request: dict, reply: dict) -> bool:
    """Strip a produced endpoint's declared lineage."""
    for s in reply["steps"]:
        if s["transform"] == request["given_index"] or not s["used"]:
            continue
        for g in s["produced"]:
            for _, ep in g:
                if reply["endpoints"][ep]["parents"]:
                    reply["endpoints"][ep]["parents"] = []
                    return True
    return False


def _decoy_givens(request: dict, reply: dict) -> bool:
    """Have the given step present an endpoint matching no declared input."""
    for s in reply["steps"]:
        if s["transform"] != request["given_index"]:
            continue
        for g in s["produced"]:
            if not g:
                continue
            _, ep = g[0]
            # A property id past the end of the interned table belongs to no
            # declared given by construction.
            reply["endpoints"][ep]["props"] = [request["n_properties"] + 1]
            return True
    return False


def _decoy_shape(request: dict, reply: dict) -> bool:
    """Drop a required slot's binding."""
    i = _first_nongiven_step(request, reply)
    if i is None or len(reply["steps"][i]["used"]) < 1:
        return False
    del reply["steps"][i]["used"][0]
    return True


def _decoy_indexed(request: dict, reply: dict) -> bool:
    """Name a slot that does not exist."""
    i = _first_nongiven_step(request, reply)
    if i is None:
        return False
    reply["steps"][i]["used"][0][0] = len(request["nodes"]) + 1000
    return True


def _decoy_nonempty(request: dict, reply: dict) -> bool:
    reply["steps"] = []
    return True


def _decoy_emission(request: dict, reply: dict) -> bool:
    """Emit an endpoint that does not carry the properties of the slot it left."""
    for s in reply["steps"]:
        if s["transform"] == request["given_index"]:
            continue
        for g in s["produced"]:
            for k, (slot, _) in enumerate(g):
                if request["nodes"][slot]["props"]:
                    reply["endpoints"].append(
                        {"props": [], "parents": [], "source_node": None})
                    g[k] = [slot, len(reply["endpoints"]) - 1]
                    return True
    return False


#: clause name -> (mutation, whether the clause must be the one that fires).
#: Several mutations legitimately trip more than one clause -- deleting the
#: target step breaks `boundary` and also orphans whatever it consumed -- so the
#: assertion is that the named clause is *among* those violated.
DECOYS: dict[str, Any] = {
    "indexed": _decoy_indexed,
    "nonempty": _decoy_nonempty,
    "shape": _decoy_shape,
    "provenance": _decoy_provenance,
    "conformance": _decoy_conformance,
    "emission": _decoy_emission,
    "rooted": _decoy_rooted,
    "givens": _decoy_givens,
    "schedulable": _decoy_schedulable,
    "boundary": _decoy_boundary,
}


def apply_decoy(clause: str, request: dict, reply: dict) -> dict | None:
    """Return a mutated copy of `reply`, or None when this case cannot host it."""
    out = copy.deepcopy(reply)
    if not DECOYS[clause](request, out):
        return None
    return out
