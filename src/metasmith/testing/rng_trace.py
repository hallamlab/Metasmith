"""The differential harness for the decision contract.

Two implementations of `solver_rng` agree if they make the same decisions in the
same order off the same stream. Comparing raw words would not show that: the
words could match perfectly while the two sides disagreed about how a weight
becomes an index, and the rules could match perfectly while one side consumed a
word the other did not. So a trace is a *script of decisions*, replayed by both
sides against one stream, and every result carries the running draw count.

That counter is the point. A rule that returns the right answer while consuming
the wrong number of words is correct exactly once, and wrong forever after --
the two streams have drifted and every later decision is independent noise. The
counter catches that on the op where it happens rather than on the op where it
finally changes an answer.

The scripts are generated, not written, and the generator draws from its own
stream so that adding a case never perturbs the traced one. It leans hard on the
cases where a "reasonable" implementation diverges: ties (the first-extremum
rule), NaN (worst in both directions), and degenerate choices (which must
consume *nothing*).
"""

from __future__ import annotations

from math import inf, isinf, isnan
from typing import Any, Sequence

from ..models.solver_engine import CallEngine, EngineInfo
from ..models.solver_rng import (
    SOLVER_RNG_VERSION,
    ChaCha8,
    DecisionStream,
    argmax_index,
    argmin_index,
    top_k_indices,
)
from ..models.solver_engine import SOLVER_WIRE_VERSION

# JSON has no NaN and no infinities; python's `json` invents a spelling for them
# and `serde_json` rejects it. Since NaN *ranking* is part of the contract, the
# wire cannot quietly not carry NaN -- so these three get names, on both sides.
_NAMES = {"nan": float("nan"), "inf": inf, "-inf": -inf}

def encode_scalar(v: float) -> float|str:
    if isnan(v): return "nan"
    if isinf(v): return "inf" if v > 0 else "-inf"
    return v

def decode_scalar(v: float|str) -> float:
    return _NAMES[v] if isinstance(v, str) else float(v)

def encode_scalars(vs: Sequence[float]) -> list[float|str]:
    return [encode_scalar(v) for v in vs]

def execute_ops(seed: int, ops: Sequence[dict]) -> dict:
    """Replay a script on the python side, in the reply shape the engine uses."""
    stream = DecisionStream(seed)
    # `raw_words` is diagnostic only: it exists so a differential failure can be
    # attributed to the stream rather than to a decision rule. `DecisionStream`
    # deliberately exposes no such thing, so it is reached through the private
    # word source here and nowhere else.
    def raw_words(n: int) -> list[int]:
        out = []
        for _ in range(n):
            out.append(stream._words.next_u32())
            stream.draws += 1
        return out

    results = []
    for op in ops:
        kind = op["op"]
        if kind == "raw_words":
            value: Any = raw_words(op["n"])
        elif kind == "bounded_int":
            value = stream.bounded_int(op["n"])
        elif kind == "weighted_index":
            value = stream.weighted_index(op["weights"])
        elif kind == "pick_top_k":
            value = stream.pick_top_k([decode_scalar(s) for s in op["scores"]], op["k"])
        elif kind == "top_k":
            value = top_k_indices([decode_scalar(s) for s in op["scores"]], op["k"])
        elif kind == "argmax":
            value = argmax_index([decode_scalar(s) for s in op["scores"]])
        elif kind == "argmin":
            value = argmin_index([decode_scalar(s) for s in op["values"]])
        else:
            raise ValueError(f"unknown op [{kind}]")
        results.append({"value": value, "draws": stream.draws})
    return {
        "wire_version": SOLVER_WIRE_VERSION,
        "rng_version": SOLVER_RNG_VERSION,
        "results": results,
        "draws": stream.draws,
    }

def execute_ops_via_engine(info: EngineInfo, seed: int, ops: Sequence[dict]) -> dict:
    """Replay the same script through the binary."""
    return CallEngine(info, "rng-trace", {
        "wire_version": SOLVER_WIRE_VERSION,
        "seed": seed,
        "ops": list(ops),
    })

def generate_ops(seed: int, n: int) -> list[dict]:
    """A reproducible script of `n` decisions, weighted toward the hard cases."""
    g = ChaCha8(seed ^ 0xA5A5A5A5)
    def draw(bound: int) -> int:
        return g.next_u32() % bound

    def scores(length: int) -> list[float|str]:
        out: list[float|str] = []
        for _ in range(length):
            r = draw(11)
            if r == 0: out.append("nan")           # the ranking rule
            elif r == 1: out.append("inf")
            elif r == 2: out.append("-inf")
            # -0.0 is here because it is the one tie python's sort calls equal
            # and a Rust `total_cmp` does not; a list holding both spellings of
            # zero is the whole test.
            elif r == 3: out.append(-0.0)
            elif r < 8: out.append(float(draw(4))) # a small range, so ties are common
            else: out.append(draw(1_000_000)/1000.0)
        return out

    ops: list[dict] = []
    for _ in range(n):
        kind = draw(7)
        if kind == 0:
            ops.append({"op": "raw_words", "n": 1 + draw(20)})
        elif kind == 1:
            # 0 and 1 are in range on purpose: they must consume no word at all.
            ops.append({"op": "bounded_int", "n": draw(64)})
        elif kind == 2:
            k = 1 + draw(6)
            ops.append({"op": "weighted_index", "weights": [1 + draw(9) for _ in range(k)]})
        elif kind == 3:
            length = 1 + draw(12)
            ops.append({"op": "pick_top_k", "scores": scores(length), "k": 1 + draw(length)})
        elif kind == 4:
            length = 1 + draw(12)
            ops.append({"op": "top_k", "scores": scores(length), "k": draw(length + 2)})
        elif kind == 5:
            ops.append({"op": "argmax", "scores": scores(1 + draw(12))})
        else:
            ops.append({"op": "argmin", "values": scores(1 + draw(12))})
    return ops
