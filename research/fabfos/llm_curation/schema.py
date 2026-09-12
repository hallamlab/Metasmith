# The output contract, in one place, because three programs have to agree on it.
#
# `run_panel.py` hands this to the server as a decoding constraint, the model's response is
# therefore shaped by it, and `arbiter.py` reads that shape back. Written once so a change
# cannot land in two of the three.
#
# **There is no `balanced` field and no `confidence` field, deliberately.** The pilot asked
# for both. `balanced` was right seven times and wrong seven times — a coin flip — and
# `confidence` was highest on exactly the failures that mattered, four `unchanged` verdicts
# on reactions the lane existed to fix. Nothing downstream may read either, and asking for a
# field nothing may read invites the model to spend tokens on self-assessment instead of
# chemistry. The arbiter recomputes balance from structures; that is the only verdict.
#
# `maxItems` and `maxLength` are load-bearing rather than tidy, a habit taken from capella's
# `extract.py`: under constrained decoding an unbounded array is an invitation to generate
# until `max_tokens`, and the bill arrives either way.

from __future__ import annotations

TERM = {
    "type": "object",
    "properties": {
        "id": {"type": "string", "maxLength": 24},
        "smiles": {"type": "string", "maxLength": 400},
        "n": {"type": "number"},
        "label": {"type": "string", "maxLength": 80},
    },
    "required": ["n"],
    "additionalProperties": False,
}

SUBSTITUTION = {
    "type": "object",
    "properties": {
        "id": {"type": "string", "maxLength": 24},
        "smiles": {"type": "string", "maxLength": 400},
        "why": {"type": "string", "maxLength": 120},
    },
    "required": ["id", "smiles", "why"],
    "additionalProperties": False,
}

# A `working` scratchpad field, emitted first so the model tallies before it commits, was
# tried as r3 and measured worse on both axes -- 12.2% against r2's 15.0%, and 21 control
# regressions against zero. Field order is a real lever on a constrained decode, but here
# it bought prose and cost answers. See `ITERATION.md`.
SIMPLIFY = {
    "type": "object",
    "properties": {
        "action": {"type": "string", "enum": ["rewrite", "refuse"]},
        "reason": {"type": "string", "maxLength": 160},
        "left": {"type": "array", "maxItems": 24, "items": TERM},
        "right": {"type": "array", "maxItems": 24, "items": TERM},
        "substitutions": {"type": "array", "maxItems": 12, "items": SUBSTITUTION},
    },
    "required": ["action", "reason", "left", "right", "substitutions"],
    "additionalProperties": False,
}

DIRECTION = {
    "type": "object",
    "properties": {
        "call": {"type": "string", "enum": ["left_to_right", "right_to_left", "unsure"]},
        "reason": {"type": "string", "maxLength": 300},
    },
    "required": ["call", "reason"],
    "additionalProperties": False,
}
