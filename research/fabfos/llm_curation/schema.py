"""The output contract, in one place, because three programs have to agree on it.

`run_panel.py` hands this to the server as a decoding constraint, the model's response is
therefore shaped by it, and `arbiter.py` reads that shape back. Written once so a change
cannot land in two of the three.

**There is no `balanced` field and no `confidence` field, deliberately.** The pilot asked
for both. `balanced` was right seven times and wrong seven times — a coin flip — and
`confidence` was highest on exactly the failures that mattered, four `unchanged` verdicts
on reactions the lane existed to fix. Nothing downstream may read either, and asking for a
field nothing may read invites the model to spend tokens on self-assessment instead of
chemistry. The arbiter recomputes balance from structures; that is the only verdict.

`maxItems` and `maxLength` are load-bearing rather than tidy, a habit taken from capella's
`extract.py`: under constrained decoding an unbounded array is an invitation to generate
until `max_tokens`, and the bill arrives either way.
"""

from __future__ import annotations

# A participant. `id` names a MetaNetX accession whose structure the bake already has;
# `smiles` supplies one directly for a stand-in the model chose. Requiring only `n` keeps
# the grammar simple -- a term with neither is caught by the arbiter as `unusable`, which
# is where every other malformed-chemistry judgement is already made.
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

# Which blocker each stand-in stands in for. Without this the rewrite is harvestable per
# REACTION and not per METABOLITE: the model emits a new equation, and nothing in it says
# that the thioester on the right replaced `MNXM1090405` rather than being unrelated. The
# crosswalk lane needs the correspondence, and `why` becomes the `basis` citation that
# `admit()` refuses a row without.
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

# `working` is first because a constrained decode emits properties in schema order, which
# makes field order the only scratchpad a non-thinking model gets. Asked for the sides
# straight away it has nowhere to do the arithmetic the prompt demands; asked to tally the
# elements first, the tally is in its context when it writes them.
SIMPLIFY = {
    "type": "object",
    "properties": {
        "working": {"type": "string", "maxLength": 400},
        "action": {"type": "string", "enum": ["rewrite", "refuse"]},
        "reason": {"type": "string", "maxLength": 160},
        "left": {"type": "array", "maxItems": 24, "items": TERM},
        "right": {"type": "array", "maxItems": 24, "items": TERM},
        "substitutions": {"type": "array", "maxItems": 12, "items": SUBSTITUTION},
    },
    "required": ["working", "action", "reason", "left", "right", "substitutions"],
    "additionalProperties": False,
}

# Direction, asked in both orientations. A call is kept only when the two answers
# DISAGREE with each other -- agreement across a swap means the model read the layout
# rather than the chemistry. See T5.
DIRECTION = {
    "type": "object",
    "properties": {
        "call": {"type": "string", "enum": ["left_to_right", "right_to_left", "unsure"]},
        "reason": {"type": "string", "maxLength": 300},
    },
    "required": ["call", "reason"],
    "additionalProperties": False,
}
