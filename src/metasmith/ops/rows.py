"""What a recipe row holds, and how it becomes a file.

The shape of an input row is decided here and nowhere else, because both
:mod:`ops.inputs` (which writes the library from rows) and :mod:`ops.samples`
(which decides which rows fan out over a sheet) have to read it, and `inputs`
already imports `samples`. `frontend/src/lib/rows.js` is the page's copy of the
same three questions, for the same reason.

A **value row** holds an ordered list of entries, each a key and a value. One
unkeyed entry writes its text verbatim -- which is what a value row has always
been, so nothing written before the list existed moves a byte -- and anything
else writes the JSON object those pairs describe. That is the point of the list:
read metadata is several facts, and the alternative is typing JSON by hand.

Every field of a row carries **two** answers, not one: the text typed into it,
and the sheet column it binds. Which of the two is live is decided by one thing
-- whether a sample table is attached -- and never by what the text looks like.
That is the whole state machine: with no sheet a field is its text, with a sheet
it is its column, and the other answer waits rather than being overwritten, so
attaching and detaching a sheet moves between two states instead of destroying
one. A value entry's **key** is the exception, and always literal: it names the
field in the object the row writes and is never read as a column.
"""
from __future__ import annotations

import json


def scalar(v):
    """Text typed into a box, given the type it looks like.

    One rule, so it is the same everywhere: a value that reads as a JSON scalar
    becomes that scalar, anything else stays the string it was. `50` is a
    number, `--partition=x` is a string, and `"50"` is a string on purpose --
    which is the escape hatch for the one case the rule gets wrong. A list or an
    object stays its text too: structure is what the keyed entries are for, and
    there is deliberately no way to nest one inside another.

    On the server rather than in the GUI because the CLI and the notebook build
    the same libraries and must get the same answer; `gui.api._param_value` is
    the other caller.
    """
    if not isinstance(v, str): return v
    s = v.strip()
    if not s: return v
    try:
        parsed = json.loads(s)
    except ValueError:
        return v
    if isinstance(parsed, (dict, list)): return v
    return parsed


def column_of(field: dict) -> str:
    """The sheet column a field binds, or `""` for one that binds none.

    Takes a value entry or a file row -- both spell it the same way, which is
    the point: the binding is a field of a field, beside the text, rather than
    something read out of it.

    An empty answer is not a constant. With a sheet attached it is a blank in
    the recipe, the way an empty path is, and it registers nothing.
    """
    return str(field.get("column") or "").strip()


def entries(row: dict) -> list[dict]:
    """What a value row holds, as `[{key, value, column}]`.

    The one place the row's shape is decided. A row written before this held a
    single `value` string and still does; it reads as one unkeyed entry, and is
    never written back in the old spelling by anything here -- the same
    concession `name` already gets. A row written before fields had bindings
    reads as bound to nothing, which is what it is.

    Tolerant of junk, because a request body is stored verbatim and has no
    schema behind it.
    """
    raw = row.get("values")
    if not isinstance(raw, list):
        return [{"key": "", "value": row.get("value") or "", "column": ""}]
    out: list[dict] = []
    for e in raw:
        if not isinstance(e, dict):
            continue
        # keys are stripped, values are not: trailing whitespace in a value can
        # be the point, and `scalar` strips its own before parsing
        out.append({
            "key": str(e.get("key") or "").strip(),
            "value": e.get("value") or "",
            "column": column_of(e),
        })
    return out


def render_value(ents: list[dict]) -> str:
    """The file those entries describe.

    A single unkeyed entry is its own text, verbatim -- which is what a value
    row has always written, so no row from before this moves a byte. Anything
    else is the JSON object the pairs describe, in entry order, each value
    typed by :func:`scalar`.
    """
    if not ents:
        # a row emptied down to nothing writes nothing; it is a launch problem,
        # not a sync one, and refusing here would stop a half-typed recipe solving
        return ""
    if len(ents) == 1 and not ents[0]["key"]:
        return ents[0]["value"]
    return json.dumps({e["key"]: scalar(e["value"]) for e in ents})
