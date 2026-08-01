"""The input library, built from the recipe's rows.

An input row is a durable thing: it lives in the workflow's request, it is
edited there, and it never becomes something else. The *library* is what gets
built from it -- at solve time, every solve, by :func:`sync` -- which is the
same relationship a sample-array row has always had with the items it stands
for. There is one writer of an input library and this is it.

Two functions carry that:

* :func:`sync` makes the library exactly what the rows say, moving as little as
  possible. That is not tidiness: identity here is a function of path, a task
  key is a function of identity, and a rebuild would silently re-mint every id
  on every solve and cost every user their cache. So an unchanged row calls
  nothing at all, and a library nothing changed in is not even saved.

* :func:`adopt` goes the other way, once: a workflow whose library predates all
  of this (an old project, a copy of a template, an import) has items and no
  rows, and gets one row per item so it can be edited like any other.

Which row owns which manifest entry cannot be re-derived -- a deferred path is
minted, not chosen -- so it is recorded beside the library, in the same
server-owned file the sheet's expansion has always used. Server-owned for the
same reason: the browser rewrites the request wholesale on nearly every edit.

A **value row states no path at all**: the library names its own file, with a
minted uuid. The name the user used to have to type only ever named a file
nobody opens, and typing one had a cost nothing on the page explained -- a
rename re-points the item, which re-mints its identity and silently loses its
cache. Minting is subject to the same rule as a deferred path, for the same
reason: once, then recorded, never re-derived.

What a value row *holds* is a list of entries, each a key and a value, and
:func:`render_value` is the only place that list becomes a file. One unkeyed
entry writes its text verbatim -- which is what a value row has always been, so
nothing written before this moves -- and anything else writes the JSON object
those pairs describe, with each value given the type it looks like. That is the
point of the list: read metadata is several facts, and the alternative to boxes
is typing JSON by hand.

Whether a row is registered as it stands or fanned out over a sheet is decided
by one thing and it is not the row: a sample table attached makes every row an
array row, and its fields read the columns they are bound to. See `ops.samples`.

For an **array** value row the mint is per (row x grouping key), where the key
is the sheet cells that row's entries bind -- NOT per sheet row. Two sheet rows
naming one pangenome are two samples of *one* pangenome, and that shared parent
is how multiplicity is expressed here; a uuid per sheet row would quietly turn
it into three pangenomes with one genome each.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path

from ..models.libraries import DataInstanceLibrary
from ..models.paths import DEFERRED, is_deferred
from . import data as op_data
from . import samples as op_samples
from ._common import load_data_lib
# What a row holds, and what it renders to. In their own module because
# `ops.samples` reads the same shape and this module already imports it.
from .rows import column_of, entries, render_value, scalar  # noqa: F401  (re-exported)


# A value row is something typed into a box -- a read-pair descriptor, a few
# lines of metadata. This bound is far above any of those and well below
# anything that would make a payload unpasteable; a library-owned file bigger
# than this is not a value, and a row that points at one says so with a path.
MAX_VALUE_BYTES = 64 * 1024


def read_value(library_path: str | Path, item_path: str | Path | None) -> str | None:
    """The contents of a library-owned row, or `None` if it is not one."""
    if not item_path or Path(item_path).is_absolute():
        return None
    f = Path(library_path) / item_path
    if not f.is_file() or f.stat().st_size > MAX_VALUE_BYTES:
        return None
    try:
        return f.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None


def immediate_parents(lib: DataInstanceLibrary, path: Path) -> list[Path]:
    """An item's declared parents, not its ancestors.

    `Load` expands a stated link into the full closure and `Pack` collapses it
    again, so what a loaded library reports is every ancestor. Compared raw
    against what a row declares, any row with a grandparent looks changed
    forever -- and written back raw, a link the user removed comes straight
    back. This is the same collapse `Pack` does, in one place, so the two agree.
    """
    direct = [pm.path for pm in lib.parents.get(path, [])]
    inherited = {gp.path for d in direct for gp in lib.parents.get(d, [])}
    return [d for d in direct if d not in inherited]


# -- rows --------------------------------------------------------------------
#
# A row's id is what everything else refers to it by, `#`-prefixed, because a
# library path never starts with one. Lineage is stated in row ids and not in
# paths: a row may not have a path yet, and two rows with no path yet would
# otherwise be the same string.


def row_key(row: dict) -> str:
    return f"#{row.get('id')}"


def parent_ids(row: dict) -> list[str]:
    return [str(p)[1:] for p in (row.get("parents") or []) if str(p).startswith("#")]


def literal_parents(row: dict) -> list[str]:
    return [str(p) for p in (row.get("parents") or []) if not str(p).startswith("#")]


def mint_value_path() -> str:
    """A fresh library-relative name for a value nobody has to type.

    Minted once, at add time, and recorded beside the library thereafter --
    never re-derived, for the reason ``mint_deferred_path`` gives: identity is a
    function of path, so regenerating would hand the same workflow a different
    task key every time it was opened.

    Deliberately carries no type prefix and no extension. A retype is an
    in-place manifest edit, and folding the type into the path would turn every
    retype into a re-mint.
    """
    return uuid.uuid4().hex


def row_identity(row: dict) -> str:
    """What the row says it is: a path, or -- for a value row written before the
    library minted its own -- the name the user typed.

    A value row states no path now; it is recorded. This is only how a row from
    before that is matched to the item it already owns.
    """
    if row.get("mode") == "value":
        return (row.get("name") or "").strip()
    return (row.get("path") or "").strip()


def registerable(row: dict) -> bool:
    """Whether this row has enough on it to be an input: a type.

    Neither mode states a path. A *file* row with none lands as a deferred
    input -- "I will have one of these" is a complete statement about a plan,
    and the refusal belongs at stage. A value row's path is the library's to
    mint, so it never had to say one either; requiring a non-empty *value*
    instead would be worse than useless, since blanking the box would drop the
    row out of `want`, delete its item, and re-mint the identity on retype.
    """
    return bool((row.get("dtype") or "").strip())


def problems(rows: list[dict], table: dict | None = None) -> list[str]:
    """The blanks left in this recipe, in words, or nothing.

    Not a sync guard and deliberately not called by one: solving a half-filled
    recipe is the normal way to work out what a plan needs, and every blank here
    is something the library holds a perfectly good deferred or empty entry for.
    It is the *run* that cannot mean anything -- a deferred input has no file to
    stage and an unkeyed pair has no name to be read under -- so this is read at
    launch, off the solve that produced the bundle.

    Takes the sheet because what counts as a blank depends on it: with one
    attached the fields hold columns and an empty *box* is not a blank at all,
    while an unbound field is. Keys are literal in both states, so what is
    checked about them does not move.
    """
    out: list[str] = []
    if table is not None:
        out += [p["message"] for p in op_samples.unbound_problems(rows)]
    for r in rows:
        if not isinstance(r, dict) or not registerable(r):
            continue
        label = op_samples.row_label(r)
        if r.get("mode") != "value":
            if table is None and not (r.get("path") or "").strip():
                out.append(f"[{(r.get('dtype') or '').strip()}] has no path")
            continue
        ents = entries(r)
        if not ents:
            out.append(f"[{label}] has no values")
            continue
        seen: set[str] = set()
        for i, e in enumerate(ents):
            if table is None and not e["value"].strip():
                if e["key"]:
                    out.append(f"[{label}] has nothing under [{e['key']}]")
                elif len(ents) == 1:
                    out.append(f"[{label}] has nothing in it")
                else:
                    out.append(f"[{label}] has nothing in field {i + 1}")
            if len(ents) == 1:
                # one entry needs no key: unkeyed is what a plain typed value is
                continue
            if not e["key"]:
                out.append(f"[{label}] has {len(ents)} fields, and field {i + 1} has no key")
            elif e["key"] in seen:
                out.append(f"[{label}] uses the key [{e['key']}] twice")
            seen.add(e["key"])
    return list(dict.fromkeys(out))


def assert_acyclic(rows: list[dict]):
    by_id = {str(r["id"]): r for r in rows}
    done: set[str] = set()

    def visit(rid: str, stack: tuple[str, ...]):
        if rid in done:
            return
        assert rid not in stack, "these rows descend from each other in a loop"
        for p in parent_ids(by_id[rid]):
            if p in by_id:
                visit(p, stack + (rid,))
        done.add(rid)

    for rid in by_id:
        visit(rid, ())


# -- what the rows want the library to hold ----------------------------------


def _desired(rows: list[dict]) -> dict[str, dict]:
    """Every registerable plain row, as `{rid: {path, dtype, mode, value}}`.

    `path` is `None` for a file row with nothing in it yet -- which means
    deferred, and is deliberately not a minted path: whether that row keeps the
    one it already has or gets a fresh one is a question about the library, not
    about the row.
    """
    out: dict[str, dict] = {}
    for r in rows:
        if not registerable(r):
            continue
        rid = str(r["id"])
        dtype = (r.get("dtype") or "").strip()
        if r.get("mode") == "value":
            out[rid] = {
                "rid": rid, "dtype": dtype, "mode": "value",
                # The library owns a value's path and mints it. The row states
                # none -- the same thing a deferred file row says, and for the
                # same reason: whether this keeps the path it has or gets a
                # fresh one is a question about the library, not about the row.
                "path": None,
                # ...except for a row written when a value was named by hand,
                # which still carries that name. Believed once, by `_claim`.
                "legacy": (r.get("name") or "").strip() or None,
                "value": render_value(entries(r)),
            }
        else:
            p = (r.get("path") or "").strip()
            out[rid] = {
                "rid": rid, "dtype": dtype, "mode": "file",
                "path": Path(p) if p else None, "value": None,
            }
    return out


def _claim(lib, want: dict[str, dict], prior: dict[str, str]) -> dict[str, Path]:
    """Which manifest entry each row already owns.

    The record says, and is believed first. But the record is written *after*
    the library is saved, so a torn write leaves a row registered and unrecorded
    -- and treating that as new would assert `already added` on its own path. So
    a row with no record entry is matched against the manifest by the path it
    asks for before anything else happens to it.
    """
    held: dict[str, Path] = {}
    taken: set[str] = set()
    for rid in want:
        p = prior.get(rid)
        if p is not None and Path(p) in lib.manifest and p not in taken:
            held[rid] = Path(p)
            taken.add(p)
    spoken_for = {v for k, v in prior.items() if k in want} | taken
    for rid, spec in want.items():
        if rid in held:
            continue
        # A value row states no path, so the only thing it can be matched by is
        # the name it was written with before the library minted its own. That
        # binding happens once here; from then on the record answers.
        want_path = spec["path"] or (
            Path(spec["legacy"]) if spec.get("legacy") else None
        )
        if want_path is None:
            continue
        p = str(want_path)
        if Path(p) in lib.manifest and p not in spoken_for:
            held[rid] = want_path
            taken.add(p)
    return held


# -- the sheet ---------------------------------------------------------------


def _group_key(row: dict, record: dict) -> str:
    """The sheet cells an array value row's entries bind.

    This is what its minted path is keyed on, and the choice is load-bearing.
    Keying per *sheet row* would give two rows naming one pangenome two separate
    pangenome instances, turning a shared parent -- which is the entire way
    multiplicity is expressed here -- into a fan-out of one.

    Over the *union* of what every entry binds, so that binding a second field
    is what says two sheet rows are no longer the same thing. The encoding is
    unchanged and must stay so: this string is a key in the record's `minted`
    map, and a row with one entry has to produce the byte-identical key it
    always did or every existing project re-mints its array items.
    """
    cols = sorted({e["column"] for e in entries(row) if e["column"]})
    return json.dumps([[c, record.get(c)] for c in cols], separators=(",", ":"))


def _array_plan(
    table: dict, array_rows: list[dict], prior_minted: dict | None = None,
) -> list[dict]:
    """One entry per (array row x sheet row), parents already resolved.

    A parent that is another array row resolves to that row's own item on this
    sheet row -- which is what makes one declared DAG instance once per sample.
    A parent that is a plain row is left as its `#id`: plain rows are registered
    first and their paths are not known until they are.

    Two entries landing on the same path is a deliberate grouping, not a
    collision -- for a value row it is the *only* way to say "these samples share
    one of these" -- so the second one contributes only its lineage.
    """
    if not array_rows:
        return [], {}, {}
    order = op_samples.order_array_rows(array_rows)
    by_id = {str(t["id"]): t for t in array_rows}
    plan: list[dict] = []
    seen: dict[str, dict] = {}
    # what each array row stands for, counted per sheet row rather than per
    # distinct path: "x 3 registered" is a statement about the sheet, and two
    # sheet rows sharing one instance are still two sheet rows
    generated: dict[str, list[str]] = {str(t["id"]): [] for t in array_rows}
    prior_minted = prior_minted or {}
    # Rebuilt each call and carried forward only on a hit, so a key the sheet no
    # longer produces drops out rather than growing the record without bound --
    # and never resurrects a path whose file `Remove` left on disk.
    minted: dict[str, dict[str, str]] = {str(t["id"]): {} for t in array_rows}
    for record in table.get("rows") or []:
        here: dict[str, str] = {}
        for row in order:
            rid = str(row["id"])
            is_value = row.get("mode") == "value"
            if is_value:
                gkey = _group_key(row, record)
                path = minted[rid].get(gkey) or prior_minted.get(rid, {}).get(gkey)
                if path is None:
                    path = mint_value_path()
                minted[rid][gkey] = path
            else:
                # the cell itself, not something built out of it -- the sheet
                # holds finished values
                path = (record.get(column_of(row)) or "").strip()
            parents = []
            for p in parent_ids(row):
                if p in by_id:
                    if p in here:
                        parents.append(here[p])
                else:
                    parents.append(f"#{p}")
            parents += literal_parents(row)
            here[rid] = path
            generated[rid].append(path)
            if path in seen:
                for p in parents:
                    if p not in seen[path]["parents"]:
                        seen[path]["parents"].append(p)
                continue
            entry = {
                "rid": rid, "path": path, "dtype": (row.get("dtype") or "").strip(),
                "mode": "value" if is_value else "file",
                "value": render_value([
                    {"key": e["key"], "value": (record.get(e["column"]) or "").strip()}
                    for e in entries(row)
                ]) if is_value else None,
                "parents": list(parents),
            }
            seen[path] = entry
            plan.append(entry)
    return plan, generated, minted


# -- the sync ----------------------------------------------------------------


def sync(
    library_path: str,
    rows: list[dict],
    table: dict | None = None,
    on_progress=None,
) -> dict:
    """Make the input library exactly what these rows say.

    Incremental on purpose. A row whose path, type and lineage are all what the
    library already holds has nothing called on it, so its manifest entry and
    the identity hanging off that entry cannot move -- which is what lets two
    solves over an untouched recipe produce the same task key. A library nothing
    changed in is not saved either.

    Ordering is load-bearing. Everything is validated first, against the state
    the library will be *left* in rather than the one it is in, because the
    calls below assert mid-way and the save is at the end -- a collision found
    the hard way leaves a half-built library on disk. Then removals (repairing
    survivors' lineage first, since removal never touches children), then
    repoints, then retypes, then adds, then the sheet, then lineage, then one
    save, then the record.
    """
    lib = load_data_lib(library_path)
    record = op_samples.read_record(library_path)
    prior_rows = {str(k): str(v) for k, v in (record.get("rows") or {}).items()}
    prior_generated = {
        str(k): [str(x) for x in v] for k, v in (record.get("generated") or {}).items()
    }
    prior_array = {p for ps in prior_generated.values() for p in ps}

    rows = [dict(r) for r in rows if isinstance(r, dict) and r.get("id") is not None]
    # The one switch. A sheet attached makes every row an array row -- its
    # fields read the columns they bind -- and no sheet makes every row a plain
    # one holding its own text. A row with a field bound to nothing registers
    # nothing rather than falling back to its text: that fallback would be the
    # constant-under-a-sheet escape hatch this deliberately does not have, and
    # `problems` names the row instead of it going quiet.
    if table is None:
        array_rows, plain_rows = [], rows
    else:
        array_rows, plain_rows = op_samples.array_rows_of(rows), []

    want = _desired(plain_rows)
    held = _claim(lib, want, prior_rows)
    prior_minted = {
        str(k): {str(kk): str(vv) for kk, vv in (v or {}).items()}
        for k, v in (record.get("minted") or {}).items()
    }
    plan, generated, minted = (
        _array_plan(table, array_rows, prior_minted) if array_rows else ([], {}, {})
    )

    keep = set(held.values())
    wanted_array = {e["path"] for e in plan}
    doomed = [
        Path(p) for rid, p in prior_rows.items()
        if rid not in held and Path(p) in lib.manifest and Path(p) not in keep
    ]
    doomed += [
        Path(p) for p in sorted(prior_array)
        if p not in wanted_array and Path(p) in lib.manifest and Path(p) not in keep
    ]
    doomed = list(dict.fromkeys(doomed))

    problems = _problems(lib, want, held, plan, rows, table, array_rows, doomed)
    assert not problems, "; ".join(problems)

    changed = False

    # -- removals ----------------------------------------------------------
    if doomed:
        changed = True
        gone = set(doomed)
        # survivors first: `Remove` drops the item's own parent list and never
        # touches its children's, and neither `Pack` nor `Unpack` notices a
        # dangling one -- the first `Get` on the child raises, a long way away.
        for path in list(lib.manifest):
            if path in gone:
                continue
            current = lib.parents.get(path)
            if not current:
                continue
            kept = [m for m in current if m.path not in gone]
            if len(kept) != len(current):
                lib.parents[path] = kept
        for path in doomed:
            op_data.remove_item(library_path, str(path), save=False, lib=lib)

    # -- repoints ----------------------------------------------------------
    for rid, spec in want.items():
        at = held.get(rid)
        if at is None:
            continue
        if spec["mode"] == "value":
            # The library owns this path and never moves it: `at` is either what
            # the record remembered or what was minted for it, and re-pointing
            # would re-mint the identity hanging off it for nothing. The one
            # exception is a path the library cannot own at all -- what a row
            # switched over from file mode leaves behind. Giving that up matters:
            # `lib.location / at` with an absolute `at` discards the left side,
            # so a deferred path would be written at the filesystem root.
            if at.is_absolute():
                op_data.remove_item(library_path, str(at), save=False, lib=lib)
                del held[rid]
                changed = True
            continue
        to = spec["path"]
        if to is None:
            # An emptied path is not "no change", it is "I do not have this
            # yet", which is what a deferred row already says. One that already
            # says it keeps the path it minted; one that does not gives up the
            # path it had and is re-registered deferred by the adds below.
            if is_deferred(at):
                continue
            op_data.remove_item(library_path, str(at), save=False, lib=lib)
            del held[rid]
            changed = True
            continue
        if to == at:
            continue
        if at.is_absolute() != to.is_absolute():
            # not a repoint the library can express -- one side is a pointer at
            # the user's file and the other is a name it owns. Re-registered
            # instead; lineage is rebuilt from the rows below either way.
            op_data.remove_item(library_path, str(at), save=False, lib=lib)
            del held[rid]
        else:
            op_data.repoint_item(library_path, str(at), str(to), save=False, lib=lib)
            held[rid] = to
        changed = True

    # -- retypes -----------------------------------------------------------
    for rid, spec in want.items():
        at = held.get(rid)
        if at is None or lib.manifest.get(at) == spec["dtype"]:
            continue
        op_data.retype_item(library_path, str(at), spec["dtype"], save=False, lib=lib)
        changed = True

    # -- adds --------------------------------------------------------------
    #
    # Parentless, all of them: lineage is one pass at the end, over rows and
    # sheet items alike, which is what makes the order rows arrive in irrelevant
    # and lets a child be added before its parent.
    for rid, spec in want.items():
        if rid in held:
            continue
        changed = True
        if spec["mode"] == "value":
            out = op_data.add_value(
                library_path, mint_value_path(), spec["value"], spec["dtype"],
                save=False, lib=lib,
            )
        else:
            out = op_data.add_item(
                library_path,
                str(spec["path"]) if spec["path"] is not None else DEFERRED,
                spec["dtype"], save=False, lib=lib,
            )
        held[rid] = Path(out["path"])

    # -- the sheet ---------------------------------------------------------
    for i, entry in enumerate(plan):
        path = Path(entry["path"])
        if path in lib.manifest:
            if lib.manifest[path] != entry["dtype"]:
                op_data.retype_item(library_path, str(path), entry["dtype"], save=False, lib=lib)
                changed = True
            continue
        changed = True
        if entry["mode"] == "value":
            op_data.add_value(
                library_path, entry["path"], entry["value"], entry["dtype"],
                save=False, lib=lib,
            )
        else:
            op_data.add_item(library_path, entry["path"], entry["dtype"], save=False, lib=lib)
        if on_progress and (i + 1) % 25 == 0:
            on_progress(i + 1, len(plan))

    # -- values ------------------------------------------------------------
    #
    # A plain file write, with no manifest call: identity here is provenance,
    # not bytes, and `fork` is the explicit way to say "treat these as new".
    for rid, spec in want.items():
        if spec["mode"] != "value":
            continue
        f = lib.location / held[rid]
        if not f.is_file() or f.read_text(encoding="utf-8") != spec["value"]:
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text(spec["value"], encoding="utf-8")
    for entry in plan:
        if entry["mode"] != "value":
            continue
        f = lib.location / entry["path"]
        if not f.is_file() or f.read_text(encoding="utf-8") != entry["value"]:
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text(entry["value"], encoding="utf-8")

    # -- lineage -----------------------------------------------------------
    by_id = {str(r["id"]): r for r in plain_rows}
    for rid, at in held.items():
        want_parents = _resolve(by_id[rid], held, lib)
        if want_parents != immediate_parents(lib, at):
            op_data.replace_item_parents(
                library_path, str(at), [str(p) for p in want_parents], save=False, lib=lib,
            )
            changed = True
    for entry in plan:
        at = Path(entry["path"])
        want_parents = _resolve_paths(entry["parents"], held, lib)
        if want_parents != immediate_parents(lib, at):
            op_data.replace_item_parents(
                library_path, str(at), [str(p) for p in want_parents], save=False, lib=lib,
            )
            changed = True

    mapping = {rid: str(p) for rid, p in held.items()}
    made = [p for ps in generated.values() for p in ps]
    next_record = record | {
        "adopted": True,
        "rows": dict(sorted(mapping.items())),
        "paths": made,
        "generated": generated,
        # Per array value row, grouping key -> the path minted for it. Without
        # this the mint is not a mint but a re-roll on every solve, and every
        # sheet item loses its identity (and its cache) each time it is opened.
        "minted": minted,
        "row_count": len(table.get("rows") or []) if table is not None else 0,
        "columns": list(table.get("columns") or []) if table is not None else [],
    }

    if changed:
        # saved before the record is written: a torn write then leaves a row
        # registered and unrecorded, which `_claim` recovers from by matching on
        # the path itself. The other order leaves a record naming nothing.
        lib.Save()
    if next_record != record:
        op_samples.write_record(library_path, next_record)
    return {
        "rows": mapping,
        "generated": generated,
        "counts": {k: len(v) for k, v in generated.items()},
        "row_count": next_record["row_count"],
        "item_count": len(lib.manifest),
        "removed": [str(p) for p in doomed],
        "changed": changed,
    }


def _resolve(row: dict, held: dict[str, Path], lib) -> list[Path]:
    keys = [f"#{p}" for p in parent_ids(row)] + literal_parents(row)
    return _resolve_paths(keys, held, lib)


def _resolve_paths(keys: list[str], held: dict[str, Path], lib) -> list[Path]:
    """Parent references as manifest paths, dropping what is not there.

    A reference to a row that has gone, or to one with nothing on it yet, is
    dropped rather than refused: it is what deleting a row leaves behind, and
    the link is already gone from the recipe the user is looking at.
    """
    out: list[Path] = []
    for k in keys:
        p = held.get(k[1:]) if k.startswith("#") else Path(k)
        if p is None or p not in lib.manifest or p in out:
            continue
        out.append(p)
    return out


def _problems(lib, want, held, plan, rows, table, array_rows, doomed) -> list[str]:
    """Everything that would fail, before anything has been touched."""
    problems: list[str] = []
    try:
        assert_acyclic(rows)
    except AssertionError as exc:
        return [str(exc)]

    for spec in want.values():
        try:
            lib.GetType(spec["dtype"])
        except (AssertionError, ValueError, KeyError):
            problems.append(f"[{spec['dtype']}] is not a type in this library")
        # The no-slash guard that used to live here is gone with the name it
        # guarded: a value's path is minted, so there is no user input to
        # sanitise. Every guard below short-circuits on `path is None`, which is
        # what a value row now always states, so they are file-row guards now.
        if (
            spec["mode"] != "value"
            and spec["path"] is not None
            and not spec["path"].is_absolute()
            # ...unless it is already registered at exactly that path. A
            # library-owned file too big or too binary to read back as a value
            # is adopted as a file row holding its relative path, and refusing
            # that would make every solve fail over a row nobody had touched.
            and held.get(spec["rid"]) != spec["path"]
        ):
            # Relative means the library owns the file; absolute means it does
            # not. A file row points at a file on disk that is the user's, so a
            # relative one would have the library claim a file it never wrote --
            # and every later move of it would try to rename something that was
            # never there. The two are the row's own modes; this is refused
            # rather than guessed at.
            problems.append(
                f"[{spec['path']}] is a relative path, and a file row points at a "
                f"file of yours -- give it an absolute path, or make it a value "
                f"row for something the library should hold"
            )

    # what will still be in the manifest, and who will own it
    going = set(doomed)
    owner = {p: rid for rid, p in held.items()}
    seen: dict[Path, str] = {}
    for rid, spec in want.items():
        if spec["path"] is None:
            continue
        if spec["path"] in seen:
            problems.append(
                f"two input rows both want to be [{spec['path']}] -- one row, one path"
            )
        seen[spec["path"]] = rid
        p = spec["path"]
        if p in lib.manifest and p not in going and owner.get(p, rid) == rid:
            continue
        if p in lib.manifest and p not in going:
            # owned by another row: either a swap (named below) or a plain
            # collision, and either way this one cannot have it
            if want.get(owner[p], {}).get("path") != held.get(rid):
                problems.append(f"[{p}] is already registered here")

    # a swap is two repoints that are each blocked by the other, and the library
    # has no way to say it in one move
    for rid, spec in want.items():
        at = held.get(rid)
        if at is None or spec["path"] is None or spec["path"] == at:
            continue
        holder = next((r for r, p in held.items() if p == spec["path"] and r != rid), None)
        if holder is not None and want.get(holder, {}).get("path") == at:
            problems.append(
                f"[{at}] and [{spec['path']}] would swap places, which cannot be "
                f"done in one step -- change one of them to something else first"
            )

    if table is not None and array_rows:
        checked = op_samples.validate(str(lib.location), table, rows)
        problems += [p["message"] for p in checked["problems"]]

    for entry in plan:
        p = Path(entry["path"])
        holder = next((r for r, h in held.items() if h == p), None)
        if holder is not None:
            problems.append(
                f"the sheet produces [{p}], which an input row is already registered as"
            )
    return list(dict.fromkeys(problems))


# -- adoption ----------------------------------------------------------------


def _adopted_id(path: str) -> str:
    """A stable row id for an item that arrived without one.

    Derived from the path so adopting twice produces the same row rather than a
    second one -- which matters, because two reads of a workflow can race.
    """
    return "i" + hashlib.md5(path.encode("utf-8")).hexdigest()[:10]


def _adopted_entries(value: str) -> list[dict]:
    """What a registered value file reads as, as recipe entries.

    One unkeyed entry holding the file verbatim is what a value row has always
    been and stays the fallback. An object splits into the keyed entries
    describing it instead, because two labelled boxes are what a person can edit
    and a line of JSON in one box is not -- read metadata is several facts.

    Only when `render_value` puts the file back byte for byte, though: a leaf's
    identity is content addressed, so a file this did not write is one it must
    not rewrite. A value that would change under the round trip -- a nested
    object, a string that reads as a number -- keeps the single entry it always
    had.
    """
    try:
        parsed = json.loads(value)
    except ValueError:
        return [{"key": "", "value": value}]
    if not isinstance(parsed, dict) or not parsed:
        return [{"key": "", "value": value}]
    ents = [
        {"key": str(k), "value": v if isinstance(v, str) else json.dumps(v)}
        for k, v in parsed.items()
    ]
    return ents if render_value(ents) == value else [{"key": "", "value": value}]


def adopt(library_path: str, rows: list[dict], record: dict | None = None) -> dict | None:
    """One editable row per registered item that no row speaks for.

    Runs once in a workflow's life, and the record says so afterwards. Without
    that mark, deleting a row could not be expressed: the item outlives the row
    until the next solve, and every read in between would put the row back.

    Returns `{rows, record}`, or `None` when there is nothing to do.
    """
    record = op_samples.read_record(library_path) if record is None else record
    if record.get("adopted"):
        return None
    lib = load_data_lib(library_path)
    rows = [dict(r) for r in rows if isinstance(r, dict) and r.get("id") is not None]

    prior = {str(k): str(v) for k, v in (record.get("rows") or {}).items()}
    by_path: dict[str, str] = {v: k for k, v in prior.items()}
    spoken_for = set(prior.values())
    spoken_for |= {str(p) for ps in (record.get("generated") or {}).values() for p in ps}
    for r in rows:
        rid = str(r["id"])
        name = row_identity(r)
        if rid not in prior and name and Path(name) in lib.manifest:
            spoken_for.add(name)
            by_path.setdefault(name, rid)
            prior[rid] = name

    made: list[dict] = []
    made_ids: set[str] = set()
    for path, dtype in lib.manifest.items():
        s = str(path)
        if s in spoken_for:
            continue
        rid = _adopted_id(s)
        value = read_value(lib.location, path)
        if is_deferred(path):
            row = {"id": rid, "mode": "file", "path": "", "name": "",
                   "values": [{"key": "", "value": ""}], "dtype": dtype, "parents": []}
        elif value is not None:
            # No name: the row is bound to this item by the record written
            # below, not by restating the path on the row. So a value adopted
            # from a library that named its files keeps that filename forever,
            # invisibly, and nothing is re-minted.
            #
            row = {"id": rid, "mode": "value", "path": "", "name": "",
                   "values": _adopted_entries(value), "dtype": dtype, "parents": []}
        else:
            row = {"id": rid, "mode": "file", "path": s, "name": "",
                   "values": [{"key": "", "value": ""}], "dtype": dtype, "parents": []}
        made.append(row)
        made_ids.add(rid)
        prior[rid] = s
        by_path[s] = rid

    # Lineage, in row ids: unbound, every deferred row's path is the same
    # nothing, and a path is not what a row is called any more.
    out = rows + made
    for row in out:
        at = prior.get(str(row["id"]))
        if str(row["id"]) in made_ids and at is not None:
            row["parents"] = [
                f"#{by_path[str(p)]}" if str(p) in by_path else str(p)
                for p in immediate_parents(lib, Path(at))
            ]
        else:
            row["parents"] = [
                p if str(p).startswith("#") else (
                    f"#{by_path[str(p)]}" if str(p) in by_path else str(p)
                )
                for p in (row.get("parents") or [])
            ]

    next_record = record | {"adopted": True, "rows": dict(sorted(prior.items()))}
    if not made and next_record == record and out == rows:
        return None
    return {"rows": out, "record": next_record}
