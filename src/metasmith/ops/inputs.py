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
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from ..models.libraries import DataInstanceLibrary
from ..models.paths import DEFERRED, is_deferred
from . import data as op_data
from . import samples as op_samples
from ._common import load_data_lib


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


def row_identity(row: dict) -> str:
    """What the row says it is: a value's name, or a path."""
    if row.get("mode") == "value":
        return (row.get("name") or "").strip()
    return (row.get("path") or "").strip()


def registerable(row: dict) -> bool:
    """Whether this row has enough on it to be an input.

    A type, and -- for a value row -- a name to hold it under. A *file* row with
    no path is registerable and lands as a deferred input: "I will have one of
    these" is a complete statement about a plan, it is what a template's rows
    are, and the refusal belongs at stage rather than here.
    """
    if not (row.get("dtype") or "").strip():
        return False
    if row.get("mode") == "value":
        return bool((row.get("name") or "").strip())
    return True


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
                "path": Path((r.get("name") or "").strip()),
                "value": r.get("value") or "",
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
        if rid in held or spec["path"] is None:
            continue
        p = str(spec["path"])
        if Path(p) in lib.manifest and p not in spoken_for:
            held[rid] = spec["path"]
            taken.add(p)
    return held


# -- the sheet ---------------------------------------------------------------


def _array_plan(table: dict, array_rows: list[dict]) -> list[dict]:
    """One entry per (array row x sheet row), parents already resolved.

    A parent that is another array row resolves to that row's own item on this
    sheet row -- which is what makes one declared DAG instance once per sample.
    A parent that is a plain row is left as its `#id`: plain rows are registered
    first and their paths are not known until they are.

    Two entries landing on the same path is a deliberate grouping, not a
    collision -- `samples.validate` has already ruled out the case where they
    disagree about what that shared name holds -- so the second one contributes
    only its lineage.
    """
    if not array_rows:
        return [], {}
    order = op_samples.order_array_rows(array_rows)
    by_id = {str(t["id"]): t for t in array_rows}
    plan: list[dict] = []
    seen: dict[str, dict] = {}
    # what each array row stands for, counted per sheet row rather than per
    # distinct path: "x 3 registered" is a statement about the sheet, and two
    # sheet rows sharing one instance are still two sheet rows
    generated: dict[str, list[str]] = {str(t["id"]): [] for t in array_rows}
    for record in table.get("rows") or []:
        here: dict[str, str] = {}
        for row in order:
            rid = str(row["id"])
            is_value = row.get("mode") == "value"
            key = row.get("name") if is_value else row.get("path")
            path = op_samples.substitute(key, record)
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
                "value": op_samples.substitute(row.get("value"), record) if is_value else None,
                "parents": list(parents),
            }
            seen[path] = entry
            plan.append(entry)
    return plan, generated


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
    array_rows = [r for r in rows if op_samples.is_array_row(r)]
    plain_rows = [r for r in rows if not op_samples.is_array_row(r)]
    # a token with no sheet behind it stands for nothing; whatever the last
    # expansion left is taken back rather than left to plan as itself
    if table is None:
        array_rows = []

    want = _desired(plain_rows)
    held = _claim(lib, want, prior_rows)
    plan, generated = _array_plan(table, array_rows) if array_rows else ([], {})

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
                library_path, str(spec["path"]), spec["value"], spec["dtype"],
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
        if spec["mode"] == "value" and "/" in str(spec["path"]):
            problems.append(
                f"[{spec['path']}] is a value's name, which is a filename in the "
                f"library -- it cannot hold a slash"
            )
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
            row = {"id": rid, "mode": "file", "path": "", "name": "", "value": "",
                   "dtype": dtype, "parents": []}
        elif value is not None:
            row = {"id": rid, "mode": "value", "path": "", "name": s, "value": value,
                   "dtype": dtype, "parents": []}
        else:
            row = {"id": rid, "mode": "file", "path": s, "name": "", "value": "",
                   "dtype": dtype, "parents": []}
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
