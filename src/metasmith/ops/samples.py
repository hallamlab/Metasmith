"""Sample tables: one sheet, one declared row per kind of input, N runs.

Metasmith has always been able to fan a plan out across samples -- register a
per-sample marker, hang that sample's files off it, and name the marker's type
as the sample type. What it has never had is a way to *say* that other than one
`AddItem` call per sample per file. This module is that way: a table you already
have, plus one declared input row per column, expanded into the library.

A **sample array** is an ordinary input row whose path (or, for a value row, its
name or value) holds `{column}` tokens: one declaration standing for N items,
indexed by the sheet. It is never registered as it stands. Exactly one array row
is the **index**: it becomes one item per table row with nothing above it, every
other array row's items descend from it, and its declared type is what the
planner is handed as `sample_type`. That shape is not incidental --

  * `AsSamples` masks a sample as {index item} u ancestors u descendants. An
    index item with a *parent* puts that parent's whole subtree in every mask,
    which collapses all samples into one view. So the index row must have no
    parents at all.
  * An array row that does not transitively reach the index lands in no mask,
    and the planner never sees it. So every other one must descend from it.

Both are refusals here rather than warnings, because both fail silently.

(A *template*, elsewhere in metasmith, is a stored workflow you start from --
a different thing entirely, which is why this one is not called that.)

The generated items are recorded so a re-expansion can take back exactly what
the last one put down. That record is server-owned and belongs beside
`result.yml`, never in the request the browser rewrites wholesale.
"""
from __future__ import annotations

import io
import re
from pathlib import Path

from ._common import load_data_lib

# `{column}` -- one level, no nesting, no braces inside. A path is not a
# templating language and the moment it starts to look like one, a person has to
# know which of two things `{a{b}}` means.
TOKEN = re.compile(r"\{([^{}]*)\}")

DELIMITED_SUFFIXES = {".csv": ",", ".tsv": "\t", ".tab": "\t", ".txt": None}
EXCEL_SUFFIXES = {".xlsx", ".xlsm"}

EXCEL_HELP = (
    "reading excel needs openpyxl, which this install does not have. "
    "Save the sheet as csv and paste or upload that instead."
)


# -- the table ---------------------------------------------------------------


def _columns_of(header: list) -> list[str]:
    cols = [str(c).strip() for c in header]
    assert all(cols), "the table has a column with no name in its header row"
    dupes = sorted({c for c in cols if cols.count(c) > 1})
    assert not dupes, (
        f"the table names the same column more than once: {', '.join(dupes)} -- "
        f"a token could mean either"
    )
    return cols


def parse_table(data: bytes, fmt: str | None = None, filename: str | None = None) -> dict:
    """Read a sheet into `{format, columns, rows, row_count}`.

    Every cell comes back as a string with NA handling off, deliberately: left
    to itself pandas turns an empty cell into the float `NaN` -- a literal `nan`
    in a path -- and a sample id of `007` into `7`.
    """
    import pandas as pd

    fmt = (fmt or "").strip().lower() or None
    suffix = Path(filename or "").suffix.lower()
    if fmt is None:
        fmt = "excel" if suffix in EXCEL_SUFFIXES else "delimited"

    # The header row is taken here rather than left to pandas: given two columns
    # of the same name pandas silently renames the second to `x.1`, and a token
    # would then mean whichever of the two the user was not thinking of.
    if fmt == "excel":
        try:
            import openpyxl  # noqa: F401
        except ImportError:
            raise AssertionError(EXCEL_HELP)
        frame = pd.read_excel(
            io.BytesIO(data), dtype=str, keep_default_na=False, engine="openpyxl",
            header=None,
        )
    else:
        text = data.decode("utf-8-sig") if isinstance(data, bytes) else str(data)
        assert text.strip(), "the table is empty"
        sep = {"csv": ",", "tsv": "\t"}.get(fmt, DELIMITED_SUFFIXES.get(suffix))
        frame = pd.read_csv(
            io.StringIO(text),
            # sep=None asks the python engine to sniff the delimiter, which is
            # what makes "or other table" mean anything
            sep=sep, engine="python" if sep is None else "c",
            dtype=str, keep_default_na=False, header=None,
        )
        fmt = "delimited"

    records = list(frame.itertuples(index=False, name=None))
    assert records, "the table is empty"
    columns = _columns_of(records[0])
    rows = [
        {col: str(val).strip() for col, val in zip(columns, record)}
        for record in records[1:]
    ]
    assert rows, "the table has a header row and nothing under it"
    return {"format": fmt, "columns": columns, "rows": rows, "row_count": len(rows)}


def read_table_file(path: str | Path, fmt: str | None = None) -> dict:
    p = Path(path)
    assert p.is_file(), f"no table at [{p}]"
    return parse_table(p.read_bytes(), fmt=fmt, filename=p.name)


# -- the attached sheet ------------------------------------------------------
#
# Stored verbatim, under a fixed stem beside the library. Verbatim because it is
# the user's own file and re-emitting a parsed copy would quietly drop whatever
# the parse did not understand; under a fixed stem because the workflow
# directory *is* the task bundle root, and a name taken from the upload could
# collide with `task.yml`, `data/`, `transforms/` or the staging directory.

TABLE_STEM = "sample_table"
DEFAULT_SUFFIX = ".csv"


def attached_table_path(where: str | Path) -> Path | None:
    found = sorted(Path(where).glob(f"{TABLE_STEM}.*"))
    return found[0] if found else None


def attach_table(
    where: str | Path,
    data: bytes,
    filename: str | None = None,
    fmt: str | None = None,
) -> dict:
    """Store a sheet, replacing whatever was there. Parses first: an unreadable
    upload should leave the previous one alone rather than half-replace it."""
    where = Path(where)
    parsed = parse_table(data, fmt=fmt, filename=filename)
    suffix = Path(filename or "").suffix.lower()
    if suffix not in DELIMITED_SUFFIXES and suffix not in EXCEL_SUFFIXES:
        suffix = DEFAULT_SUFFIX
    detach_table(where)
    where.mkdir(parents=True, exist_ok=True)
    dest = where / f"{TABLE_STEM}{suffix}"
    dest.write_bytes(data)
    return {"filename": filename or dest.name, "path": str(dest), **parsed}


def read_attached_table(where: str | Path) -> dict | None:
    p = attached_table_path(where)
    if p is None:
        return None
    return {"filename": p.name, "path": str(p), **read_table_file(p)}


def detach_table(where: str | Path) -> dict:
    removed = []
    for p in sorted(Path(where).glob(f"{TABLE_STEM}.*")):
        p.unlink()
        removed.append(p.name)
    return {"removed": removed}


# -- sample arrays -----------------------------------------------------------


def columns_in(text: str | None) -> list[str]:
    """The column names a field names, in order, without duplicates."""
    out: list[str] = []
    for m in TOKEN.finditer(text or ""):
        name = m.group(1).strip()
        if name and name not in out:
            out.append(name)
    return out


def is_array_row(row: dict) -> bool:
    return bool(columns_in(row.get("path")) or columns_in(row.get("name"))
                or columns_in(row.get("value")))


def substitute(text: str | None, record: dict[str, str]) -> str:
    return TOKEN.sub(lambda m: record[m.group(1).strip()], text or "")


def _fields_of(row: dict) -> list[tuple[str, str]]:
    """(label, text) for every field of an array row that may hold a token."""
    if row.get("mode") == "value":
        return [("name", row.get("name") or ""), ("value", row.get("value") or "")]
    return [("path", row.get("path") or "")]


def _array_parents(row: dict) -> list[str]:
    return [str(p)[1:] for p in (row.get("parents") or []) if str(p).startswith("#")]


def _plain_parents(row: dict) -> list[str]:
    return [str(p) for p in (row.get("parents") or []) if not str(p).startswith("#")]


def order_array_rows(array_rows: list[dict]) -> list[dict]:
    """Array rows, parents before children. Assumes the lineage is acyclic."""
    by_id = {str(t["id"]): t for t in array_rows}
    out: list[dict] = []
    seen: set[str] = set()

    def visit(tid: str, stack: tuple[str, ...] = ()):
        if tid in seen or tid not in by_id:
            return
        assert tid not in stack, "these rows descend from each other in a loop"
        for p in _array_parents(by_id[tid]):
            visit(p, stack + (tid,))
        seen.add(tid)
        out.append(by_id[tid])

    for t in array_rows:
        visit(str(t["id"]))
    return out


# -- validation --------------------------------------------------------------


def _problem(where: str, message: str) -> dict:
    return {"where": where, "message": message}


def validate(library_path: str, table: dict, rows: list[dict]) -> dict:
    """Everything wrong with this expansion, without touching the library.

    `rows` is the whole input side of the recipe -- array rows and plain drafts
    together -- because half of what can be wrong is about how the two relate.
    Returns `{problems, array_rows, index_id}`; `problems` empty means expandable.
    """
    array_rows = [r for r in rows if is_array_row(r)]
    problems: list[dict] = []
    if not array_rows:
        return {"problems": problems, "array_rows": [], "index_id": None}

    columns = set(table.get("columns") or [])
    by_id = {str(t["id"]): t for t in array_rows}

    marked = [t for t in array_rows if t.get("index")]
    index_id = str(marked[0]["id"]) if len(marked) == 1 else None
    if not marked:
        problems.append(_problem("index", (
            "no row is marked as the sample index -- one array row has to say "
            "what a sample *is*, and its type is what the planner splits on"
        )))
    elif len(marked) > 1:
        problems.append(_problem("index", (
            f"{len(marked)} rows are marked as the sample index; exactly one can be"
        )))

    for t in array_rows:
        tid = str(t["id"])
        label = (t.get("path") or t.get("name") or tid)
        if not t.get("dtype"):
            problems.append(_problem(tid, f"[{label}] has no type"))
        for field, text in _fields_of(t):
            for col in columns_in(text):
                if col not in columns:
                    problems.append(_problem(tid, (
                        f"[{label}] names a column [{col}] in its {field}, which "
                        f"this table does not have"
                    )))
        if t.get("mode") == "value" and "/" in (t.get("name") or ""):
            problems.append(_problem(tid, (
                f"[{label}] is a value row, and a value's name is a filename in "
                f"the library -- it cannot hold a slash"
            )))
        for p in _array_parents(t):
            if p not in by_id:
                problems.append(_problem(tid, f"[{label}] descends from a row that is gone"))

    try:
        order_array_rows(array_rows)
    except AssertionError as exc:
        problems.append(_problem("lineage", str(exc)))
        return {"problems": problems, "array_rows": array_rows, "index_id": index_id}

    if index_id is not None:
        index = by_id[index_id]
        if index.get("parents"):
            problems.append(_problem(index_id, (
                "the sample index cannot descend from anything: a shared ancestor "
                "above it puts every sample's files in every other sample, and the "
                "plan silently becomes one run over the whole library"
            )))
        # reachability up the array chain -- a row that cannot get to the
        # index lands in no sample's mask and the planner never sees it
        reaches: dict[str, bool] = {}

        def reaches_index(tid: str, stack: frozenset = frozenset()) -> bool:
            if tid == index_id:
                return True
            if tid in reaches:
                return reaches[tid]
            if tid in stack or tid not in by_id:
                return False
            out = any(reaches_index(p, stack | {tid}) for p in _array_parents(by_id[tid]))
            reaches[tid] = out
            return out

        for t in array_rows:
            tid = str(t["id"])
            if reaches_index(tid):
                continue
            label = (t.get("path") or t.get("name") or tid)
            problems.append(_problem(tid, (
                f"[{label}] does not descend from the sample index, so its files "
                f"belong to no sample and the planner will not see them"
            )))

    problems += _path_problems(library_path, table, array_rows, by_id)
    return {"problems": problems, "array_rows": array_rows, "index_id": index_id}


def _path_problems(library_path, table, array_rows, by_id) -> list[dict]:
    """What the substituted paths themselves are wrong about.

    Checked before anything is registered, because `AddItem` asserts mid-loop on
    a path already in the manifest and `Save` is at the end -- a collision found
    the hard way leaves a half-expanded library on disk.
    """
    lib = load_data_lib(library_path)
    previous = set(read_record(library_path).get("paths", []))
    existing = {str(p) for p in lib.manifest} - previous
    problems: list[dict] = []
    minted: dict[str, tuple[str, str, int]] = {}
    columns = set(table.get("columns") or [])
    # an array row naming a column that is not there is already reported, and
    # substituting it here would raise instead of adding to the list
    array_rows = [
        t for t in array_rows
        if all(c in columns for _f, text in _fields_of(t) for c in columns_in(text))
    ]

    for i, record in enumerate(table.get("rows") or []):
        for t in array_rows:
            tid = str(t["id"])
            label = (t.get("path") or t.get("name") or tid)
            fields = dict(_fields_of(t))
            missing = [
                c for text in fields.values() for c in columns_in(text)
                if c in columns and not (record.get(c) or "").strip()
            ]
            if missing:
                problems.append(_problem(tid, (
                    f"row {i + 1} of the table has nothing under "
                    f"{', '.join(sorted(set(missing)))}, which [{label}] needs"
                )))
                continue
            key = fields.get("path") if t.get("mode") != "value" else fields.get("name")
            path = substitute(key, record)
            if not path:
                problems.append(_problem(tid, f"[{label}] comes out empty on row {i + 1}"))
            elif path in existing:
                problems.append(_problem(tid, (
                    f"[{label}] comes out as [{path}] on row {i + 1}, which is "
                    f"already registered"
                )))
            elif path in minted:
                other_tid, other_label, other_row = minted[path]
                if other_tid == tid:
                    problems.append(_problem(tid, (
                        f"[{label}] comes out as [{path}] on both row {other_row} "
                        f"and row {i + 1} -- it does not vary per row"
                    )))
                else:
                    problems.append(_problem(tid, (
                        f"[{label}] comes out as [{path}] on row {i + 1}, which is "
                        f"also what [{other_label}] comes out as on row {other_row}"
                    )))
            else:
                minted[path] = (tid, label, i + 1)
        if len(problems) > 40:
            problems.append(_problem("", "...and more; the first forty are shown"))
            break

    for t in array_rows:
        for p in _plain_parents(t):
            if Path(p) not in lib.manifest:
                problems.append(_problem(str(t["id"]), (
                    f"descends from [{p}], which is not registered"
                )))
    return problems


# -- the record --------------------------------------------------------------
#
# What the last expansion put down, so the next one can take back exactly that
# and no more. Server-owned and beside the library rather than in the request:
# the browser rewrites the request wholesale through an unlocked
# read-modify-write on nearly every edit, and would race a background expand.

RECORD_FILE = "expansion.yml"


def record_path(library_path: str | Path) -> Path:
    return Path(library_path).parent / RECORD_FILE


def read_record(library_path: str | Path) -> dict:
    import yaml

    p = record_path(library_path)
    if not p.is_file():
        return {}
    with open(p) as f:
        return yaml.safe_load(f) or {}


def write_record(library_path: str | Path, record: dict) -> Path:
    import yaml

    p = record_path(library_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    with open(tmp, "w") as f:
        yaml.safe_dump(record, f, sort_keys=False)
    tmp.replace(p)
    return p


# -- expansion ---------------------------------------------------------------


def clear(library_path: str, save: bool = True) -> dict:
    """Unregister everything the last expansion registered.

    Survivors are repaired first: `Remove` drops the item's own parent list but
    never touches its *children's* records, and neither `Pack` nor `Unpack`
    notices a dangling one -- the first `Get` on the child raises `KeyError`, a
    long way from here. Removal takes the paths from the record but intersects
    them with the manifest, since a generated row may have been deleted by hand.
    """
    lib = load_data_lib(library_path)
    record = read_record(library_path)
    doomed = [Path(p) for p in record.get("paths", [])]
    present = [p for p in doomed if p in lib.manifest]
    doomed_set = set(present)

    for path in list(lib.manifest):
        if path in doomed_set:
            continue
        current = lib.parents.get(path)
        if not current:
            continue
        kept = [m for m in current if m.path not in doomed_set]
        if len(kept) != len(current):
            lib.parents[path] = kept

    for path in present:
        lib.Remove(path)
    if save:
        lib.Save()
    if record:
        write_record(library_path, record | {"paths": [], "generated": {}})
    return {"removed": [str(p) for p in present]}


def expand(
    library_path: str,
    table: dict,
    rows: list[dict],
    on_progress=None,
) -> dict:
    """Register one item per (array row x table row). Validates first, saves once.

    The library is loaded once and saved once: `ops.data.add_item` re-loads and
    re-saves per call, which over a two-hundred-row sheet is both slow and a
    window in which a failure leaves the library half-built.
    """
    checked = validate(library_path, table, rows)
    assert not checked["problems"], "; ".join(p["message"] for p in checked["problems"])
    array_rows = checked["array_rows"]
    if not array_rows:
        return {"generated": {}, "counts": {}, "row_count": 0, "sample_type": None}

    clear(library_path, save=True)

    lib = load_data_lib(library_path)
    order = order_array_rows(array_rows)
    index_id = checked["index_id"]
    records = table.get("rows") or []
    generated: dict[str, list[str]] = {str(t["id"]): [] for t in array_rows}
    made: list[str] = []

    for i, record in enumerate(records):
        per_row: dict[str, Path] = {}
        for row in order:
            tid = str(row["id"])
            parents = [per_row[p] for p in _array_parents(row) if p in per_row]
            parents += [Path(p) for p in _plain_parents(row)]
            if row.get("mode") == "value":
                path = lib.AddValue(
                    substitute(row.get("name"), record),
                    substitute(row.get("value"), record),
                    row["dtype"], parents=parents,
                )
            else:
                path = lib.AddItem(
                    substitute(row.get("path"), record), row["dtype"], parents=parents,
                )
            per_row[tid] = path
            generated[tid].append(str(path))
            made.append(str(path))
        if on_progress and (i + 1) % 25 == 0:
            on_progress(i + 1, len(records))
    lib.Save()

    sample_type = next(
        (t.get("dtype") for t in array_rows if str(t["id"]) == index_id), None,
    )
    record = {
        "paths": made,
        "generated": generated,
        "row_count": len(records),
        "index_id": index_id,
        "sample_type": sample_type,
        "columns": list(table.get("columns") or []),
    }
    write_record(library_path, record)
    return {
        "generated": generated,
        "counts": {k: len(v) for k, v in generated.items()},
        "row_count": len(records),
        "sample_type": sample_type,
        "item_count": len(made),
    }
