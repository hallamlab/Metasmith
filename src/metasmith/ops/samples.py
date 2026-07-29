"""Sample tables: one sheet, one declared row per kind of input, N runs.

Metasmith has always been able to fan a plan out across samples -- register a
per-sample marker, hang that sample's files off it, and name the marker's type
as the sample type. What it has never had is a way to *say* that other than one
`AddItem` call per sample per file. This module is that way: a table you already
have, plus one declared input row per column, expanded into the library.

A **sample array** is an ordinary input row whose path (or, for a value row, its
name or value) holds `{column}` tokens: one declaration standing for N items,
indexed by the sheet. It is never registered as it stands. Array rows wire into
an arbitrary parent DAG the same way any other input row does -- a column with
no parents, a column parented to another -- and every table row instances that
whole DAG once. There is no privileged "index" column and no per-sample masking
here: multiplicity (how many distinct pangenomes, how many distinct samples)
falls out of ordinary lineage, the same way `group_by` resolves it at the
transform level. Two rows that substitute to the same path share one instance
-- a deliberate grouping, not a collision -- so long as a value row agrees with
itself about what that shared name holds.

(`AsSamples`, elsewhere in metasmith, masks a library by an index item's
ancestors/descendants; it is a valid, separate, lower-level primitive that this
module does not use or produce.)

(A *template*, elsewhere in metasmith, is a stored workflow you start from --
a different thing entirely, which is why this one is not called that.)

This module reads the sheet and says what is wrong with it; it does not write
to a library. `ops.inputs.sync` is the one writer, and it registers array rows
and plain rows in the same pass -- they are the same kind of row and there is
one library state, so two functions with two ideas of what is in it is exactly
the state that leaves it half built.

The record of what was put down lives here because this module owns its shape.
It is server-owned and belongs beside `result.yml`, never in the request the
browser rewrites wholesale.
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
# The upload's own name, or its absence: the sheet itself is always stored
# under the fixed stem above, so this is the only place a paste is told apart
# from an upload once the request that made it is gone. Named with an
# underscore rather than a dot so `{TABLE_STEM}.*` -- which finds the sheet
# itself -- never matches it.
ORIGIN_FILE = f"{TABLE_STEM}_origin.txt"


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
    # Written unconditionally, even empty for a paste: its mere presence is what
    # tells "attached with no name, on purpose" apart from "attached before this
    # file existed", which `read_attached_table` falls back to `dest.name` for.
    (where / ORIGIN_FILE).write_text(filename or "", encoding="utf-8")
    return {"filename": filename or "pasted", "path": str(dest), **parsed}


def read_attached_table(where: str | Path) -> dict | None:
    p = attached_table_path(where)
    if p is None:
        return None
    origin = Path(where) / ORIGIN_FILE
    if origin.is_file():
        name = origin.read_text(encoding="utf-8").strip() or "pasted"
    else:
        name = p.name  # attached before ORIGIN_FILE existed
    return {"filename": name, "path": str(p), **read_table_file(p)}


def detach_table(where: str | Path) -> dict:
    removed = []
    for p in sorted(Path(where).glob(f"{TABLE_STEM}.*")):
        p.unlink()
        removed.append(p.name)
    origin = Path(where) / ORIGIN_FILE
    if origin.is_file():
        origin.unlink()
        removed.append(origin.name)
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
    Returns `{problems, array_rows}`; `problems` empty means expandable.
    """
    array_rows = [r for r in rows if is_array_row(r)]
    problems: list[dict] = []
    if not array_rows:
        return {"problems": problems, "array_rows": []}

    columns = set(table.get("columns") or [])
    by_id = {str(t["id"]): t for t in array_rows}
    all_ids = {str(r["id"]) for r in rows if r.get("id") is not None}

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
            # ...against every row, not just the array ones: an array row
            # descending from a plain row is one declared DAG hung off a single
            # shared input, which is the ordinary shape of a sample recipe
            if p not in all_ids:
                problems.append(_problem(tid, f"[{label}] descends from a row that is gone"))

    try:
        order_array_rows(array_rows)
    except AssertionError as exc:
        problems.append(_problem("lineage", str(exc)))
        return {"problems": problems, "array_rows": array_rows}

    problems += _path_problems(library_path, table, array_rows, by_id, rows)
    return {"problems": problems, "array_rows": array_rows}


def _plain_paths(rows: list[dict]) -> set[str]:
    """What the recipe's non-array rows will occupy once they are registered.

    Read off the rows rather than off the manifest: a plain row is registered
    from the row on every solve, so what the library holds right now is the
    *previous* answer -- it still lists a row that has since been deleted, and
    does not list one that has since been typed in.
    """
    out: set[str] = set()
    for r in rows:
        if is_array_row(r) or not (r.get("dtype") or "").strip():
            continue
        name = (r.get("name") if r.get("mode") == "value" else r.get("path")) or ""
        if name.strip():
            out.add(name.strip())
    return out


def _path_problems(library_path, table, array_rows, by_id, rows) -> list[dict]:
    """What the substituted paths themselves are wrong about.

    Checked before anything is registered, because `AddItem` asserts mid-loop on
    a path already in the manifest and `Save` is at the end -- a collision found
    the hard way leaves a half-expanded library on disk.
    """
    lib = load_data_lib(library_path)
    record = read_record(library_path)
    previous = set(record.get("paths", []))
    owned = {str(v) for v in (record.get("rows") or {}).values()}
    existing = _plain_paths(rows) | ({str(p) for p in lib.manifest} - previous - owned)
    problems: list[dict] = []
    minted: dict[str, tuple[str, str, int, str | None]] = {}
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
            is_value = t.get("mode") == "value"
            key = fields.get("name") if is_value else fields.get("path")
            path = substitute(key, record)
            value = substitute(fields.get("value"), record) if is_value else None
            if not path:
                problems.append(_problem(tid, f"[{label}] comes out empty on row {i + 1}"))
            elif path in existing:
                problems.append(_problem(tid, (
                    f"[{label}] comes out as [{path}] on row {i + 1}, which is "
                    f"already registered"
                )))
            elif path in minted:
                other_tid, other_label, other_row, other_value = minted[path]
                if other_tid == tid:
                    # the same declared column landing on the same path again is a
                    # deliberate grouping -- rows sharing one name become one shared
                    # instance. Only a value row can disagree with itself: two rows
                    # naming the same thing but writing different content into it.
                    if is_value and value != other_value:
                        problems.append(_problem(tid, (
                            f"[{label}] comes out as [{path}] on both row {other_row} "
                            f"and row {i + 1}, but with different values -- rows "
                            f"sharing a name must agree on what it holds"
                        )))
                else:
                    problems.append(_problem(tid, (
                        f"[{label}] comes out as [{path}] on row {i + 1}, which is "
                        f"also what [{other_label}] comes out as on row {other_row}"
                    )))
            else:
                minted[path] = (tid, label, i + 1, value)
        if len(problems) > 40:
            problems.append(_problem("", "...and more; the first forty are shown"))
            break

    # A literal path in a lineage is a leftover from when a registered row was
    # named by its path; a row is named by its id now. It still has to point at
    # something -- either an entry that is there, or a row that will put one there.
    reachable = _plain_paths(rows) | {str(p) for p in lib.manifest}
    for t in array_rows:
        for p in _plain_parents(t):
            if p not in reachable:
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


