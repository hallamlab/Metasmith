from __future__ import annotations

import io
from pathlib import Path

from ._common import load_data_lib
from .rows import column_of
from .rows import entries as row_entries

DELIMITED_SUFFIXES = {".csv": ",", ".tsv": "\t", ".tab": "\t", ".txt": None}
EXCEL_SUFFIXES = {".xlsx", ".xlsm"}

EXCEL_HELP = (
    "reading excel needs openpyxl, which this install does not have. "
    "Save the sheet as csv and paste or upload that instead."
)


def _columns_of(header: list) -> list[str]:
    cols = [str(c).strip() for c in header]
    assert all(cols), "the table has a column with no name in its header row"
    dupes = sorted({c for c in cols if cols.count(c) > 1})
    assert not dupes, (
        f"the table names the same column more than once: {', '.join(dupes)} -- "
        f"a field bound to one could mean either"
    )
    return cols


def parse_table(data: bytes, fmt: str | None = None, filename: str | None = None) -> dict:
    import pandas as pd

    fmt = (fmt or "").strip().lower() or None
    suffix = Path(filename or "").suffix.lower()
    if fmt is None:
        fmt = "excel" if suffix in EXCEL_SUFFIXES else "delimited"

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


TABLE_STEM = "sample_table"
DEFAULT_SUFFIX = ".csv"
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
    where = Path(where)
    parsed = parse_table(data, fmt=fmt, filename=filename)
    suffix = Path(filename or "").suffix.lower()
    if suffix not in DELIMITED_SUFFIXES and suffix not in EXCEL_SUFFIXES:
        suffix = DEFAULT_SUFFIX
    detach_table(where)
    where.mkdir(parents=True, exist_ok=True)
    dest = where / f"{TABLE_STEM}{suffix}"
    dest.write_bytes(data)
    (where / ORIGIN_FILE).write_text(filename or "", encoding="utf-8")
    return {"filename": filename or "pasted", "path": str(dest), **parsed}


def table_to_text(table: dict) -> str:
    """The attached table, serialized back to delimited text -- what a
    re-edit starts from, whether the table began as a paste or an upload."""
    import csv
    import io

    columns = table["columns"]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    for record in table["rows"]:
        writer.writerow([record.get(c, "") for c in columns])
    return buf.getvalue()


def read_attached_table(where: str | Path) -> dict | None:
    p = attached_table_path(where)
    if p is None:
        return None
    origin = Path(where) / ORIGIN_FILE
    if origin.is_file():
        name = origin.read_text(encoding="utf-8").strip() or "pasted"
    else:
        name = p.name
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


def bound_fields(row: dict) -> list[tuple[str, str]]:
    if row.get("mode") == "value":
        ents = row_entries(row)
        return [
            (
                f"[{e['key']}]" if e["key"]
                else ("value" if len(ents) == 1 else f"field {i + 1}"),
                e["column"],
            )
            for i, e in enumerate(ents)
        ]
    return [("path", column_of(row))]


def is_bound(row: dict) -> bool:
    fields = bound_fields(row)
    return bool(fields) and all(col for _label, col in fields)


def row_uniques(table: dict, rows: list[dict]) -> dict[str, int]:
    """How many distinct items each bound array row would register, counting
    duplicate values in its bound column(s) once rather than once per sheet
    row -- the estimate shown before a solve has actually run."""
    columns = set(table.get("columns") or [])
    out: dict[str, int] = {}
    for t in array_rows_of(rows):
        cols = [col for _label, col in bound_fields(t)]
        if not cols or any(c not in columns for c in cols):
            continue
        seen = {
            tuple((record.get(c) or "").strip() for c in cols)
            for record in table.get("rows") or []
        }
        seen.discard(tuple("" for _ in cols))
        out[str(t["id"])] = len(seen)
    return out


def unbound_problems(rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        if not isinstance(row, dict) or not (row.get("dtype") or "").strip():
            continue
        label = row_label(row)
        for field, col in bound_fields(row):
            if not col:
                out.append(_problem(str(row.get("id")), (
                    f"[{label}] has no column chosen for its {field}"
                )))
    return out


def row_label(row: dict) -> str:
    if row.get("mode") == "value":
        ents = row_entries(row)
        first = ents[0] if ents else {"key": "", "value": "", "column": ""}
        head = (first["value"] or "").strip().splitlines()
        text = head[0] if head else first["column"]
        if first["key"]:
            text = f"{first['key']}: {text}" if text else first["key"]
        if len(text) > 40:
            text = text[:40] + "\u2026"
    else:
        text = (row.get("path") or "").strip() or column_of(row)
    return text or str(row.get("id"))


def _array_parents(row: dict) -> list[str]:
    return [str(p)[1:] for p in (row.get("parents") or []) if str(p).startswith("#")]


def _plain_parents(row: dict) -> list[str]:
    return [str(p) for p in (row.get("parents") or []) if not str(p).startswith("#")]


def order_array_rows(array_rows: list[dict]) -> list[dict]:
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


def _problem(where: str, message: str) -> dict:
    return {"where": where, "message": message}


def array_rows_of(rows: list[dict]) -> list[dict]:
    return [
        r for r in rows
        if isinstance(r, dict) and r.get("id") is not None
        and (r.get("dtype") or "").strip() and is_bound(r)
    ]


def validate(library_path: str, table: dict, rows: list[dict]) -> dict:
    array_rows = array_rows_of(rows)
    problems: list[dict] = []
    if not array_rows:
        return {"problems": problems, "array_rows": []}

    columns = set(table.get("columns") or [])
    by_id = {str(t["id"]): t for t in array_rows}
    all_ids = {str(r["id"]) for r in rows if r.get("id") is not None}

    for t in array_rows:
        tid = str(t["id"])
        label = row_label(t)
        for field, col in bound_fields(t):
            if col not in columns:
                problems.append(_problem(tid, (
                    f"[{label}] names a column [{col}] in its {field}, which "
                    f"this table does not have"
                )))
        for p in _array_parents(t):
            if p not in all_ids:
                problems.append(_problem(tid, f"[{label}] descends from a row that is gone"))

    try:
        order_array_rows(array_rows)
    except AssertionError as exc:
        problems.append(_problem("lineage", str(exc)))
        return {"problems": problems, "array_rows": array_rows}

    problems += _path_problems(library_path, table, array_rows, by_id, rows)
    return {"problems": problems, "array_rows": array_rows}


def _path_problems(library_path, table, array_rows, by_id, rows) -> list[dict]:
    lib = load_data_lib(library_path)
    stored = read_record(library_path)
    previous = set(stored.get("paths", []))
    owned = {str(v) for v in (stored.get("rows") or {}).values()}
    existing = {str(p) for p in lib.manifest} - previous - owned
    problems: list[dict] = []
    minted: dict[str, tuple[str, str, int, str | None]] = {}
    columns = set(table.get("columns") or [])
    array_rows = [
        t for t in array_rows if all(c in columns for _f, c in bound_fields(t))
    ]

    for i, record in enumerate(table.get("rows") or []):
        for t in array_rows:
            tid = str(t["id"])
            label = row_label(t)
            fields = bound_fields(t)
            missing = [
                c for _label, c in fields if not (record.get(c) or "").strip()
            ]
            if missing:
                problems.append(_problem(tid, (
                    f"row {i + 1} of the table has nothing under "
                    f"{', '.join(sorted(set(missing)))}, which [{label}] needs"
                )))
                continue
            if t.get("mode") == "value":
                continue
            path = (record.get(dict(fields)["path"]) or "").strip()
            value = None
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
                    pass
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

    reachable = {str(p) for p in lib.manifest}
    for t in array_rows:
        for p in _plain_parents(t):
            if p not in reachable:
                problems.append(_problem(str(t["id"]), (
                    f"descends from [{p}], which is not registered"
                )))
    return problems


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
