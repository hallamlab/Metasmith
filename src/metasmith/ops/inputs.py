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
from .rows import column_of, entries, render_value, scalar  # noqa: F401  (re-exported)


MAX_VALUE_BYTES = 64 * 1024


def read_value(library_path: str | Path, item_path: str | Path | None) -> str | None:
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
    direct = [pm.path for pm in lib.parents.get(path, [])]
    inherited = {gp.path for d in direct for gp in lib.parents.get(d, [])}
    return [d for d in direct if d not in inherited]


def row_key(row: dict) -> str:
    return f"#{row.get('id')}"


def parent_ids(row: dict) -> list[str]:
    return [str(p)[1:] for p in (row.get("parents") or []) if str(p).startswith("#")]


def literal_parents(row: dict) -> list[str]:
    return [str(p) for p in (row.get("parents") or []) if not str(p).startswith("#")]


def mint_value_path() -> str:
    return uuid.uuid4().hex


def row_identity(row: dict) -> str:
    if row.get("mode") == "value":
        return (row.get("name") or "").strip()
    return (row.get("path") or "").strip()


def registerable(row: dict) -> bool:
    return bool((row.get("dtype") or "").strip())


def problems(rows: list[dict], table: dict | None = None) -> list[str]:
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


def _desired(rows: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for r in rows:
        if not registerable(r):
            continue
        rid = str(r["id"])
        dtype = (r.get("dtype") or "").strip()
        if r.get("mode") == "value":
            out[rid] = {
                "rid": rid, "dtype": dtype, "mode": "value",
                "path": None,
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


def _group_key(row: dict, record: dict) -> str:
    cols = sorted({e["column"] for e in entries(row) if e["column"]})
    return json.dumps([[c, record.get(c)] for c in cols], separators=(",", ":"))


def _array_plan(
    table: dict, array_rows: list[dict], prior_minted: dict | None = None,
) -> list[dict]:
    if not array_rows:
        return [], {}, {}
    order = op_samples.order_array_rows(array_rows)
    by_id = {str(t["id"]): t for t in array_rows}
    plan: list[dict] = []
    seen: dict[str, dict] = {}
    generated: dict[str, list[str]] = {str(t["id"]): [] for t in array_rows}
    prior_minted = prior_minted or {}
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


def sync(
    library_path: str,
    rows: list[dict],
    table: dict | None = None,
    on_progress=None,
) -> dict:
    lib = load_data_lib(library_path)
    record = op_samples.read_record(library_path)
    prior_rows = {str(k): str(v) for k, v in (record.get("rows") or {}).items()}
    prior_generated = {
        str(k): [str(x) for x in v] for k, v in (record.get("generated") or {}).items()
    }
    prior_array = {p for ps in prior_generated.values() for p in ps}

    rows = [dict(r) for r in rows if isinstance(r, dict) and r.get("id") is not None]
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

    if doomed:
        changed = True
        gone = set(doomed)
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

    for rid, spec in want.items():
        at = held.get(rid)
        if at is None:
            continue
        if spec["mode"] == "value":
            if at.is_absolute():
                op_data.remove_item(library_path, str(at), save=False, lib=lib)
                del held[rid]
                changed = True
            continue
        to = spec["path"]
        if to is None:
            if is_deferred(at):
                continue
            op_data.remove_item(library_path, str(at), save=False, lib=lib)
            del held[rid]
            changed = True
            continue
        if to == at:
            continue
        if at.is_absolute() != to.is_absolute():
            op_data.remove_item(library_path, str(at), save=False, lib=lib)
            del held[rid]
        else:
            op_data.repoint_item(library_path, str(at), str(to), save=False, lib=lib)
            held[rid] = to
        changed = True

    for rid, spec in want.items():
        at = held.get(rid)
        if at is None or lib.manifest.get(at) == spec["dtype"]:
            continue
        op_data.retype_item(library_path, str(at), spec["dtype"], save=False, lib=lib)
        changed = True

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
        "minted": minted,
        "row_count": len(table.get("rows") or []) if table is not None else 0,
        "columns": list(table.get("columns") or []) if table is not None else [],
    }

    if changed:
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
    out: list[Path] = []
    for k in keys:
        p = held.get(k[1:]) if k.startswith("#") else Path(k)
        if p is None or p not in lib.manifest or p in out:
            continue
        out.append(p)
    return out


def _problems(lib, want, held, plan, rows, table, array_rows, doomed) -> list[str]:
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
        if (
            spec["mode"] != "value"
            and spec["path"] is not None
            and not spec["path"].is_absolute()
            and held.get(spec["rid"]) != spec["path"]
        ):
            problems.append(
                f"[{spec['path']}] is a relative path, and a file row points at a "
                f"file of yours -- give it an absolute path, or make it a value "
                f"row for something the library should hold"
            )

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
            if want.get(owner[p], {}).get("path") != held.get(rid):
                problems.append(f"[{p}] is already registered here")

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


def _adopted_id(path: str) -> str:
    return "i" + hashlib.md5(path.encode("utf-8")).hexdigest()[:10]


def _adopted_entries(value: str) -> list[dict]:
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
            row = {"id": rid, "mode": "value", "path": "", "name": "",
                   "values": _adopted_entries(value), "dtype": dtype, "parents": []}
        else:
            row = {"id": rid, "mode": "file", "path": s, "name": "",
                   "values": [{"key": "", "value": ""}], "dtype": dtype, "parents": []}
        made.append(row)
        made_ids.add(rid)
        prior[rid] = s
        by_path[s] = rid

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
