from __future__ import annotations

import importlib.util
import shutil
import stat
import subprocess
from pathlib import Path

from ..agents.templates import library_index, standard_library_root
from ..constants import MODULE_PATH, STDLIB_NAME
from ..logging import Log

LIBRARY_STAMP = "LIBRARY_STAMP"

_COPY_IGNORE = shutil.ignore_patterns("__pycache__", "*.pyc", "_metadata", ".git")


def library_module_root() -> Path | None:
    return standard_library_root()


def _library_dirs(root: Path) -> tuple[list[str], list[str], list[str]]:
    def dirs(name: str) -> list[str]:
        d = root / name
        if not d.is_dir():
            return []
        return sorted(
            str(p) for p in d.iterdir()
            if p.is_dir() and not p.name.startswith((".", "_"))
        )
    types = [str(root / "data_types")] if (root / "data_types").is_dir() else []
    return types, dirs("transforms"), dirs("resources")


def compile_library(root: Path) -> dict:
    from ..ops import build as op_build

    root = Path(root)
    types, transforms, uniques = _library_dirs(root)
    if not types:
        raise FileNotFoundError(f"[{root}] has no data_types/ — not a library")
    return op_build.build_all(types, transforms, uniques)


def _library_version(root: Path) -> str:
    # The library has no version of its own any more: it ships inside this
    # package, so metasmith's version IS its version. A source checkout that
    # still carries a version.txt is honoured, for a library kept elsewhere.
    v = root / "version.txt"
    if v.is_file(): return v.read_text().strip()
    from ..constants import VERSION
    return VERSION


def _stamp(src: Path) -> str:
    from .._build_hash import compute_build_hash

    return f"{_library_version(src)}+{compute_build_hash(src)}"


def _make_writable(root: Path) -> None:
    for p in (root, *root.rglob("*")):
        try:
            p.chmod(p.stat().st_mode | stat.S_IWUSR)
        except OSError:
            pass


def _rebuild_stdlib(dest: Path) -> dict:
    """Copy and compile the standard library into a staging dir beside `dest`,
    then swap it in. Never touches `dest` on failure."""
    src = library_module_root()
    if src is None:
        err = (
            "this metasmith carries no standard library: neither a vendored "
            "bundle inside the package nor an importable metasmith_libraries. "
            "An installed metasmith should always have the former -- if this is "
            "a source checkout, point PYTHONPATH at its src/; if it is an "
            "install, it was built without `dev/metasmith.sh --vendor-library`."
        )
        Log.Error(err)
        return {"path": str(dest), "ok": False, "error": err}

    Log.Info(f"copying the standard library from [{src}]...")
    staging = dest.with_name(dest.name + ".partial")
    if staging.exists():
        _make_writable(staging)
        shutil.rmtree(staging)
    try:
        shutil.copytree(src, staging, ignore=_COPY_IGNORE)
        _make_writable(staging)
        Log.Info("compiling the standard library...")
        compile_library(staging)
        (staging / LIBRARY_STAMP).write_text(_stamp(src))
    except Exception as e:
        shutil.rmtree(staging, ignore_errors=True)
        err = f"could not build the standard library from [{src}]: {e}"
        Log.Error(err)
        return {"path": str(dest), "ok": False, "error": err}
    if dest.exists():
        _make_writable(dest)
        shutil.rmtree(dest)
    staging.rename(dest)
    return {"path": str(dest), "ok": True, "source": str(src)}


def clone_stdlib(root: Path) -> dict:
    dest = Path(root) / STDLIB_NAME
    if dest.exists():
        return {"path": str(dest), "cloned": False}
    out = _rebuild_stdlib(dest)
    result = {"path": out["path"], "cloned": out["ok"]}
    if "error" in out:
        result["error"] = out["error"]
    if "source" in out:
        result["source"] = out["source"]
    return result


def update_stdlib(root: Path) -> dict:
    """Force a fresh copy of the standard library over whatever is already
    cloned at `root`, then bust the caches and workflow copies built from the
    old one -- the everyday `clone_stdlib` skips entirely once a copy exists,
    which is right for bootstrapping a project but wrong for picking up
    changes to an installed or edited `metasmith_libraries`."""
    dest = Path(root) / STDLIB_NAME
    out = _rebuild_stdlib(dest)
    if out["ok"]:
        _TYPES_CACHE.clear()
        _INDEX_CACHE.clear()
    return {"path": out["path"], "updated": out["ok"]} | (
        {"error": out["error"]} if "error" in out else {}
    ) | ({"source": out["source"]} if "source" in out else {})


def stdlib_commit(root: Path) -> str | None:
    stamp = Path(root) / STDLIB_NAME / LIBRARY_STAMP
    try:
        return stamp.read_text().strip() or None
    except OSError:
        return None


def copy_example_resources(root: Path) -> dict:
    dest = Path(root) / "example_resources"
    if dest.exists():
        return {"path": str(dest), "copied": False}
    src = MODULE_PATH / "example_resources"
    if not src.exists():
        return {"path": str(dest), "copied": False}
    Log.Info("loading tutorials...")
    subprocess.run(["rsync", "-auP", f"{src}/", f"{dest}"], text=True)
    return {"path": str(dest), "copied": True}


def bootstrap_project(root: Path, with_examples: bool = True) -> dict:
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    out: dict = {"root": str(root)}
    if with_examples:
        out["examples"] = copy_example_resources(root)
    out["stdlib"] = clone_stdlib(root)
    return out


def discover(root: Path) -> dict:
    lib = (Path(root) / STDLIB_NAME).resolve()
    return {
        "path": str(lib),
        "present": lib.is_dir(),
        "commit": stdlib_commit(root),
    } | library_index(lib)


_TYPES_CACHE: dict[tuple, list[dict]] = {}


def available_types(root: Path, refresh: bool = False) -> list[dict]:
    from ..models.libraries import DataTypeLibrary

    found = discover(root)
    key = (str(Path(root).resolve()), found["commit"], tuple(found["data_types"]))
    if not refresh and key in _TYPES_CACHE:
        return _TYPES_CACHE[key]

    out: list[dict] = []
    for p in found["data_types"]:
        path = Path(p)
        namespace = path.stem
        try:
            lib = DataTypeLibrary.Load(path)
        except Exception as exc:
            out.append({"namespace": namespace, "path": p, "error": str(exc)})
            continue
        for name, endpoint in lib.types.items():
            out.append({
                "namespace": namespace,
                "name": name,
                "full_name": f"{namespace}::{name}",
                "path": p,
                "properties": sorted(endpoint.properties),
            })
    _TYPES_CACHE[key] = out
    return out


def resync_workflow_types(p: "Project") -> None:  # noqa: F821
    from ..logging import Log
    from ..ops import data as op_data

    found = discover(p.root)
    type_paths = found["data_types"]
    if not type_paths:
        return
    for wf in p.list_workflows(include_archived=False):
        lib_path = p.input_library_path(wf.name)
        if not lib_path.is_dir():
            continue
        try:
            op_data.resync_type_libraries(str(lib_path), type_paths)
        except Exception as exc:
            Log.Warn(f"could not resync types for workflow [{wf.name}]: {exc}")


_INDEX_CACHE: dict[tuple, dict] = {}

MATCH_EXACT = "exact"
MATCH_ALIAS = "alias"
MATCH_NARROWER = "narrower"
MATCH_BROADER = "broader"
_RANK = {MATCH_EXACT: 0, MATCH_ALIAS: 1, MATCH_NARROWER: 2, MATCH_BROADER: 2}


def _named_types(root: Path, found: dict, transform_libs: dict) -> dict[str, frozenset]:
    from ..models.libraries import DataTypeLibrary

    named: dict[str, frozenset] = {}
    for p in found["data_types"]:
        namespace = Path(p).stem
        try:
            lib = DataTypeLibrary.Load(Path(p))
        except Exception as exc:
            Log.Error(f"could not read type library [{p}]: {exc}")
            continue
        for name, endpoint in lib.types.items():
            named.setdefault(f"{namespace}::{name}", frozenset(endpoint.properties))
    for lib in transform_libs.values():
        for namespace, dtlib in lib.types.items():
            if namespace == "transforms":
                continue
            for name, endpoint in dtlib.types.items():
                named.setdefault(f"{namespace}::{name}", frozenset(endpoint.properties))
    return named


def _slot_index(dep, slots: list) -> int | None:
    for i, s in enumerate(slots):
        if s is dep:
            return i
    for i, s in enumerate(slots):
        if s == dep:
            return i
    return None


def _slots(slots: list, lib) -> list[dict]:
    from ..ops._common import dep_info

    out = []
    for d in slots:
        parents = []
        for p in getattr(d, "parents", None) or ():
            i = _slot_index(p, slots)
            if i is not None and i not in parents:
                parents.append(i)
        out.append({"as": dep_info(d, lib).get("type"), "parents": sorted(parents)})
    return out


def type_index(root: Path, refresh: bool = False) -> dict:
    from ..ops._common import dep_info, load_transform_lib

    found = discover(root)
    key = (
        str(Path(root).resolve()), found["commit"],
        tuple(found["transform_libraries"]), tuple(found["data_types"]),
    )
    if not refresh and key in _INDEX_CACHE:
        return _INDEX_CACHE[key]

    libraries: list[dict] = []
    transforms: list[dict] = []
    loaded_libs: dict[str, object] = {}
    declared: list[dict[str, list[tuple]]] = []

    for path in found["transform_libraries"]:
        entry = {"path": path, "name": Path(path).name, "transform_count": 0}
        libraries.append(entry)
        try:
            lib = load_transform_lib(path)
            loaded = list(lib.IterateTransforms())
        except Exception as exc:
            entry["error"] = str(exc)
            Log.Error(f"could not index transform library [{path}]: {exc}")
            continue
        loaded_libs[path] = lib
        for tr_path, tr in loaded:
            def _resolve(deps) -> list[tuple]:
                out = []
                for d in deps:
                    name = dep_info(d, lib).get("type")
                    props = frozenset(d.properties)
                    if (name, props) not in out:
                        out.append((name, props))
                return out

            slots = list(tr.model.requires)
            requires = _resolve(tr.model.requires)
            produces = _resolve([d for group in tr.model.produces for d in group])
            transforms.append({
                "name": tr.name,
                "path": str(tr_path),
                "library": path,
                "library_name": entry["name"],
                "inputs": [n for n, _ in requires if n],
                "outputs": [n for n, _ in produces if n],
                "requires": _slots(slots, lib),
                "group_by": dep_info(tr.group_by, lib).get("type") if tr.group_by else None,
            })
            declared.append({"requires": requires, "produces": produces})
            entry["transform_count"] += 1

    named = _named_types(root, found, loaded_libs)
    for spec in declared:
        for side in ("requires", "produces"):
            for name, props in spec[side]:
                if name:
                    named.setdefault(name, props)

    by_type: dict[str, dict] = {}
    for tname, tprops in named.items():
        produced: list[dict] = []
        consumed: list[dict] = []
        for idx, spec in enumerate(declared):
            best = _best_match(tname, tprops, spec["produces"], produced_side=True)
            if best:
                produced.append({"i": idx, **best})
            best = _best_match(tname, tprops, spec["requires"], produced_side=False)
            if best:
                consumed.append({"i": idx, **best})
        by_type[tname] = {
            "produced_by": sorted(produced, key=lambda e: (_RANK[e["match"]], e["i"])),
            "consumed_by": sorted(consumed, key=lambda e: (_RANK[e["match"]], e["i"])),
        }

    index = {
        "commit": found["commit"],
        "libraries": libraries,
        "transforms": transforms,
        "by_type": by_type,
    }
    _INDEX_CACHE[key] = index
    return index


def _best_match(
    tname: str, tprops: frozenset, deps: list[tuple], produced_side: bool,
) -> dict | None:
    best: dict | None = None
    for name, props in deps:
        if produced_side:
            if not tprops.issubset(props):
                continue
        else:
            if not props.issubset(tprops):
                continue
        if name == tname:
            match = MATCH_EXACT
        elif props == tprops:
            match = MATCH_ALIAS
        else:
            match = MATCH_NARROWER if produced_side else MATCH_BROADER
        if best is None or _RANK[match] < _RANK[best["match"]]:
            best = {"as": name, "match": match}
        if best["match"] == MATCH_EXACT:
            break
    return best
