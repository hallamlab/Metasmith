"""The standard library clone, and what a project bootstrap consists of.

`msm lab` and `msm gui` open the same working directory and need the same things
in it: the bundled example resources, and a clone of the standard library. That
bootstrap lives here so the two front ends share it rather than drifting apart.

The library is pulled from main with no configuration -- there is deliberately no
UI for it and no per-project pin. Everything it contains is offered to the
planner, so adding a transform library to the repository is enough to make it
available.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

from ..constants import MODULE_PATH, STDLIB_NAME, STDLIB_URL
from ..logging import Log

DATA_TYPES_DIRNAME = "data_types"
TRANSFORMS_DIRNAME = "transforms"
RESOURCES_DIRNAME = "resources"


def clone_stdlib(root: Path, url: str = STDLIB_URL) -> dict:
    """Clone the standard library into `root` if it is not already there.

    A failed clone is reported, not raised: a user without network access should
    still get a notebook or a page, with the absence stated plainly rather than a
    traceback at startup. Callers surface `error` in the UI.
    """
    dest = Path(root) / STDLIB_NAME
    if dest.exists():
        return {"path": str(dest), "cloned": False}
    Log.Info(f"downloading standard library from [{url}]...")
    res = subprocess.run(
        ["git", "clone", "--depth", "1", url, str(dest)],
        text=True, capture_output=True,
    )
    if res.returncode != 0:
        err = res.stderr.strip() or f"git clone exited {res.returncode}"
        Log.Error(f"failed to clone [{url}]: {err}")
        return {"path": str(dest), "cloned": False, "error": err}
    return {"path": str(dest), "cloned": True}


def stdlib_commit(root: Path) -> str | None:
    """The commit the clone is on, recorded with each plan so a result is traceable."""
    dest = Path(root) / STDLIB_NAME
    if not (dest / ".git").exists():
        return None
    res = subprocess.run(
        ["git", "-C", str(dest), "rev-parse", "HEAD"], text=True, capture_output=True,
    )
    return res.stdout.strip() or None if res.returncode == 0 else None


def copy_example_resources(root: Path) -> dict:
    """Copy the bundled tutorials/transforms into the project, once."""
    dest = Path(root) / "example_resources"
    if dest.exists():
        return {"path": str(dest), "copied": False}
    src = MODULE_PATH / "example_resources"
    if not src.exists():
        return {"path": str(dest), "copied": False}
    Log.Info("loading tutorials...")
    subprocess.run(["rsync", "-auP", f"{src}/", f"{dest}"], text=True)
    return {"path": str(dest), "copied": True}


def bootstrap_project(root: Path, with_examples: bool = True, url: str = STDLIB_URL) -> dict:
    """Everything a fresh working directory needs before either front end opens."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    out: dict = {"root": str(root)}
    if with_examples:
        out["examples"] = copy_example_resources(root)
    out["stdlib"] = clone_stdlib(root, url)
    return out


# -- discovery ---------------------------------------------------------------
#
# The repository is a plain directory tree; these are the three shapes in it.


def _dirs(path: Path) -> list[Path]:
    if not path.is_dir():
        return []
    return sorted(
        p for p in path.iterdir()
        if p.is_dir() and not p.name.startswith((".", "__"))
    )


def discover(root: Path) -> dict:
    """List the type, transform, and resource libraries in the clone.

    Resolved: `MetasmithLibraries` is sometimes a symlink -- someone iterating
    on a shared stdlib checkout across several projects, say -- and
    `Template.Load`/`Spec.Unpack` already resolve a template's own root before
    joining its relative library references onto it (`templates.py`,
    `spec.py`). Leaving this one unresolved meant the two sides named the same
    library by two different strings -- the symlink path here, its real
    target there -- so a workflow created from a template could never match
    its own libraries against this list, and every one of them read as
    disabled with no error to say why.
    """
    lib = (Path(root) / STDLIB_NAME).resolve()
    types = []
    tdir = lib / DATA_TYPES_DIRNAME
    if tdir.is_dir():
        types = sorted(str(p) for p in tdir.glob("*.yml"))
    return {
        "path": str(lib),
        "present": lib.is_dir(),
        "commit": stdlib_commit(root),
        "data_types": types,
        "transform_libraries": [str(p) for p in _dirs(lib / TRANSFORMS_DIRNAME)],
        "resource_libraries": [str(p) for p in _dirs(lib / RESOURCES_DIRNAME)],
    }


def available_types(root: Path) -> list[dict]:
    """Every data type in the standard library, namespaced by its file stem."""
    from ..models.libraries import DataTypeLibrary

    out: list[dict] = []
    for p in discover(root)["data_types"]:
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
    return out


def resync_workflow_types(p: "Project") -> None:  # noqa: F821
    """Bring every workflow's input library's types up to date with the stdlib.

    A workflow's input library is handed its type namespaces once, at
    creation (see `create_workflow` in `gui/api.py`) -- nothing keeps that in
    step with `MetasmithLibraries` afterwards. Run at startup rather than per
    solve: a type added to an existing namespace after a workflow was made
    would otherwise leave that workflow permanently unable to use it, with no
    gesture short of hand-editing its library able to fix it. Best-effort and
    non-fatal per workflow, same posture as `warm_type_index` /
    `warm_template_dags` alongside which this runs.
    """
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


# -- the type index ----------------------------------------------------------
#
# Which transforms consume and produce each type. The page needs this to answer
# "what can I do with this?" while a type is being chosen, and a round trip per
# keystroke is not an option -- so it is built once, for *every* library found,
# and the browser filters it by whichever libraries are enabled. That also means
# toggling a library is instant and never refetches.

_INDEX_CACHE: dict[tuple, dict] = {}

# How a transform's *declared* type relates to the type being asked about. The
# solver matches on properties, never on names (`Node.IsA`: x.IsA(y) iff y's
# properties are a subset of x's), so name equality is only the easiest of four
# ways a transform can turn out to be relevant.
MATCH_EXACT = "exact"        # the same type
MATCH_ALIAS = "alias"        # same properties under another name
MATCH_NARROWER = "narrower"  # declared type is more specific than the one asked about
MATCH_BROADER = "broader"    # declared type is more general
_RANK = {MATCH_EXACT: 0, MATCH_ALIAS: 1, MATCH_NARROWER: 2, MATCH_BROADER: 2}


def _named_types(root: Path, found: dict, transform_libs: dict) -> dict[str, frozenset]:
    """Every type that has a name, and the property set behind it.

    Two sources, because a type can be named in either: the standard library's
    own type files, and the `_metadata/types/` copy each transform library
    carries. The same properties are routinely named in both -- that is what an
    `alias` match is -- so the first name found wins and the other is kept as a
    separate key rather than collapsed away.
    """
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
            # the same namespace `dep_info` skips: it holds the transforms' own
            # signatures, not data types a user could ever register
            if namespace == "transforms":
                continue
            for name, endpoint in dtlib.types.items():
                named.setdefault(f"{namespace}::{name}", frozenset(endpoint.properties))
    return named


def _slot_index(dep, slots: list) -> int | None:
    """Which requirement slot a parent reference points at.

    Identity first, and this is not fussiness: `Dependency` inherits `Node`'s
    equality, which compares property signatures, so two slots of the same type
    are equal to each other and `==` would answer with whichever came first.
    `d.parents` is a set built from the very objects in `requires`, so identity
    is the answer in every case a transform declared; equality is the fallback
    for a parent rebuilt rather than referenced.
    """
    for i, s in enumerate(slots):
        if s is dep:
            return i
    for i, s in enumerate(slots):
        if s == dep:
            return i
    return None


def _slots(slots: list, lib) -> list[dict]:
    """One entry per requirement slot, in declaration order, with its lineage.

    `parents` holds positions in this same list -- the constraint the transform
    declared, that this input must descend from that one. Plumbing slots
    (container images, bundled scripts) stay in the list so the positions are the
    transform's own; the consumer drops them by namespace as it already does
    elsewhere. `as` is null for a slot whose properties no type file names.
    """
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
    """A map of type -> the transforms on either side of it.

    Transforms are listed once, in `transforms`; `by_type` holds entries of
    `{i, as, match}` pointing into that list, so a transform taking five types
    is still stored once.

    Matching is by property set, not by name -- the same `IsA` the solver uses.
    A transform requiring `sequences::assembly` does take a
    `sequences::flye_assembly`, and a target of `sequences::assembly` is
    satisfied by megahit's narrower product; keying on names alone said
    "nothing takes this" and "nothing can make this" to both. Each entry
    carries the name the transform actually declared (`as`) and how it relates
    (`match`), because "takes it as something more general" is a different
    thing to know than "takes exactly this".

    CAUTION: loading a transform library imports every transform in it, through
    the same process-global path the planner uses (see `_plan_lock` in api.py).
    Callers must hold that lock.
    """
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
    # (declared name | None, properties) per side, parallel to `transforms`
    declared: list[dict[str, list[tuple]]] = []

    for path in found["transform_libraries"]:
        entry = {"path": path, "name": Path(path).name, "transform_count": 0}
        libraries.append(entry)
        try:
            lib = load_transform_lib(path)
            loaded = list(lib.IterateTransforms())
        except Exception as exc:
            # one unloadable library must not blank the whole panel
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
                # what the transform itself says, for the "also needs" lines
                "inputs": [n for n, _ in requires if n],
                "outputs": [n for n, _ in produces if n],
                # ...and the same requirements *unflattened*: one entry per slot,
                # carrying the lineage declared between them. `inputs` cannot say
                # this -- it de-dupes, so a slot has no stable position in it.
                "requires": _slots(slots, lib),
                "group_by": dep_info(tr.group_by, lib).get("type") if tr.group_by else None,
            })
            declared.append({"requires": requires, "produces": produces})
            entry["transform_count"] += 1

    named = _named_types(root, found, loaded_libs)
    # a transform may name a type no type file exports; it is still a key
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
            # a transform producing P can stand in for T when P.IsA(T)
            best = _best_match(tname, tprops, spec["produces"], produced_side=True)
            if best:
                produced.append({"i": idx, **best})
            # T can be fed to a transform requiring R when T.IsA(R)
            best = _best_match(tname, tprops, spec["requires"], produced_side=False)
            if best:
                consumed.append({"i": idx, **best})
        by_type[tname] = {
            # direct matches first: they are what someone scanning the list wants
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
    """The strongest relation between one type and one transform's deps on a side.

    A transform can declare several types that all match -- it may require both
    an assembly and a specific flavour of one -- and it belongs in the list once,
    under the closest of them. `None` when nothing on this side matches at all.
    """
    best: dict | None = None
    for name, props in deps:
        if produced_side:
            # P.IsA(T): the product is at least as specific as the type asked about
            if not tprops.issubset(props):
                continue
        else:
            # T.IsA(R): the type asked about is at least as specific as the need
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
