"""The standard library snapshot, and what a project bootstrap consists of.

`msm lab` and `msm gui` open the same working directory and need the same things
in it: the bundled example resources, and a working copy of the standard
library.  That bootstrap lives here so the two front ends share it rather than
drifting apart.

There is deliberately no UI for it and no per-project pin. Everything the
library contains is offered to the planner, so adding a transform library to
the repository is enough to make it available.

**The library is an installed module, and the project gets a compiled copy of
it.** `metasmith_libraries` ships as its own distribution; this module locates
that package, copies it into the project, and compiles the copy in place.
Three things follow, and each replaces a failure mode the older
git-pull/vendored-bundle path had:

* The install directory is treated as read-only — a site-packages or conda
  `pkgs` tree may genuinely be, and compiling there would write build products
  into a shared install that other projects also read.
* `_metadata/` is a build product, so the copy is stripped of any it inherited
  and rebuilt here. Nothing shipped needs to carry compiled metadata, which
  is what retires the compile-before-copy ordering that used to be enforced by
  a guard in the release script.
* Materialisation is atomic: the copy is built under a `.partial` name and
  renamed only once its metadata compiles. A library directory that exists is
  therefore a library that resolves — the half-materialised state that would
  otherwise persist across restarts, silently, cannot be reached.
"""
from __future__ import annotations

import importlib.util
import shutil
import stat
import subprocess
from pathlib import Path

from ..agents.templates import library_index
from ..constants import MODULE_PATH, STDLIB_NAME
from ..logging import Log

# Written into the materialised copy; `stdlib_commit` reads it back. The GUI
# keys its type index and its cached template drawings on that value, so it
# has to change whenever the library's content does and cost nothing to read.
LIBRARY_STAMP = "LIBRARY_STAMP"

# `_metadata/` is rebuilt here, never inherited; the rest is Python build litter.
_COPY_IGNORE = shutil.ignore_patterns("__pycache__", "*.pyc", "_metadata", ".git")


def library_module_root() -> Path | None:
    """The installed `metasmith_libraries` package directory, or None.

    Resolved by import rather than by path so one arm covers both worlds: a
    conda/pip install finds it in site-packages, and a source checkout run
    with `PYTHONPATH=src` finds `src/metasmith_libraries` — the same directory
    the authoring tools edit. Shape is checked, not just importability, since
    a namespace package with no `data_types/` is not a library.
    """
    try:
        spec = importlib.util.find_spec("metasmith_libraries")
    except (ImportError, ValueError):
        return None
    if spec is None or not spec.origin:
        return None
    root = Path(spec.origin).resolve().parent
    return root if (root / "data_types").is_dir() else None


def _library_dirs(root: Path) -> tuple[list[str], list[str], list[str]]:
    """The three argument lists a compile takes, read off the library layout."""
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
    """Compile `_metadata/` for the library at `root`, in place.

    The same build `dev/libraries.sh -bm` runs, called in-process. A library
    with no metadata does not degrade to resolving fewer types — it raises
    before planning begins — so this is what makes a fresh copy usable.

    CAUTION: this imports every transform in the library, through the same
    process-global path the planner uses. Callers inside a running GUI must
    hold `_plan_lock` (see `gui/api.py`); the bootstrap runs before serving.
    """
    from ..ops import build as op_build

    root = Path(root)
    types, transforms, uniques = _library_dirs(root)
    if not types:
        raise FileNotFoundError(f"[{root}] has no data_types/ — not a library")
    return op_build.build_all(types, transforms, uniques)


def _library_version(root: Path) -> str:
    v = root / "version.txt"
    return v.read_text().strip() if v.is_file() else "unknown"


def _stamp(src: Path) -> str:
    """Version plus a content hash of the source the copy was made from.

    Two libraries of the same version are not necessarily the same library —
    a dev checkout changes under a fixed `version.txt` all day — so the hash
    is what actually keys the caches. Computed once here rather than on every
    `discover()`, which the template list calls per request.
    """
    from .._build_hash import compute_build_hash

    return f"{_library_version(src)}+{compute_build_hash(src)}"


def _make_writable(root: Path) -> None:
    """Give the owner write permission over the whole copy.

    `copytree` preserves the source's mode bits, and an install directory is
    routinely read-only -- conda's `pkgs` cache hardlinks its files in as
    read-only, and a system-wide site-packages is not the user's to write. The
    copy inherits that, and the compile then fails partway through trying to
    create `_metadata/` inside it. Applied to the copy, never to the source,
    which is the reason there is a copy at all.
    """
    for p in (root, *root.rglob("*")):
        try:
            p.chmod(p.stat().st_mode | stat.S_IWUSR)
        except OSError:
            pass  # not ours to chmod; the compile will say so if it mattered


def clone_stdlib(root: Path) -> dict:
    """Materialize the standard library into `root` if it is not already there.

    A failure is reported, not raised: a user whose install is missing the
    library package should still get a notebook or a page, with the absence
    stated in the UI rather than a traceback at startup.
    """
    dest = Path(root) / STDLIB_NAME
    if dest.exists():
        return {"path": str(dest), "cloned": False}

    src = library_module_root()
    if src is None:
        err = (
            "the metasmith_libraries package is not installed, so there is no "
            "standard library to copy. Install it (`conda install -c hallamlab "
            "metasmith_libraries`), or run from a source checkout with "
            "PYTHONPATH pointed at its src/."
        )
        Log.Error(err)
        return {"path": str(dest), "cloned": False, "error": err}

    Log.Info(f"copying the standard library from [{src}]...")
    staging = dest.with_name(dest.name + ".partial")
    if staging.exists():
        # left by an attempt that died between the copy and the rename. It may
        # be read-only, having inherited the install's mode bits, so it is made
        # writable before being removed rather than after -- otherwise a single
        # failed bootstrap wedges every later one.
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
        return {"path": str(dest), "cloned": False, "error": err}
    staging.rename(dest)
    return {"path": str(dest), "cloned": True, "source": str(src)}


def stdlib_commit(root: Path) -> str | None:
    """What version of the library this project holds, recorded with each plan.

    Named for the git commit it used to be, because that is what it still is
    to every caller: an opaque token that changes when the library does.
    """
    stamp = Path(root) / STDLIB_NAME / LIBRARY_STAMP
    try:
        return stamp.read_text().strip() or None
    except OSError:
        return None


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


def bootstrap_project(root: Path, with_examples: bool = True) -> dict:
    """Everything a fresh working directory needs before either front end opens."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    out: dict = {"root": str(root)}
    if with_examples:
        out["examples"] = copy_example_resources(root)
    out["stdlib"] = clone_stdlib(root)
    return out


# -- discovery ---------------------------------------------------------------


def discover(root: Path) -> dict:
    """What the copy holds: `library_index` plus the stamp it was built under.

    The three library lists are the repository convention, which
    `agents/templates.py` owns because a template is read against it -- both
    a library-shipped one, resolved against the repository it ships in, and a
    user's, resolved against this list.

    Resolved: `MetasmithLibraries` is sometimes a symlink -- someone pointing
    several projects at one built copy, say -- and
    `Template.Load`/`Spec.Unpack` already resolve a template's own root before
    joining its relative library references onto it (`templates.py`,
    `spec.py`). Leaving this one unresolved meant the two sides named the same
    library by two different strings -- the symlink path here, its real
    target there -- so a workflow created from a template could never match
    its own libraries against this list, and every one of them read as
    disabled with no error to say why.
    """
    lib = (Path(root) / STDLIB_NAME).resolve()
    return {
        "path": str(lib),
        "present": lib.is_dir(),
        "commit": stdlib_commit(root),
    } | library_index(lib)


_TYPES_CACHE: dict[tuple, list[dict]] = {}


def available_types(root: Path, refresh: bool = False) -> list[dict]:
    """Every data type in the standard library, namespaced by its file stem.

    Cached the same way `type_index` below is, keyed on what would change the
    answer -- unlike that function, this one never imports a transform, so it
    needs none of `_plan_lock`.
    """
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
