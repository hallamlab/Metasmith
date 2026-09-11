from __future__ import annotations

import os
import shutil
import tempfile
import time
from pathlib import Path

from ..hashing import KeyGenerator
from ..models.libraries import DataInstanceLibrary
from ..models.paths import DEFERRED
from ..models.remote import Source
from ._common import load_data_lib


def inspect_library(library_path: str) -> dict:
    lib = load_data_lib(library_path)
    items = [{"path": str(p), "type_name": dtype_name} for p, dtype_name, _ep in lib.Iterate()]
    return {
        "path": str(library_path),
        "schema": lib.schema,
        "type_namespaces": list(lib.types.keys()),
        "item_count": len(lib.manifest),
        "items": items,
    }


def list_items(library_path: str, type_filter: str | None = None) -> list[dict]:
    lib = load_data_lib(library_path)
    results = []
    for p, dtype_name, _ep in lib.Iterate():
        if type_filter and dtype_name != type_filter:
            continue
        results.append({"path": str(p), "type_name": dtype_name})
    return results


def create_library(
    path: str,
    type_library_paths: list[str] | None = None,
    purge: bool = False,
) -> dict:
    p = Path(path).resolve()
    lib = DataInstanceLibrary(p)
    if purge:
        lib.Purge()
    for tp in type_library_paths or []:
        lib.AddTypeLibrary(Path(tp).resolve())
    lib.Save()
    return {"path": str(p), "type_namespaces": list(lib.types.keys())}


def _link_or_copy(src, dst, *, follow_symlinks=True):
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst, follow_symlinks=follow_symlinks)


def fork_library(
    library_path: str,
    dest_path: str,
    fork_id: str | None = None,
) -> dict:
    src = Path(library_path).resolve()
    dest = Path(dest_path).resolve()
    assert src.is_dir(), f"library [{src}] does not exist"
    assert src != dest, "fork destination must differ from the source"
    assert not dest.exists() or not any(dest.iterdir()), (
        f"fork destination [{dest}] already exists and is not empty"
    )
    load_data_lib(src)
    shutil.copytree(src, dest, symlinks=True, copy_function=_link_or_copy, dirs_exist_ok=True)

    lib = DataInstanceLibrary.Load(dest)
    lib.fork_id = fork_id or KeyGenerator().GenerateUID(l=8)
    lib.Save()
    lib._calculate_key()
    return {
        "library": str(dest),
        "forked_from": str(src),
        "fork_id": lib.fork_id,
        "key": lib.GetKey(),
    }


def copy_library(
    library_path: str,
    dest_path: str,
    type_library_paths: list[str] | None = None,
) -> dict:
    src = Path(library_path).resolve()
    dest = Path(dest_path).resolve()
    assert src.is_dir(), f"library [{src}] does not exist"
    assert src != dest, "copy destination must differ from the source"
    assert not dest.exists() or not any(dest.iterdir()), (
        f"copy destination [{dest}] already exists and is not empty"
    )
    load_data_lib(src)
    shutil.copytree(src, dest, symlinks=True, copy_function=_link_or_copy, dirs_exist_ok=True)

    lib = DataInstanceLibrary.Load(dest)
    for tp in type_library_paths or []:
        lib.AddTypeLibrary(Path(tp).resolve(), on_exist="skip")
    lib.Save()
    return {
        "library": str(dest),
        "copied_from": str(src),
        "type_namespaces": list(lib.types.keys()),
        "key": lib.GetKey(),
    }


def materialize_template(
    inline: dict,
    dest_path: str,
    type_library_paths: list[str] | None = None,
) -> dict:
    dest = Path(dest_path).resolve()
    assert not dest.exists() or not any(dest.iterdir()), (
        f"copy destination [{dest}] already exists and is not empty"
    )
    lib = DataInstanceLibrary.FromInline(inline, dest)
    for tp in type_library_paths or []:
        lib.AddTypeLibrary(Path(tp).resolve(), on_exist="skip")
    lib.Save()
    return {
        "library": str(dest),
        "type_namespaces": list(lib.types.keys()),
        "key": lib.GetKey(),
    }


def derive_template_library(
    library_path: str,
    type_library_paths: list[str] | None = None,
) -> DataInstanceLibrary:
    src = load_data_lib(library_path)
    dest = Path(tempfile.mkdtemp(prefix="msm-save-template-"))
    lib = DataInstanceLibrary(dest)
    for ns, source_path in src._type_sources.items():
        lib.AddTypeLibrary(source_path, namespace=ns, on_exist="skip")
    for tp in type_library_paths or []:
        lib.AddTypeLibrary(Path(tp).resolve(), on_exist="skip")

    mapping: dict[Path, Path] = {}
    remaining = dict(src.manifest)
    while remaining:
        progressed = False
        for path, dtype in list(remaining.items()):
            parent_paths = [
                pm.path for pm in src.parents.get(path, [])
                if pm.path in src.manifest
            ]
            if not all(pp in mapping for pp in parent_paths):
                continue
            new_path = lib.AddItem(DEFERRED, dtype, parents=[mapping[pp] for pp in parent_paths])
            mapping[path] = new_path
            del remaining[path]
            progressed = True
        assert progressed, (
            f"could not derive a template from [{library_path}]: a cyclic or "
            f"cross-library parent reference left {len(remaining)} item(s) unplaced"
        )
    lib.Save()
    return lib


def attach_type_library(
    library_path: str,
    type_library_path: str,
    namespace: str | None = None,
    on_exist: str = "skip",
) -> dict:
    lib = load_data_lib(library_path)
    lib.AddTypeLibrary(Path(type_library_path).resolve(), namespace=namespace, on_exist=on_exist)
    lib.Save()
    return {"library": str(library_path), "type_namespaces": list(lib.types.keys())}


def resync_type_libraries(library_path: str, type_library_paths: list[str]) -> dict:
    lib = load_data_lib(library_path)
    for tp in type_library_paths:
        lib.AddTypeLibrary(Path(tp).resolve(), on_exist="overwrite")
    lib.Save()
    return {"library": str(library_path), "type_namespaces": list(lib.types.keys())}


def _lib_for(library_path, lib: DataInstanceLibrary | None):
    return load_data_lib(library_path) if lib is None else lib


def add_item(
    library_path: str,
    host_path: str,
    dtype: str,
    parents: list[str] | None = None,
    save: bool = True,
    lib: DataInstanceLibrary | None = None,
) -> dict:
    lib = _lib_for(library_path, lib)
    parent_paths = [Path(p) for p in (parents or [])]
    path = DEFERRED if host_path is DEFERRED or host_path == str(DEFERRED) else Path(host_path)
    rec_path = lib.AddItem(path, dtype, parents=parent_paths)
    if save:
        lib.Save()
    return {"library": str(library_path), "path": str(rec_path), "dtype": dtype}


def add_value(
    library_path: str,
    name: str,
    value,
    dtype: str,
    parents: list[str] | None = None,
    save: bool = True,
    lib: DataInstanceLibrary | None = None,
) -> dict:
    lib = _lib_for(library_path, lib)
    parent_paths = [Path(p) for p in (parents or [])]
    rec_path = lib.AddValue(name, value, dtype, parents=parent_paths)
    if save:
        lib.Save()
    return {"library": str(library_path), "path": str(rec_path), "dtype": dtype, "value": value}


def _ancestors_of(lib, start: Path) -> set[Path]:
    seen: set[Path] = set()
    queue = [start]
    while queue:
        for meta in lib.parents.get(queue.pop(), []):
            if meta.path in seen:
                continue
            seen.add(meta.path)
            queue.append(meta.path)
    return seen


def _assert_acyclic(lib, item: Path, parent_paths: list[str]):
    for raw in parent_paths:
        p = Path(raw)
        assert p != item, f"[{item}] cannot descend from itself"
        assert item not in _ancestors_of(lib, p), (
            f"[{item}] cannot descend from [{p}]: that already descends from this one"
        )


def set_item_parents(
    library_path: str,
    item_path: str,
    parent_paths: list[str],
    save: bool = True,
) -> dict:
    lib = load_data_lib(library_path)
    _assert_acyclic(lib, Path(item_path), parent_paths)
    parents = [lib.Get(Path(p)) for p in parent_paths]
    lib.AddParentsTo(Path(item_path), parents)
    if save:
        lib.Save()
    return {"library": str(library_path), "path": item_path, "parents": parent_paths}


def replace_item_parents(
    library_path: str,
    item_path: str,
    parent_paths: list[str],
    save: bool = True,
    lib: DataInstanceLibrary | None = None,
) -> dict:
    lib = _lib_for(library_path, lib)
    item = Path(item_path)
    assert item in lib.manifest, f"not found [{item_path}]"
    _assert_acyclic(lib, item, parent_paths)
    lib.SetParentsOf(item, [lib.Get(Path(p)) for p in parent_paths])
    if save:
        lib.Save()
    return {"library": str(library_path), "path": item_path, "parents": parent_paths}


def remove_item(
    library_path: str,
    item_path: str,
    save: bool = True,
    lib: DataInstanceLibrary | None = None,
) -> dict:
    lib = _lib_for(library_path, lib)
    lib.Remove(Path(item_path))
    if save:
        lib.Save()
    return {"library": str(library_path), "removed": item_path}


def rename_item(library_path: str, item_path: str, new_path: str) -> dict:
    lib = load_data_lib(library_path)
    lib.Rename(Path(item_path), Path(new_path))
    return {"library": str(library_path), "old": item_path, "new": new_path}


def retype_item(
    library_path: str,
    item_path: str,
    dtype: str,
    save: bool = True,
    lib: DataInstanceLibrary | None = None,
) -> dict:
    lib = _lib_for(library_path, lib)
    item = Path(item_path)
    assert item in lib.manifest, f"not found [{item_path}]"
    lib.GetType(dtype)
    was = lib.manifest[item]
    lib.manifest[item] = dtype
    lib._invalidate_endpoint_cache()
    relinked = _relink_children(lib, item, item)
    if save:
        lib.Save()
    return {
        "library": str(library_path),
        "path": str(item),
        "dtype": dtype,
        "was": was,
        "relinked": relinked,
    }


def _relink_children(lib: DataInstanceLibrary, old: Path, new: Path) -> int:
    affected = [
        (child, [pm.path for pm in plist])
        for child, plist in lib.parents.items()
        if any(pm.path == old for pm in plist)
    ]
    for child, paths in affected:
        lib.SetParentsOf(child, [lib.Get(new if p == old else p) for p in paths])
    return len(affected)


def repoint_item(
    library_path: str,
    item_path: str,
    new_path: str,
    save: bool = True,
    lib: DataInstanceLibrary | None = None,
) -> dict:
    lib = _lib_for(library_path, lib)
    old, new = Path(item_path), Path(new_path)
    assert old in lib.manifest, f"not found [{item_path}]"
    if old == new:
        return {"library": str(library_path), "old": item_path, "new": new_path,
                "moved": False, "relinked": 0}
    assert new not in lib.manifest, f"[{new}] is already registered here"
    assert old.is_absolute() == new.is_absolute(), (
        f"[{old}] is {'an absolute' if old.is_absolute() else 'a library-relative'} path, "
        f"so [{new}] has to be one too"
    )

    moved = not old.is_absolute()
    if moved:
        lib.Rename(old, new, _save=False)
    else:
        lib.manifest[new] = lib.manifest[old]
        del lib.manifest[old]
        if old in lib.parents:
            lib.parents[new] = lib.parents[old]
            del lib.parents[old]
        lib._migrate_instance_meta(old, new)
        lib._invalidate_endpoint_cache()
    relinked = _relink_children(lib, old, new)
    if save:
        lib.Save()
    return {
        "library": str(library_path),
        "old": str(old),
        "new": str(new),
        "moved": moved,
        "relinked": relinked,
    }


def rename_by_parent(library_path: str, parent_type: str) -> dict:
    lib = load_data_lib(library_path)
    lib.RenameByParent(parent_type)
    return {"library": str(library_path), "parent_type": parent_type}


def prune_types(
    library_path: str,
    whitelist: list[str] | None = None,
    save: bool = True,
) -> dict:
    lib = load_data_lib(library_path)
    wl = set(whitelist) if whitelist else None
    lib.PruneTypes(save=save, whitelist=wl)
    return {"library": str(library_path), "type_namespaces": list(lib.types.keys())}


def consolidate(library_path: str) -> dict:
    lib = load_data_lib(library_path)
    new_paths = lib.Consolidate()
    return {
        "library": str(library_path),
        "moves": {str(k): str(v) for k, v in new_paths.items()},
    }


def save_library(library_path: str, update_types: bool = True) -> dict:
    lib = load_data_lib(library_path)
    lib.Save(update_types=update_types)
    return {"library": str(library_path), "saved": True}


def pin_library(library_path: str, deep: bool = False) -> dict:
    lib = load_data_lib(library_path)
    return lib.Pin(deep=deep)


def unpin_library(library_path: str) -> dict:
    lib = DataInstanceLibrary.Load(library_path, check_pinned_stamps=False)
    return lib.Unpin()


def restamp_library(library_path: str, entry: str | None = None) -> dict:
    lib = DataInstanceLibrary.Load(library_path, check_pinned_stamps=False)
    return lib.Restamp([Path(entry)] if entry else None)


def invalidate_items(
    library_path: str, entries: list[str] | None = None, all: bool = False
) -> dict:
    lib = load_data_lib(library_path)
    if not entries and not all:
        raise ValueError("name at least one entry, or pass all=True")
    return lib.Invalidate(None if all else [Path(e) for e in entries])


def verify_library(library_path: str, deep: bool = False) -> dict:
    lib = DataInstanceLibrary.Load(library_path, check_pinned_stamps=False)
    return lib.Verify(deep=deep)


def trace_lineage(library_path: str, from_type: str, to_type: str) -> dict:
    lib = load_data_lib(library_path)
    pairs: dict[str, list[str]] = {}
    for from_inst, to_inst in lib.Trace(from_type, to_type):
        pairs.setdefault(str(from_inst.path), []).append(str(to_inst.path))
    return {
        "library": str(library_path),
        "from_type": from_type,
        "to_type": to_type,
        "pairs": pairs,
    }


def load_remote_library(
    src_uri: str,
    dest_path: str,
    on_exist: str = "skip",
    as_image: bool = True,
) -> dict:
    src = Source.Parse(src_uri)
    lib = DataInstanceLibrary.LoadFrom(src, Path(dest_path).resolve(), as_image, on_exist)
    return {
        "library": str(lib.location),
        "src": src_uri,
        "item_count": len(lib.manifest),
    }


def import_library(
    src_uri: str,
    dest_path: str,
    cache_root: str | None = None,
    on_exist: str = "skip",
    as_image: bool = True,
) -> dict:
    src = Source.Parse(src_uri)
    dest = Path(dest_path).resolve()
    lib = DataInstanceLibrary.LoadFrom(src, dest, as_image, on_exist)

    from ..caching.layout import default_cache_root

    if cache_root is None:
        cache_root_path = default_cache_root(dest.parent)
    else:
        cache_root_path = Path(cache_root).resolve()

    admitted = admit_library_items(lib, cache_root_path)
    return {
        "library": str(lib.location),
        "src": src_uri,
        "item_count": len(lib.manifest),
        "imported_cache_entries": admitted["admitted"],
        "skipped_leaf_entries": admitted["skipped_leaf"],
        "cache_root": str(cache_root_path),
    }


def admit_library_items(
    lib: DataInstanceLibrary,
    cache_root: Path,
    *,
    paths: list[Path] | None = None,
    include_leaves: bool = False,
) -> dict:
    """Register a library's items in the pool, through the pool's write door.

    The item keeps its bytes where they are. What enters the store is the
    entry: the item's identity, the name of its type, where it sits, and the
    identities it descends from. Nothing is stat'd beyond the one call that
    asks how big it is, and nothing is walked -- a folder of six hundred
    thousand files costs what a single file costs.
    """
    from ..caching.admission import IMPORTED, PoolFile, admit
    from ..caching.store import CacheStore

    cache_root = Path(cache_root)
    cache_root.mkdir(parents=True, exist_ok=True)
    store = CacheStore.open(cache_root)
    try:
        admitted = 0
        skipped_leaf = 0
        for path in (paths if paths is not None else list(lib.manifest)):
            meta = lib.instance_meta.get(path)
            if meta is None:
                continue
            origin = meta.get("origin", "leaf")
            if origin == "leaf" and not include_leaves:
                skipped_leaf += 1
                continue
            instance_id_hex = meta.get("instance_id")
            if not instance_id_hex:
                continue
            try:
                key = bytes.fromhex(instance_id_hex)
            except ValueError:
                continue
            inst = lib.Get(path)
            written = admit(
                cache_root=cache_root,
                key=key,
                origin=IMPORTED,
                files=[PoolFile(
                    dtype_name=inst.dtype_name,
                    dtype_key=inst.dtype.key,
                    abspath=str(inst.ResolvePath()),
                    slot_id=instance_id_hex,
                    parents=_parent_ids(lib, path),
                    size=_shallow_size(inst.ResolvePath()),
                )],
                lineage_payload=meta.get("lineage_payload") or b"",
                store=store,
            )
            if written.status != "failed":
                admitted += 1
    finally:
        store.close()
    return {
        "cache_root": str(cache_root),
        "admitted": admitted,
        "skipped_leaf": skipped_leaf,
    }


def _parent_ids(lib: DataInstanceLibrary, path: Path) -> list[str]:
    ids = []
    for pm in lib.parents.get(path, []):
        if pm.path not in lib.manifest:
            continue
        ids.append(lib.Get(pm.path).instance_id)
    return sorted(set(ids))


def _shallow_size(path: Path) -> int:
    # One stat, never a walk. A directory reports its own entry size, which is
    # not what it holds -- the alternative is a tree walk on every import, and
    # that is the cost this whole design exists to refuse.
    try:
        return int(Path(path).stat().st_size)
    except OSError:
        return 0


def resolve_store_root(
    agent_home: str | None = None, cache_root: str | None = None,
) -> Path:
    """Which pool a store verb acts on.

    The pool lives at an agent's home, because that is where a run's products
    land. An explicit root wins; otherwise the named agent's, otherwise
    $AGENT_HOME's.
    """
    from ..caching.layout import default_cache_root

    if cache_root is not None:
        return Path(cache_root).resolve()
    home = agent_home or os.environ.get("AGENT_HOME")
    if not home:
        raise ValueError(
            "no store: pass --agent-home or --cache-root, or set AGENT_HOME. "
            "The pool lives at an agent's home."
        )
    return default_cache_root(Path(home).resolve())


def _resolve_dtype(dtype: str, type_library_paths: list[str] | None):
    """The endpoint a type name refers to, when anything here can say.

    Given type libraries, an unresolvable name is refused: the caller asked for
    the check. Given none, the name is taken as declared -- the type is the
    user's statement of what the file is, and this command never opens the file
    to second-guess it.
    """
    from ..models.libraries import DataTypeLibrary

    if not type_library_paths:
        return None, False
    libs = {}
    for raw in type_library_paths:
        ns, _sep, tp = str(raw).partition("=")
        if not tp:
            ns, tp = "", ns
        tp = Path(tp).resolve()
        libs[ns or tp.stem] = DataTypeLibrary.Load(tp)
    if "::" not in dtype:
        raise ValueError(f"[{dtype}] is not in the format <namespace>::<type>")
    ns, name = dtype.split("::", 1)
    if ns not in libs:
        raise ValueError(
            f"namespace [{ns}] is not among the type libraries given: "
            f"{sorted(libs)}"
        )
    if name not in libs[ns]:
        raise ValueError(f"type [{name}] is not in [{ns}]")
    return libs[ns][name], True


def import_item(
    path: str,
    dtype: str,
    *,
    agent_home: str | None = None,
    cache_root: str | None = None,
    name: str | None = None,
    parents: list[str] | None = None,
    tags: list[str] | None = None,
    type_library_paths: list[str] | None = None,
) -> dict:
    """Register a file or folder the user already has as a pool instance.

    Nothing is copied, moved or read. The item keeps its bytes where they are
    and the pool records what it is: a freshly minted identity, the type, the
    name it was given, where it sits, and what it descends from.

    Every call is a separate act and mints a separate identity. Importing one
    path twice is therefore two entries, which is how a caller says a second
    declaration is a second thing, and two files handed the same name stay two
    things rather than collapsing into one. `mint_import_id` carries the whole
    argument; it reverses what this function used to do.

    An import is a setup act, performed once against a pool. A driver
    references what is already there and never calls this, which is why an
    assigned identity costs nothing in cache hits.

    The pool's lifetime is its campaign's. A minted identity cannot be rebuilt,
    so every shard keyed on this import dies when the pool does, reuse never
    crosses a campaign boundary, and a measurement comparing two batches needs
    both of them inside the agent home's retention window.
    """
    from ..caching.admission import IMPORTED, PoolFile, admit, mint_import_id

    root = resolve_store_root(agent_home, cache_root)
    target = Path(path).expanduser().resolve()
    endpoint, resolved = _resolve_dtype(dtype, type_library_paths)
    label = name if name is not None else str(target)
    key_hex = mint_import_id(dtype, label)
    arrival_ns = time.time_ns()

    parent_ids = _resolve_parent_ids(root, parents or [])
    written = admit(
        cache_root=root,
        key=bytes.fromhex(key_hex),
        origin=IMPORTED,
        files=[PoolFile(
            dtype_name=dtype,
            dtype_key=endpoint.key if endpoint is not None else "",
            abspath=str(target),
            slot_id=key_hex,
            parents=parent_ids,
            size=_shallow_size(target),
        )],
        name=label,
        imported_at=arrival_ns,
        tags=tuple(tags or ()),
    )
    return {
        "cache_root": str(root),
        "path": str(target),
        "dtype": dtype,
        "name": label,
        "instance_id": key_hex,
        "type_resolved": resolved,
        "parents": parent_ids,
        "tags": sorted(set(tags or ())),
        "status": written.status,
    }


def _resolve_parent_ids(cache_root: Path, parents: list[str]) -> list[str]:
    """Parents named by instance id, or by a path already in the store.

    A path names the newest entry that claims it, because an import of a path
    already imported supersedes the earlier declaration. Name the id to reach
    an older one.
    """
    if not parents:
        return []
    by_path = store_ids_by_path(cache_root)
    known = set(by_path.values())
    out = []
    for p in parents:
        if p in known:
            out.append(p)
            continue
        resolved = by_path.get(str(Path(p).expanduser().resolve()))
        if resolved is None:
            raise ValueError(
                f"[{p}] is neither an instance id nor a path in the store at "
                f"[{cache_root}]. A parent must already be in the pool, or the "
                "edge it records points at nothing."
            )
        out.append(resolved)
    return sorted(set(out))


def store_ids_by_path(cache_root: Path) -> dict:
    """Every file the store indexes, by where it sits. Needs no type library.

    One path can be claimed by several entries now that an import mints a fresh
    identity each time, so the newest claim wins -- the same rule the projection
    applies, for the same reason.
    """
    from ..caching.admission import manifest_arrival_ns, manifest_files
    from ..caching.store import CacheStore, decode_manifest

    root = Path(cache_root)
    if not (root / "cache.sqlite").exists():
        return {}
    store = CacheStore.open(root)
    try:
        best: dict[str, tuple] = {}
        for entry in store.iter_entries():
            try:
                manifest = decode_manifest(entry.payload)
            except Exception:
                continue
            arrival = manifest_arrival_ns(manifest, entry.created_at)
            for f in manifest_files(manifest):
                iid = f.InstanceId()
                rank = (arrival, iid)
                path = str(f.Resolve(entry.output_root))
                if path not in best or rank > best[path][0]:
                    best[path] = (rank, iid)
        return {path: iid for path, (_rank, iid) in best.items()}
    finally:
        store.close()


def forget_item(
    instance_id: str,
    *,
    agent_home: str | None = None,
    cache_root: str | None = None,
    delete: bool = False,
) -> dict:
    """Drop an imported entry from the pool.

    Only the entry. The bytes were never the pool's -- an import indexes a file
    where the user put it -- so there is nothing here that could delete them,
    and `--delete` removes the shard, which holds only the manifest.
    """
    import shutil as _shutil

    from ..caching.admission import IMPORTED, shard_for
    from ..caching.store import CacheStore

    root = resolve_store_root(agent_home, cache_root)
    key = bytes.fromhex(instance_id)
    store = CacheStore.open(root)
    try:
        entry = store.probe(key)
        if entry is None:
            return {"cache_root": str(root), "key": instance_id, "found": False}
        if entry.origin != IMPORTED:
            raise ValueError(
                f"[{instance_id}] is a product, not an import. A product is "
                "reclaimed by `metasmith cache gc`, which knows it can be "
                "re-derived."
            )
        store.tombstone(key)
        shard = shard_for(root, instance_id, IMPORTED)
        removed = False
        if delete and shard.is_dir() and shard.is_relative_to(root):
            _shutil.rmtree(shard)
            removed = True
    finally:
        store.close()
    return {
        "cache_root": str(root),
        "key": instance_id,
        "found": True,
        "tombstoned": True,
        "shard_removed": removed,
    }


def show_item_lineage(
    library_path: str,
    item_path: str,
    *,
    of: str | None = None,
    fmt: str = "json",
    depth: int | None = None,
    include_logs: bool = False,
    render: bool = True,
    lib: DataInstanceLibrary | None = None,
) -> dict:
    lib = _lib_for(library_path, lib)
    p = Path(item_path)
    inst = lib.Get(p)

    result = {
        "path": str(inst.path),
        "type_name": inst.dtype_name,
        "properties": inst.dtype.Pack()["properties"],
        "parents": [
            {"path": str(pm.path), "type_name": pm.name}
            for pm in lib.parents.get(p, [])
        ],
        "format": fmt,
        "depth": depth,
        "rendered": None,
        "written_to": None,
        "logs": None,
    }
    if not render:
        return result

    node = lib.get_lineage_of(inst)

    if fmt == "mermaid":
        rendered = node.to_mermaid(depth=depth if depth is not None else 16)
    else:
        rendered = node.to_json(indent=2, depth=depth)

    if include_logs:
        try:
            bundle = lib.get_logs_of(inst)
            result["logs"] = bundle.to_dict() if hasattr(bundle, "to_dict") else None
        except Exception as e:
            result["logs"] = {"error": repr(e)}

    if of is not None:
        out_path = Path(of)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(rendered, encoding="utf-8")
        result["written_to"] = str(of)
    else:
        result["rendered"] = rendered

    return result
