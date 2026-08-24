from __future__ import annotations

import os
import shutil
import tempfile
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

    from ..caching.layout import (
        MANIFEST_NAME,
        default_cache_root,
        imported_shard_dir,
    )

    if cache_root is None:
        cache_root_path = default_cache_root(dest.parent)
    else:
        cache_root_path = Path(cache_root).resolve()
    cache_root_path.mkdir(parents=True, exist_ok=True)

    from ..caching.store import CacheStore, encode_manifest

    store = CacheStore.open(cache_root_path)
    try:
        upserts = 0
        skipped_leaf = 0
        for path in lib.manifest:
            meta = lib.instance_meta.get(path)
            if meta is None:
                continue
            origin = meta.get("origin", "leaf")
            if origin == "leaf":
                skipped_leaf += 1
                continue
            instance_id_hex = meta.get("instance_id")
            if not instance_id_hex:
                continue
            try:
                key = bytes.fromhex(instance_id_hex)
            except ValueError:
                continue
            lineage_payload = meta.get("lineage_payload") or b""
            output_dir = imported_shard_dir(cache_root_path, instance_id_hex)
            output_root_rel = str(output_dir.relative_to(cache_root_path))
            output_dir.mkdir(parents=True, exist_ok=True)
            payload = encode_manifest(
                cache_key=key,
                transform_key="",
                signature="",
                lineage_payload=lineage_payload,
                output_files=[{"relpath": str(path)}],
                out_identities={},
                index_payload=[],
            )
            (output_dir / MANIFEST_NAME).write_bytes(payload)
            size_bytes = 0
            try:
                size_bytes = (lib.location / path).stat().st_size
            except OSError:
                pass
            store.upsert(
                key=key,
                transform_key="",
                payload=payload,
                output_root=output_root_rel,
                size_bytes=size_bytes,
                origin="imported",
            )
            upserts += 1
    finally:
        store.close()

    return {
        "library": str(lib.location),
        "src": src_uri,
        "item_count": len(lib.manifest),
        "imported_cache_entries": upserts,
        "skipped_leaf_entries": skipped_leaf,
        "cache_root": str(cache_root_path),
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
