"""Data instance library operations: CRUD on libraries and their items."""
from __future__ import annotations

import os
import shutil
from pathlib import Path

from ..hashing import KeyGenerator
from ..models.libraries import DataInstanceLibrary
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
    """Hardlink a library-internal file, falling back to a copy across filesystems."""
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst, follow_symlinks=follow_symlinks)


def fork_library(
    library_path: str,
    dest_path: str,
    fork_id: str | None = None,
) -> dict:
    """Copy a library's manifest to a new location under a fresh fork id.

    Identity in metasmith is content-free by design, so re-running with new bytes at
    the same paths reuses the old task. A fork is the explicit way to say the inputs
    changed: the new fork id lands in the manifest, which changes the library key,
    every instance_id derived from it, and therefore the task key -- with no file
    contents read. It also discards all cache reuse, so it is the expensive path.

    Data is not duplicated: items recorded as absolute paths are only manifest
    entries, symlinks are preserved as symlinks, and library-internal regular files
    are hardlinked where the filesystem allows.
    """
    src = Path(library_path).resolve()
    dest = Path(dest_path).resolve()
    assert src.is_dir(), f"library [{src}] does not exist"
    assert src != dest, "fork destination must differ from the source"
    assert not dest.exists() or not any(dest.iterdir()), (
        f"fork destination [{dest}] already exists and is not empty"
    )
    load_data_lib(src)  # fail before copying if the source is not a valid library
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


def add_item(
    library_path: str,
    host_path: str,
    dtype: str,
    parents: list[str] | None = None,
    save: bool = True,
) -> dict:
    lib = load_data_lib(library_path)
    parent_paths = [Path(p) for p in (parents or [])]
    rec_path = lib.AddItem(Path(host_path), dtype, parents=parent_paths)
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
) -> dict:
    lib = load_data_lib(library_path)
    parent_paths = [Path(p) for p in (parents or [])]
    rec_path = lib.AddValue(name, value, dtype, parents=parent_paths)
    if save:
        lib.Save()
    return {"library": str(library_path), "path": str(rec_path), "dtype": dtype, "value": value}


def set_item_parents(
    library_path: str,
    item_path: str,
    parent_paths: list[str],
    save: bool = True,
) -> dict:
    lib = load_data_lib(library_path)
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
) -> dict:
    """The same, but as a replacement: what is not listed is unlinked.

    `set_item_parents` can only ever add, so it cannot express "this no longer
    descends from that" -- and an editable lineage has to. An empty list clears
    an item's parents outright.
    """
    lib = load_data_lib(library_path)
    item = Path(item_path)
    assert item in lib.manifest, f"not found [{item_path}]"
    lib.SetParentsOf(item, [lib.Get(Path(p)) for p in parent_paths])
    if save:
        lib.Save()
    return {"library": str(library_path), "path": item_path, "parents": parent_paths}


def remove_item(library_path: str, item_path: str, save: bool = True) -> dict:
    lib = load_data_lib(library_path)
    lib.Remove(Path(item_path))
    if save:
        lib.Save()
    return {"library": str(library_path), "removed": item_path}


def rename_item(library_path: str, item_path: str, new_path: str) -> dict:
    lib = load_data_lib(library_path)
    lib.Rename(Path(item_path), Path(new_path))
    return {"library": str(library_path), "old": item_path, "new": new_path}


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


def show_item_lineage(library_path: str, item_path: str) -> dict:
    lib = load_data_lib(library_path)
    p = Path(item_path)
    inst = lib.Get(p)
    parents = []
    for pm in lib.parents.get(p, []):
        parents.append({"path": str(pm.path), "type_name": pm.name})
    return {
        "path": str(inst.path),
        "type_name": inst.dtype_name,
        "properties": inst.dtype.Pack()["properties"],
        "parents": parents,
    }
