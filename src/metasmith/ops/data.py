"""Data instance library operations: CRUD on libraries and their items."""
from __future__ import annotations

from pathlib import Path

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


def import_library(
    src_uri: str,
    dest_path: str,
    cache_root: str | None = None,
    on_exist: str = "skip",
    as_image: bool = True,
) -> dict:
    """S7 — Import a library across workspaces, preserving cache identity.

    Transfers the library at `src_uri` into `dest_path` via LoadFrom, then
    upserts every imported `origin in {"lineage", "imported"}` DataInstance
    into the destination `task_cache/` as `origin="imported"` rows. Leaf
    instances are NOT upserted — their identity is unique-per-AddItem and
    not cache-meaningful. The upserted rows point at the library's files
    on disk so downstream workflows resolve them as cache hits.

    `cache_root` defaults to `<dest_path>/../task_cache/` to match the
    agent-home convention; pass an explicit path to override.
    """
    src = Source.Parse(src_uri)
    dest = Path(dest_path).resolve()
    lib = DataInstanceLibrary.LoadFrom(src, dest, as_image, on_exist)

    if cache_root is None:
        cache_root_path = dest.parent / "task_cache"
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
                # Legacy (non-multihash) id; keep the library entry but
                # skip the cache row since the key shape doesn't match.
                continue
            lineage_payload = meta.get("lineage_payload") or b""
            output_root_rel = f"imported/{instance_id_hex[:2]}/{instance_id_hex[2:]}"
            output_dir = cache_root_path / output_root_rel
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
            (output_dir / "manifest.cbor").write_bytes(payload)
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
