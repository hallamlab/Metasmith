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

    A fork is the explicit way to say "treat these inputs as new", and its
    whole point is to discard cache reuse. It is the expensive path.

    Instance ids are content+path addressed, so they do not change just
    because the manifest moved. Setting `fork_id` is what makes every leaf
    id stale, and `DataInstanceLibrary` re-derives them with the fork id
    folded in. Lineage-derived ids are left alone: those are the hash of
    how an output was produced, not of where it sits.

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


def copy_library(
    library_path: str,
    dest_path: str,
    type_library_paths: list[str] | None = None,
) -> dict:
    """Copy a library verbatim: same paths, same ids, same key.

    The counterpart to `fork_library`, which exists to *break* identity. This
    one keeps it, and that is the whole point of the operation: a deferred path
    is minted once and identity follows the path, so a workflow started from a
    template inherits its rows rather than re-adding them -- re-adding would
    mint new paths and plan to a different task key than the one the template's
    own build asserted.

    `type_library_paths` are attached on top, skipping namespaces the copy
    already has. A template ships only the type libraries it used; whoever
    edits the copy needs the rest offered to them.
    """
    src = Path(library_path).resolve()
    dest = Path(dest_path).resolve()
    assert src.is_dir(), f"library [{src}] does not exist"
    assert src != dest, "copy destination must differ from the source"
    assert not dest.exists() or not any(dest.iterdir()), (
        f"copy destination [{dest}] already exists and is not empty"
    )
    load_data_lib(src)  # fail before copying if the source is not a valid library
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


def _ancestors_of(lib, start: Path) -> set[Path]:
    """Every path `start` descends from, walked rather than read off one record.

    `Load` expands the chain, so `lib.parents[p]` is usually already the closure
    -- but a library built up in memory has only the links that were stated, and
    the check below has to be right in both cases.
    """
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
    """Refuse a lineage that would close a loop.

    The browser filters these out of the menu, but this route is reachable
    without it, and a cycle is not something the library notices: `AsSamples`
    walks ancestors *and* their descendants, so a loop makes every mask the
    whole library, and the expand-on-load / collapse-on-save pair is not
    defined over one.
    """
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
) -> dict:
    """The same, but as a replacement: what is not listed is unlinked.

    `set_item_parents` can only ever add, so it cannot express "this no longer
    descends from that" -- and an editable lineage has to. An empty list clears
    an item's parents outright.
    """
    lib = load_data_lib(library_path)
    item = Path(item_path)
    assert item in lib.manifest, f"not found [{item_path}]"
    _assert_acyclic(lib, item, parent_paths)
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


def retype_item(library_path: str, item_path: str, dtype: str, save: bool = True) -> dict:
    """Say the item is a different type, without moving anything.

    Nothing about the row's identity on disk changes: a type is a label on a
    manifest entry, so this is a manifest edit and the filesystem is never
    touched. The children keep their lineage -- but the parent *record* each one
    carries names its parent's type, so those are rebuilt through the one place
    that knows how to build them, or the old name would be written back out.
    """
    lib = load_data_lib(library_path)
    item = Path(item_path)
    assert item in lib.manifest, f"not found [{item_path}]"
    lib.GetType(dtype)  # refuse an unknown type before the manifest is touched
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
    """Rebuild the parent record of everything that descends from `old`.

    A parent is stored as metadata carrying the parent's path *and* type, so a
    re-keyed or retyped parent leaves its children describing something the
    manifest no longer holds. `Rename` does not chase those down either, which
    is a latent bug rather than a licence to repeat it. Rebuilding through
    `SetParentsOf` keeps the record built in one place instead of reaching into
    the metadata objects.
    """
    affected = [
        (child, [pm.path for pm in plist])
        for child, plist in lib.parents.items()
        if any(pm.path == old for pm in plist)
    ]
    for child, paths in affected:
        lib.SetParentsOf(child, [lib.Get(new if p == old else p) for p in paths])
    return len(affected)


def repoint_item(library_path: str, item_path: str, new_path: str, save: bool = True) -> dict:
    """Point the row at a different path, and be honest about the difference.

    An absolute entry is a *pointer* to the user's own file: re-pointing it is a
    manifest edit and must not go near the filesystem -- neither the file at the
    old path nor the one at the new path is ours to move. A relative entry is
    library-owned (that is what `AddValue` writes), so there the file genuinely
    is the library's and moving it is the correct behaviour: that case delegates
    to `Rename`, which is written for it.

    Identity is derived from path and type, so either way the row's instance_id
    changes and anything downstream of it loses cache reuse.
    """
    lib = load_data_lib(library_path)
    old, new = Path(item_path), Path(new_path)
    assert old in lib.manifest, f"not found [{item_path}]"
    if old == new:
        return {"library": str(library_path), "old": item_path, "new": new_path,
                "moved": False, "relinked": 0}
    # a collision is a refusal with a message, not an assertion out of AddItem
    assert new not in lib.manifest, f"[{new}] is already registered here"
    assert old.is_absolute() == new.is_absolute(), (
        f"[{old}] is {'an absolute' if old.is_absolute() else 'a library-relative'} path, "
        f"so [{new}] has to be one too"
    )

    moved = not old.is_absolute()
    if moved:
        # library-owned: the file is the library's and the rename is a real move
        lib.Rename(old, new, _save=False)
    else:
        lib.manifest[new] = lib.manifest[old]
        del lib.manifest[old]
        if old in lib.parents:
            lib.parents[new] = lib.parents[old]
            del lib.parents[old]
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


def show_item_lineage(
    library_path: str,
    item_path: str,
    *,
    of: str | None = None,
    fmt: str = "json",
    depth: int | None = None,
    include_logs: bool = False,
    render: bool = True,
) -> dict:
    """Describe an item: declared identity, manifest parents, and lineage tree.

    Two different notions of ancestry meet here and they are not
    interchangeable. `parents` is the *manifest* relationship a user
    declares and edits -- it is what the GUI's parent picker writes and
    what its orphan detection reads, and it exists for an input library
    that has never been run. `rendered` is the *trace-derived* ancestor
    graph, which only exists for workflow-produced instances and needs a
    trace index the input library does not have. Returning only the
    second empties every consumer of the first with no error to notice,
    which is exactly what happened once.

    S7: the trace walk goes through `get_lineage_of` and renders as JSON
    or mermaid. `of=PATH` writes to disk; otherwise the rendered text
    comes back in the dict so the CLI can print it. `include_logs`
    attaches per-invocation `.command.*` paths via `get_logs_of`.

    `render=False` returns the cheap half only. List endpoints map this
    over every item in a library and must not pay for a trace walk per
    item.
    """
    lib = load_data_lib(library_path)
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
