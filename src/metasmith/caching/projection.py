"""The store, read back as a data instance library.

A pool entry is a data instance: a path, a type, an identity, and the
identities it descends from. That is the whole of what a `DataInstanceLibrary`
indexes, so the pool projects into one directly and the planner takes the
projection unchanged -- no second index, and no second thing to keep in step
with the first.

A projection is a live view of where the bytes are now, not a portable image.
Every path in it is absolute, and `MaterializeImage` drops absolute entries, so
a projection cannot be packed and shipped to a remote agent. Staging a store to
another host is a different job and a larger one.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .admission import (
    IMPORTED,
    PRODUCT,
    manifest_arrival_ns,
    manifest_files,
    manifest_lineage,
)
from .store import CacheStore, decode_manifest


@dataclass(frozen=True)
class ProjectedItem:
    """What the store knows about one projected instance, beside its type."""
    key: bytes
    origin: str
    transform_key: str
    step_name: str
    size_bytes: int
    created_at: int
    last_hit_at: int
    hit_count: int
    instance_id: str
    name: str = ""
    run: str = ""
    tags: tuple = ()


@dataclass
class Projection:
    library: object
    items: dict = field(default_factory=dict)
    # Why an entry is not in the library. Reported rather than raised: a pool
    # that predates this work holds entries no reader can type, and one of them
    # must not cost the caller every other one.
    skipped: dict = field(default_factory=dict)

    def Mask(self, **where) -> set:
        """The paths whose item matches every field given."""
        out = set()
        for path, item in self.items.items():
            if all(getattr(item, k, None) == v for k, v in where.items()):
                out.add(path)
        return out

    def View(self, **where):
        return self.library.AsView(self.Mask(**where))


def project_store(
    cache_root: Path,
    *,
    location: Path | None = None,
    types: dict | None = None,
    type_library_paths: list | None = None,
    origins: tuple = (PRODUCT, IMPORTED),
    include_tombstoned: bool = False,
) -> Projection:
    """Read the pool at `cache_root` as a data instance library.

    Types are not invented. An entry naming a type this projection was not
    handed, and an entry written before the compiler carried a type name at
    all, are both left out and counted -- the alternative is a library whose
    items claim a type nothing can resolve.
    """
    from ..models.libraries import DataInstanceLibrary, DataTypeLibrary

    cache_root = Path(cache_root)
    dtypes: dict = dict(types or {})
    for p in type_library_paths or []:
        p = Path(p)
        dtypes.setdefault(p.stem, DataTypeLibrary.Load(p))

    skipped: dict[str, list[str]] = {}
    def _skip(reason: str, key_hex: str):
        skipped.setdefault(reason, []).append(key_hex)

    # Pass one: every file in every entry, by its own identity. Parents are
    # recorded as identities, so nothing can be resolved until all of them are
    # known.
    by_id: dict[str, dict] = {}
    order: list[str] = []
    store = CacheStore.open(cache_root)
    try:
        for entry in store.iter_entries(include_tombstoned=include_tombstoned):
            key_hex = entry.key.hex()
            if entry.origin not in origins:
                continue
            try:
                manifest = decode_manifest(entry.payload)
            except Exception:
                _skip("unreadable manifest", key_hex)
                continue
            _index, payload = manifest_lineage(manifest)
            files = manifest_files(manifest)
            if not files:
                _skip("no files", key_hex)
                continue
            for f in files:
                if not f.dtype_name:
                    _skip("no type name", key_hex)
                    continue
                path = f.Resolve(entry.output_root)
                iid = f.InstanceId()
                if not iid:
                    _skip("no identity", key_hex)
                    continue
                by_id[iid] = {
                    "path": path,
                    "file": f,
                    "payload": payload,
                    "entry": entry,
                    "step_name": str(manifest.get("step_name", "")),
                    "name": str(manifest.get("name", "")),
                    "arrival": manifest_arrival_ns(manifest, entry.created_at),
                }
                order.append(iid)
    finally:
        store.close()

    # Pass two: identities to paths, one entry per path, because a library
    # manifest is keyed by path and cannot hold two instances at one.
    #
    # The newest claim wins. An import mints a fresh identity per act, so
    # importing a path already imported is a caller saying this declaration
    # supersedes the last one -- and a projection that kept the first would
    # make the new identity unreachable while leaving it in the store. The
    # displaced entry is not deleted: shards keyed on it stay valid, and it is
    # still reachable by id.
    path_of: dict[str, Path] = {}
    claimed: dict[Path, str] = {}
    rank: dict[Path, tuple] = {}
    for iid in order:
        rec = by_id[iid]
        path = rec["path"]
        here = (rec["arrival"], iid)
        if path in claimed and rank[path] >= here:
            _skip("path already claimed", rec["entry"].key.hex())
            continue
        if path in claimed:
            _skip("path already claimed", by_id[claimed[path]]["entry"].key.hex())
            path_of.pop(claimed[path], None)
        claimed[path] = iid
        rank[path] = here
        path_of[iid] = path

    parents_of = {
        iid: [
            path_of[pid] for pid in by_id[iid]["file"].parents if pid in path_of
        ]
        for iid in path_of
    }
    _drop_cycles(path_of, parents_of, _skip, by_id)

    raw_manifest: dict = {}
    items: dict[Path, ProjectedItem] = {}
    for iid, path in path_of.items():
        rec = by_id[iid]
        f = rec["file"]
        entry = rec["entry"]
        packed = {
            "type": f.dtype_name,
            "instance_id": iid,
            "origin": entry.origin,
        }
        if rec["payload"]:
            packed["lineage_payload"] = rec["payload"].hex()
        parent_paths = parents_of.get(iid, [])
        if parent_paths:
            packed["parents"] = {
                f"pool@{p}": by_id[claimed[p]]["file"].dtype_name
                for p in parent_paths
            }
        raw_manifest[str(path)] = packed
        items[path] = ProjectedItem(
            key=entry.key,
            origin=entry.origin,
            transform_key=entry.transform_key,
            step_name=rec["step_name"],
            size_bytes=entry.size_bytes,
            created_at=entry.created_at,
            last_hit_at=entry.last_hit_at,
            hit_count=entry.hit_count,
            instance_id=iid,
            name=rec["name"],
            run=entry.run,
            tags=entry.tags,
        )

    raw_manifest, items = _drop_unresolvable_types(
        raw_manifest, items, dtypes, by_id, claimed, _skip,
    )

    lib = DataInstanceLibrary.Unpack(
        location=Path(location) if location is not None else cache_root,
        raw={"schema": DataInstanceLibrary.schema, "manifest": raw_manifest},
        dtypes=dtypes,
    )
    lib.types = dtypes
    return Projection(library=lib, items=items, skipped=skipped)


def _drop_unresolvable_types(raw_manifest, items, dtypes, by_id, claimed, skip):
    """Leave out what the attached type libraries cannot name.

    Unpack resolves every item's type and every parent's, and raises on the
    first it cannot. A pool holds whatever every workflow that ever ran against
    it produced, so a projection asked for one namespace must not die on
    another.
    """
    from ..models.libraries import DataInstanceLibrary

    def _known(name: str) -> bool:
        try:
            DataInstanceLibrary._get_type(name, dtypes)
        except (ValueError, AssertionError, KeyError):
            return False
        return True

    kept: dict = {}
    for path_str, packed in raw_manifest.items():
        path = Path(path_str)
        if not _known(packed["type"]):
            skip("unknown type", items[path].key.hex())
            continue
        parents = {
            k: v for k, v in packed.get("parents", {}).items() if _known(v)
        }
        if parents:
            packed["parents"] = parents
        else:
            packed.pop("parents", None)
        kept[path_str] = packed
    items = {p: it for p, it in items.items() if str(p) in kept}
    # A parent that was itself dropped is no longer in the manifest, and Unpack
    # walks parents by path. Drop those edges too.
    for packed in kept.values():
        parents = {
            k: v for k, v in packed.get("parents", {}).items()
            if k.split("@", 1)[1] in kept
        }
        if parents:
            packed["parents"] = parents
        else:
            packed.pop("parents", None)
    return kept, items


def _drop_cycles(path_of, parents_of, skip, by_id):
    """Break any parent cycle before Unpack recurses into it.

    Unpack's ancestor walk has no cycle guard, so a store that somehow holds
    one would take the projection down with a recursion error rather than a
    message. A cycle means something upstream recorded a thing as its own
    ancestor; drop the edge that closes it and say so.
    """
    by_path = {p: iid for iid, p in path_of.items()}
    WHITE, GREY, BLACK = 0, 1, 2
    colour: dict[Path, int] = {}

    def walk(path):
        colour[path] = GREY
        iid = by_path.get(path)
        kept = []
        for parent in parents_of.get(iid, []):
            c = colour.get(parent, WHITE)
            if c == GREY:
                skip("parent cycle", by_id[iid]["entry"].key.hex())
                continue
            if c == WHITE:
                walk(parent)
            kept.append(parent)
        parents_of[iid] = kept
        colour[path] = BLACK

    for path in list(path_of.values()):
        if colour.get(path, WHITE) == WHITE:
            walk(path)
