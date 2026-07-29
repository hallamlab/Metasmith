"""The `.xgdb` store: real files on disk, each tagged with a type.

A `DataInstance` is one such file. Its `instance_id` is the identity every
downstream cache decision is made from, which is why minting one lives next
door in `identity` rather than inline here.

`DataInstanceLibrary` composes three mixins onto the store operations below:
`_LeafIdentity` for how a leaf is named, `_StoreTransfer` for getting the
store on and off disk, `_TelemetryQueries` for reading its trace back. Mixins
rather than modules of free functions because each needs the store's own state
(`instance_meta`, `location`, `_trace_index`); kept off this class so a change
to any one of them is a file someone can read end to end.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import yaml

from ...hashing import KeyGenerator
from ...logging import Log
from ..paths import DEFERRED, _DeferredPath, mint_deferred_path
from ..remote import Logistics, Source, SourceType
from ..solver import Dependency, Endpoint
from .identity import _LeafIdentity
from .telemetry_api import _TelemetryQueries
from .transfer import _StoreTransfer
from .types import DataTypeLibrary


@dataclass
class DataInstance:
    path: Path
    dtype: Endpoint
    dtype_name: str
    parent_lib: DataInstanceLibrary
    # S2 — two-source identity. `origin` is one of:
    #   "leaf"     — user-added via AddItem / AddValue. Unique per call.
    #   "lineage"  — produced by a transform; instance_id is the lineage_key
    #                over (transform_key, signature, sorted_input_ids).
    #   "imported" — round-tripped through msm data import-library from a
    #                foreign workspace; instance_id and lineage_payload are
    #                preserved verbatim.
    origin: str = "leaf"
    lineage_payload: bytes | None = None
    instance_id: str | None = None

    def __post_init__(self):
        # When instance_id is not provided, defer to the parent_lib's
        # per-path metadata cache. The lib mints + stores a fresh leaf id
        # on first sight of an unknown path (S2: unique-per-AddItem-call).
        if self.instance_id is None:
            meta = self.parent_lib._resolve_instance_meta(
                self.path, self.dtype_name
            )
            self.instance_id = meta["instance_id"]
            self.origin = meta.get("origin", "leaf")
            payload = meta.get("lineage_payload")
            self.lineage_payload = payload
        self._refresh_derived_keys()

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DataInstance) and self.instance_id == other.instance_id

    def _refresh_derived_keys(self):
        """Recompute _hash, _key, legacy_key from instance_id + dtype.

        _key tracks instance_id (modern callers); legacy_key preserves the
        old (path + dtype.key + dtype_name) shape so v0.18 serializations
        that referenced DataInstances by the old key still resolve.
        """
        self._hash, _ = KeyGenerator.FromStr(self.instance_id, l=10)
        self._key = self.instance_id
        _, self.legacy_key = KeyGenerator.FromStr("".join([
            str(self.path),
            self.dtype.key,
            self.dtype_name,
        ]), l=8)

    def RecalculateKey(self):
        """Backward-compat shim — see _refresh_derived_keys.

        Callers that mutate the instance in place (e.g., a dtype rename)
        used to invoke this to bring _hash / instance_id into sync with
        path + dtype. Under S2 the instance_id is owned by the library,
        so this just refreshes the derived shorter keys.
        """
        self._refresh_derived_keys()
        return self._key

    def WithDType(self, dtype: Endpoint, dtype_name: str | None = None):
        return self.__class__(
            path=self.path,
            dtype=dtype,
            dtype_name=self.dtype_name if dtype_name is None else dtype_name,
            parent_lib=self.parent_lib,
            origin=self.origin,
            lineage_payload=self.lineage_payload,
            instance_id=self.instance_id,
        )

    def GetDataType(self) -> tuple[str, str]:
        ns, name = self.dtype_name.split("::")
        return ns, name

    def ResolvePath(self):
        if self.path.is_absolute():
            return self.path
        else:
            return self.parent_lib.location/self.path

    def Pack(self):
        d = dict(
            path=str(self.path),
            type=f"{self.parent_lib.GetKey()}::{self.dtype_name}",
            type_id=self.dtype.key,
            instance_id=self.instance_id,
            origin=self.origin,
        )
        if self.lineage_payload is not None:
            d["lineage_payload"] = self.lineage_payload.hex()
        return d

    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataInstanceLibrary]):
        lib_key, namespace, dtype_name = raw["type"].split("::")
        lib = libraries[lib_key]
        dtype = lib.types[namespace][dtype_name]
        payload = raw.get("lineage_payload")
        if isinstance(payload, str):
            payload = bytes.fromhex(payload)

        inst = cls(
            path=Path(raw["path"]),
            dtype=dtype,
            dtype_name=f"{namespace}::{dtype_name}",
            parent_lib=lib,
            origin=raw.get("origin", "leaf"),
            lineage_payload=payload,
            instance_id=raw.get("instance_id"),
        )
        # Mirror the unpacked instance_id back into the library's meta so
        # subsequent lib.Get(path) calls return the same id rather than
        # minting a new leaf. Critical for round-trip stability when the
        # library YAML lacks per-path instance_ids but a referencing
        # workflow plan does carry them.
        if raw.get("instance_id"):
            lib.instance_meta[inst.path] = {
                "instance_id": inst.instance_id,
                "origin": inst.origin,
                "lineage_payload": inst.lineage_payload,
            }
        return inst

class DataInstanceLibrary(_LeafIdentity, _StoreTransfer, _TelemetryQueries):
    schema: str = "v1"
    _path_to_meta: Path = Path("./_metadata")
    _path_to_types: Path = Path("./_metadata/types")
    _index_name: str = "index"
    _metadata_ext: str = ".yml"

    @dataclass
    class ParentMetadata:
        dtype: Endpoint
        name: str
        library_key: str
        path: Path

    def __init__(self, location: Path|str|DataInstanceLibrary) -> None:
        self.manifest: dict[Path, str] = {}
        self.types: dict[str, DataTypeLibrary] = {}
        self._dtype2name = {}
        self.remote_src: Source|None = None
        # an optional user-set discriminator. Identity here is deliberately content-free
        # (inputs reach 100s of GB), so two libraries listing the same paths are the same
        # library. Setting this is how a user says "no, treat this as new" -- it is packed
        # into the manifest, so it flows into the library key, every instance_id, and the
        # plan/task key without any special casing downstream.
        self.fork_id: str|None = None
        self.parents: dict[Path, list[DataInstanceLibrary.ParentMetadata]] = {}
        self._endpoint_cache: dict[Path, Endpoint] = {}
        # S2 — per-path identity metadata. Each entry:
        #   {"instance_id": str, "origin": "leaf"|"lineage"|"imported",
        #    "lineage_payload": bytes|None}
        self.instance_meta: dict[Path, dict] = {}
        # Where each type namespace in `self.types` was loaded from, when that
        # was a plain path -- not needed for the normal directory-backed
        # round trip (Save/Load copy the namespace itself), only for
        # PackInline, which references the namespace instead of copying it.
        self._type_sources: dict[str, Path] = {}
        if isinstance(location, DataInstanceLibrary):
            other = location
            self.location = other.location
            self.manifest = other.manifest
            self.types = other.types
            self.instance_meta = other.instance_meta
            self.fork_id = other.fork_id
            self._type_sources = other._type_sources
        else:
            location = Path(location).resolve()
            if not location.exists():
                location.mkdir(parents=True)
            else:
                assert location.is_dir(), f"[{location}] must be a directory"
            self.location = location

    def __contains__(self, other):
        return other in self.manifest

    def Purge(self):
        if self.location.exists():
            shutil.rmtree(self.location)
        self.location.mkdir(exist_ok=True)

    def AddTypeLibrary(self, lib: DataTypeLibrary|Source|Path|str, namespace: str|None=None, on_exist: str="skip"):
        # Auto-detect swapped args: AddTypeLibrary("name", DataTypeLibrary(...))
        if isinstance(lib, str) and isinstance(namespace, DataTypeLibrary):
            lib, namespace = namespace, lib
        assert on_exist in {"skip", "error", "overwrite"}
        _source = Path(lib).resolve() if isinstance(lib, (Path, str)) else None
        if isinstance(lib, Path) or isinstance(lib, str):
            lib = Source.FromLocal(lib)
        if namespace is None:
            assert not isinstance(lib, DataTypeLibrary), f"namespace can not be left empty when a DataTypeLibrary is given directly"
            namespace = lib.GetPath().name

        if namespace in self.types:
            msg = f"[{namespace}] already exists"
            if on_exist == "skip":
                Log.Warn(msg)
                return self.types[namespace]
            elif on_exist == "error":
                raise AssertionError(msg)
            else: # on_exist == "clear":
                pass # just overwrite

        if not isinstance(lib, DataTypeLibrary):
            mover = Logistics()
            ext = self._metadata_ext
            namespace = namespace.replace(".yml", "").replace(ext, "")
            meta_path = self.location/self._path_to_types
            meta_path.mkdir(parents=True, exist_ok=True)
            lib_path = meta_path/(namespace+ext)
            lib_dest = Source(address=str(lib_path), type=SourceType.DIRECT)
            mover.QueueTransfer(
                src=lib,
                dest=lib_dest,
            )
            res = mover.ExecuteTransfers()
            assert len(res.completed) == 1, f"failed to add type library [{namespace}]"
            lib = DataTypeLibrary.Load(lib_dest.address)
        self.types[namespace] = lib
        if _source is not None:
            self._type_sources[namespace] = _source
        return self.types[namespace]

    @classmethod
    def _get_type(cls, name: str, types: dict[str, DataTypeLibrary]):
        if "::" not in name:
            raise ValueError(f"[{name}] is not in the format of <namespace>::<type>")
        namespace, name = name.split("::")
        assert namespace in types, f"namespace [{namespace}] not found"
        types_lib = types[namespace]
        assert name in types_lib, f"datatype [{name}] not found in [{namespace}]"
        return types_lib[name]

    def Get(self, path: str|Path):
        p = Path(path)
        e_name = self.manifest[p]
        e = self.GetType(e_name)
        if p in self.parents:
            # Build parent endpoints with their own lineage chains
            parent_endpoints = set()
            for parent_meta in self.parents[p]:
                parent_ep = self._build_endpoint_with_lineage(parent_meta.path)
                parent_endpoints.add(parent_ep)
            e = Endpoint(e.properties, parent_endpoints)
        return DataInstance(p, e, e_name, self)

    def _build_endpoint_with_lineage(self, path: Path, _seen: set[Path] | None = None) -> Endpoint:
        """Recursively build an endpoint with its full parent chain."""
        if _seen is None:
            _seen = set()
        if path in _seen:
            # Avoid infinite recursion
            e_name = self.manifest[path]
            return self.GetType(e_name)
        if path in self._endpoint_cache:
            return self._endpoint_cache[path]
        _seen.add(path)

        e_name = self.manifest[path]
        e = self.GetType(e_name)
        if path in self.parents:
            parent_endpoints = set()
            for parent_meta in self.parents[path]:
                parent_ep = self._build_endpoint_with_lineage(parent_meta.path, _seen)
                parent_endpoints.add(parent_ep)
            e = Endpoint(e.properties, parent_endpoints)
        self._endpoint_cache[path] = e
        return e

    def GetType(self, name: str):
        e = self._get_type(name, self.types)
        return e

    def GetName(self, dtype: Endpoint):
        if dtype in self._dtype2name: return self._dtype2name[dtype]
        for k, lib in self.types.items():
            def _name(name: str):
                return f"{k}::{name}"
            self._dtype2name.update({v:_name(name) for name, v in lib.types.items()})
        if dtype in self._dtype2name:
            return self._dtype2name[dtype]
        raise KeyError(f"datatype [{dtype}] not found")

    def Iterate(self):
        for k, v in self.manifest.items():
            proto = self.GetType(v)
            if k not in self.parents:
                yield k, v, proto
            else:
                yield k, v, Endpoint(proto.properties, {p.dtype for p in self.parents[k]})

    def AsSamples(self, index_types: str|Iterable[str], exact: bool=False):
        if isinstance(index_types, str):
            index_types=[index_types]

        if exact:
            _wl = set(index_types)
            def _accept(name: str):
                return name in _wl
        else:
            _wl = [self.GetType(n) for n in index_types]
            def _accept(name: str):
                model = self.GetType(name)
                return any(model.IsA(e) for e in _wl)

        def _get_all_ancestors(path: Path) -> set[Path]:
            """Recursively collect all ancestor paths."""
            ancestors = set()
            to_check = [path]
            while to_check:
                current = to_check.pop()
                for p in self.parents.get(current, []):
                    if p.path not in ancestors:
                        ancestors.add(p.path)
                        to_check.append(p.path)
            return ancestors

        # Build children_of reverse index once — O(N×P)
        children_of: dict[Path, set[Path]] = {}
        for item_path, parent_list in self.parents.items():
            for pm in parent_list:
                children_of.setdefault(pm.path, set()).add(item_path)

        def _get_all_descendants(ancestor_paths: set[Path]) -> set[Path]:
            """BFS down children_of index to find all descendants."""
            descendants = set()
            queue = list(ancestor_paths)
            while queue:
                current = queue.pop()
                for child in children_of.get(current, set()):
                    if child not in descendants:
                        descendants.add(child)
                        queue.append(child)
            return descendants

        _desc_cache: dict[frozenset[Path], set[Path]] = {}
        _yielded_ancestors: set[frozenset[Path]] = set()
        for path, name in self.manifest.items():
            if not _accept(name): continue
            ancestors = _get_all_ancestors(path)
            cache_key = frozenset(ancestors)
            if ancestors:
                # Everything under the shared ancestors, which is the same set
                # for every index item beneath them -- that is what makes this
                # topology collapse to one view, and what makes the cache safe.
                # `path`'s own subtree is inside it already.
                if cache_key not in _desc_cache:
                    _desc_cache[cache_key] = _get_all_descendants(ancestors)
                siblings = _desc_cache[cache_key]
            else:
                # A *root* index item has no ancestors, so every root shares the
                # empty cache key -- and caching against it handed every sample
                # the first item's subtree. Three views of the right shape, each
                # holding s0's files, silently. Roots are walked per item; each
                # walk covers only its own subtree, so the total is still linear.
                siblings = _get_all_descendants({path})
            # When path is already in siblings (shared-parent topology),
            # the mask is identical for all items with the same ancestors.
            # Yield only unique masks to avoid O(n^2) downstream.
            if path in siblings and cache_key in _yielded_ancestors:
                continue
            _yielded_ancestors.add(cache_key)
            yield DataInstanceLibraryView(original=self, mask={path} | ancestors | siblings)

    def Trace(self, from_type: str, to_type: str):
        """Trace lineage relationships between data types.

        Yields (from_instance, to_instance) pairs where from_instance is of
        from_type and to_instance is of to_type, connected through lineage.
        Works in both directions: ancestor (follow parents) and descendant
        (reverse lookup).

        Args:
            from_type: Source data type name (e.g. "mock::assembly")
            to_type: Target data type name (e.g. "mock::reads")

        Yields:
            Tuple of (DataInstance, DataInstance) pairs
        """
        # Build reverse index: path -> list of paths that have it as ancestor
        children_of: dict[Path, list[Path]] = {}
        for path, parents_list in self.parents.items():
            for p in parents_list:
                children_of.setdefault(p.path, []).append(path)

        for from_path, from_name in self.manifest.items():
            if from_name != from_type:
                continue
            from_inst = self.Get(from_path)

            # Check ancestors (to_type is an ancestor of from_type)
            for parent_meta in self.parents.get(from_path, []):
                if parent_meta.name == to_type:
                    to_inst = self.Get(parent_meta.path)
                    yield (from_inst, to_inst)

            # Check descendants (to_type is a descendant of from_type)
            for child_path in children_of.get(from_path, []):
                if self.manifest.get(child_path) == to_type:
                    to_inst = self.Get(child_path)
                    yield (from_inst, to_inst)

    def AddItem(self, path: Path|str|_DeferredPath, dtype: str, parents: Iterable[Path]|None=None):
        if parents is None:
            parents = []
        for p in parents:
            assert p in self.manifest
        # DEFERRED is a constant, so what the caller passes carries nothing to
        # tell two deferred rows apart. The manifest is keyed by path and
        # identity derives from path, so the distinct value is minted here, on
        # receipt, and persisted from then on.
        path = mint_deferred_path() if path is DEFERRED else Path(path)
        assert path not in self.manifest, f"[{path}] already added"
        type_model = self.GetType(dtype) # check if datatype exists
        self.manifest[path] = dtype
        # R1: mint the leaf instance_id at AddItem time. When the file is
        # present the id is derived from (content digest ⊕ relative path)
        # so two runs on byte-identical inputs at the same layout mint the
        # same id → cross-run cache reuse, while distinct files that share
        # bytes stay distinct; when the file is absent it falls back to a
        # unique-per-call random id (legacy S2 behaviour). See _mint_leaf_id.
        self._mint_leaf_id(path)
        self.AddParentsTo(path, [self.Get(p) for p in parents])
        self._invalidate_endpoint_cache()
        return path

    def SetLineageInstance(
        self,
        path: Path,
        *,
        instance_id: str,
        lineage_payload: bytes,
        origin: str = "lineage",
    ) -> None:
        """Register a non-leaf (origin=lineage|imported) entry.

        Used by the post-execution promote step (S5) to record that a
        transform produced an output whose identity is the lineage_key
        over its (transform_key, signature, sorted_input_ids).
        """
        assert origin in {"lineage", "imported"}, (
            f"origin must be lineage or imported, got {origin!r}"
        )
        self.instance_meta[path] = {
            "instance_id": instance_id,
            "origin": origin,
            "lineage_payload": lineage_payload,
        }

    def AddValue(self, name: str, value: str|dict, dtype: str, parents: Iterable[Path]|None=None):
        path = Path(name)
        if isinstance(value, dict):
            value = json.dumps(value)
        path = self.AddItem(path=path, dtype=dtype, parents=parents) # perform checks first
        with open(self.location/path, "w") as f:
            f.write(value)
        return path

    def _invalidate_endpoint_cache(self):
        self._endpoint_cache.clear()

    def Remove(self, path: Path):
        assert path in self.manifest, f"not found [{path}]"
        try:
            K = Path("./test")
            self.manifest[K] = ""
            del self.manifest[K]
        except RuntimeError:
            assert False, f"can not make changes while iterating library"

        del self.manifest[path]
        if path in self.parents:
            del self.parents[path]
        self.instance_meta.pop(path, None)
        self._invalidate_endpoint_cache()

    def _migrate_instance_meta(self, old: Path, new: Path):
        """Move a path's identity entry through a rename.

        A lineage or imported id hashes how the output was produced and
        does not depend on where it sits, so it follows the file. A leaf
        id folds the library-relative path, so it is re-minted at the new
        path -- which is what a library freshly built over the same bytes
        at that path holds. Carrying the old id across instead leaves one
        library state with two possible ids depending on how it got there,
        and two runs that should share a cache key stop sharing one.

        Re-minting here rather than dropping the entry is deliberate: an
        absent entry falls through to the legacy `(path, dtype, lib_key)`
        derivation, which is neither content-addressed nor what a fresh
        build would produce. That path is for v0.18 manifests only.
        """
        meta = self.instance_meta.pop(old, None)
        if meta is None:
            return
        if meta.get("origin", "leaf") != "leaf":
            self.instance_meta[new] = meta
        else:
            self._mint_leaf_id(new)

    def Rename(self, path: Path, new: Path, _save=True):
        """
        Rename data instance in library and the file system.
        *library will be corrupted if change is not saved
        """
        assert path in self.manifest, f"not found [{path}]"
        assert path.is_absolute() == new.is_absolute(), f"can not mix relative and absolute paths [{path}, {new}]"
        assert new not in self.manifest, f"already exists [{new}]"
        try:
            K = Path("./test")
            self.manifest[K] = ""
            del self.manifest[K]
        except RuntimeError:
            assert False, f"can not make changes while iterating library"
        abs_path = path
        if not path.is_absolute():
            abs_path = self.Get(path).ResolvePath()
        assert abs_path.exists(), f"file not exists [{abs_path}]"
        if not new.is_absolute():
            _new = self.location/new
        else:
            _new = new
        abs_path.rename(_new)
        self.manifest[new] = self.manifest[path]
        del self.manifest[path]
        if path in self.parents:
            self.parents[new] = self.parents[path]
            del self.parents[path]
        self._migrate_instance_meta(path, new)
        self._invalidate_endpoint_cache()
        if _save: self.Save()

    def RenameByParent(self, parent_type: str):
        """
        Rename all items in the library based on the path stem of their parent of the given type.
        Uses a transactional approach: plans all renames, executes filesystem moves, then commits manifest atomically.
        """
        # Phase A — Plan (read-only)
        rename_plan: list[tuple[Path, Path]] = []  # (old_path, proposed_new_path)

        for item_path, item_type in self.manifest.items():
            if item_type == parent_type:
                continue
            if item_path not in self.parents:
                continue
            matching_parents = [p for p in self.parents[item_path] if p.name == parent_type]
            if not matching_parents:
                continue
            matching_parents.sort(key=lambda p: str(p.path))
            parent = matching_parents[0]
            new_path = item_path.parent / (parent.path.stem + item_path.suffix)
            rename_plan.append((item_path, new_path))

        # Detect collisions: group by (directory, new_filename)
        # Also account for non-renamed items that occupy target paths
        renamed_old_paths = {old for old, _ in rename_plan}
        occupied_paths = {p for p in self.manifest if p not in renamed_old_paths}
        final_plan: list[tuple[Path, Path]] = []
        seen: dict[Path, list[int]] = {}  # new_path -> list of indices in rename_plan
        for i, (old, new) in enumerate(rename_plan):
            seen.setdefault(new, []).append(i)

        for new_path, indices in seen.items():
            needs_hash = len(indices) > 1 or new_path in occupied_paths
            for idx in indices:
                old, proposed = rename_plan[idx]
                if needs_hash:
                    _, hash_str = KeyGenerator.FromStr(str(old), l=8)
                    final_new = old.parent / (proposed.stem + "_" + hash_str + proposed.suffix)
                else:
                    final_new = proposed
                if old != final_new:
                    final_plan.append((old, final_new))

        if not final_plan:
            return

        # Execute filesystem moves
        completed: list[tuple[Path, Path]] = []
        try:
            for old, new in final_plan:
                abs_old = old if old.is_absolute() else self.location / old
                abs_new = new if new.is_absolute() else self.location / new
                abs_new.parent.mkdir(parents=True, exist_ok=True)
                abs_old.rename(abs_new)
                completed.append((old, new))
        except Exception:
            # Rollback: move completed renames back to originals
            for orig, renamed in completed:
                abs_renamed = renamed if renamed.is_absolute() else self.location / renamed
                abs_orig = orig if orig.is_absolute() else self.location / orig
                if abs_renamed.exists():
                    abs_renamed.rename(abs_orig)
            raise

        # Commit manifest atomically — two-phase for O(N×P) instead of O(R×N×P)
        old_to_new = {old: new for old, new in final_plan}
        # Phase 1: Move manifest and parents keys
        for old, new in final_plan:
            self.manifest[new] = self.manifest[old]
            del self.manifest[old]
            if old in self.parents:
                self.parents[new] = self.parents[old]
                del self.parents[old]
            self._migrate_instance_meta(old, new)
        # Phase 2: Single pass to update all parent references
        for parent_list in self.parents.values():
            for pm in parent_list:
                if pm.path in old_to_new:
                    pm.path = old_to_new[pm.path]
        self._invalidate_endpoint_cache()
        self.Save()

    def AddParentsTo(self, path: Path|str, parents: Iterable[DataInstance]):
        if all(False for _ in parents):
            return # there were no parents
        p = Path(path)
        current = self.parents.get(p, [])
        seen = {f"{x.library_key}/{x.path}" for x in current}
        def _get_k(d: DataInstance):
            return f"{d.parent_lib.GetKey()}/{d.path}"
        current += [self.ParentMetadata(p.dtype, p.dtype_name, p.parent_lib.GetKey(), p.path) for p in parents if _get_k(p) not in seen]
        self.parents[p] = current
        self._invalidate_endpoint_cache()

    def SetParentsOf(self, path: Path|str, parents: Iterable[DataInstance]):
        """Declare an item's lineage to be exactly this, dropping what it was.

        `AddParentsTo` is a union, which is what declaring lineage as items
        arrive wants and what taking a link back cannot use. Editing needs both
        directions, so this clears first and then goes through the same add --
        the parent metadata is built in one place either way.
        """
        p = Path(path)
        if p in self.parents:
            del self.parents[p]
            # ...even when the new list is empty, which AddParentsTo returns
            # early on: an endpoint cached with the old parents is now wrong
            self._invalidate_endpoint_cache()
        self.AddParentsTo(p, parents)

    def _calculate_key(self, _raw_override=None):
        if _raw_override is not None:
            me_d = {k: v for k, v in _raw_override.items() if k != "remote_src"}
        else:
            me_d = self.Pack()
            for k in ["remote_src"]:
                if k in me_d: del me_d[k]
        me = yaml.dump(me_d)
        self._hash, self._key = KeyGenerator.FromStr(me, l=12)
        return self._key

    def GetKey(self):
        if not hasattr(self, "_key"):
            self._calculate_key()
        return self._key

    def __hash__(self) -> int:
        if not hasattr(self, "_hash"):
            self._calculate_key()
        return self._hash

    def PruneTypes(self, save: bool=True, whitelist: set[str|Dependency|Endpoint]|None=None):
        used_type_names = set(self.manifest.values())
        if whitelist is None: whitelist = set() 
        wl_names = {x for x in whitelist if isinstance(x, str)}
        wl_types = {x for x in whitelist if not isinstance(x, str)}
        used_type_names |= wl_names
        for namespace, lib in list(self.types.items()):
            lib_types = {f"{namespace}::{dtype}" for dtype in lib.types}
            _used = used_type_names.intersection(lib_types)
            if len(_used) == 0:
                del self.types[namespace]
            else:
                new = DataTypeLibrary.Unpack(lib.Pack())
                new.types = {k:v for k, v in lib.types.items() if f"{namespace}::{k}" in _used or v in wl_types}
                self.types[namespace] = new
        if save: self.Save(update_types=True)

    def AsView(self, mask: set[Path], invert=False):
        """if invert=True, then items in mask are excluded"""
        return DataInstanceLibraryView(self, mask, invert)


class DataInstanceLibraryView:
    def __init__(self, original: DataInstanceLibrary, mask: set[Path]|None=None, invert=False) -> None:
        if mask is None:
            mask = set(original.manifest)
        if invert:
            oset = set(original.manifest)
            mask = oset-mask
        self._original = original
        self._mask = mask

    def __getattr__(self, name):
        # Delegate anything the view does not override (GetKey, PrepTransfer,
        # GetPath, manifest, ...) to the wrapped library. The mask only needs
        # to affect iteration and lookup; identity and transfer -- used by
        # WorkflowTask.Pack / SaveAs when staging -- come straight from the
        # original, so a masked view stages like a real library. The masked-out
        # entries are transferred but never referenced by any plan step.
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        try:
            original = object.__getattribute__(self, "_original")
        except AttributeError:
            raise AttributeError(name)
        return getattr(original, name)

    @property
    def _mask_key(self) -> frozenset[Path]:
        if not hasattr(self, '_cached_mask_key'):
            self._cached_mask_key = frozenset(self._mask)
        return self._cached_mask_key

    def Get(self, path: str|Path):
        p = Path(path)
        assert p in self._mask
        return self._original.Get(path)
    
    def Iterate(self):
        for p in self._mask:
            inst = self._original.Get(p)
            yield p, inst.dtype_name, inst.dtype
