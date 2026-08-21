from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable

import yaml

from ...hashing import KeyGenerator
from ...logging import Log
from ..paths import DEFERRED, _DeferredPath, is_deferred, mint_deferred_path
from ..remote import Logistics, Source, SourceType
from ..solver import Dependency, Endpoint
from .pinned import _PinnedLibrary
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
    origin: str = "leaf"
    lineage_payload: bytes | None = None
    instance_id: str | None = None

    def __post_init__(self):
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
        self._hash, _ = KeyGenerator.FromStr(self.instance_id, l=10)
        self._key = self.instance_id
        _, self.legacy_key = KeyGenerator.FromStr("".join([
            str(self.path),
            self.dtype.key,
            self.dtype_name,
        ]), l=8)

    def RecalculateKey(self):
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
        if raw.get("instance_id"):
            lib.instance_meta[inst.path] = {
                "instance_id": inst.instance_id,
                "origin": inst.origin,
                "lineage_payload": inst.lineage_payload,
            }
        return inst

class DataInstanceLibrary(_LeafIdentity, _StoreTransfer, _PinnedLibrary, _TelemetryQueries):
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
        self.fork_id: str|None = None
        self.parents: dict[Path, list[DataInstanceLibrary.ParentMetadata]] = {}
        self._endpoint_cache: dict[Path, Endpoint] = {}
        self.instance_meta: dict[Path, dict] = {}
        self._type_sources: dict[str, Path] = {}
        # The `pinned:` block from index.yml, or None for the overwhelming
        # majority of libraries. See pinned.py.
        self._pinned: dict|None = None
        if isinstance(location, DataInstanceLibrary):
            other = location
            self.location = other.location
            self.manifest = other.manifest
            self.types = other.types
            self.instance_meta = other.instance_meta
            self.fork_id = other.fork_id
            self._type_sources = other._type_sources
            self._pinned = other._pinned
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
        self._refuse_if_pinned("Purge")
        if self.location.exists():
            shutil.rmtree(self.location)
        self.location.mkdir(exist_ok=True)

    def AddTypeLibrary(self, lib: DataTypeLibrary|Source|Path|str, namespace: str|None=None, on_exist: str="skip"):
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
            else:
                pass

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
            parent_endpoints = set()
            for parent_meta in self.parents[p]:
                parent_ep = self._build_endpoint_with_lineage(parent_meta.path)
                parent_endpoints.add(parent_ep)
            e = Endpoint(e.properties, parent_endpoints)
        return DataInstance(p, e, e_name, self)

    def _build_endpoint_with_lineage(self, path: Path, _seen: set[Path] | None = None) -> Endpoint:
        if _seen is None:
            _seen = set()
        if path in _seen:
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
            ancestors = set()
            to_check = [path]
            while to_check:
                current = to_check.pop()
                for p in self.parents.get(current, []):
                    if p.path not in ancestors:
                        ancestors.add(p.path)
                        to_check.append(p.path)
            return ancestors

        children_of: dict[Path, set[Path]] = {}
        for item_path, parent_list in self.parents.items():
            for pm in parent_list:
                children_of.setdefault(pm.path, set()).add(item_path)

        def _get_all_descendants(ancestor_paths: set[Path]) -> set[Path]:
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
                if cache_key not in _desc_cache:
                    _desc_cache[cache_key] = _get_all_descendants(ancestors)
                siblings = _desc_cache[cache_key]
            else:
                siblings = _get_all_descendants({path})
            if path in siblings and cache_key in _yielded_ancestors:
                continue
            _yielded_ancestors.add(cache_key)
            yield DataInstanceLibraryView(original=self, mask={path} | ancestors | siblings)

    def Trace(self, from_type: str, to_type: str):
        children_of: dict[Path, list[Path]] = {}
        for path, parents_list in self.parents.items():
            for p in parents_list:
                children_of.setdefault(p.path, []).append(path)

        for from_path, from_name in self.manifest.items():
            if from_name != from_type:
                continue
            from_inst = self.Get(from_path)

            for parent_meta in self.parents.get(from_path, []):
                if parent_meta.name == to_type:
                    to_inst = self.Get(parent_meta.path)
                    yield (from_inst, to_inst)

            for child_path in children_of.get(from_path, []):
                if self.manifest.get(child_path) == to_type:
                    to_inst = self.Get(child_path)
                    yield (from_inst, to_inst)

    def _register(self, path, dtype: str, parents, set_identity):
        if parents is None:
            parents = []
        for p in parents:
            assert p in self.manifest
        path = mint_deferred_path() if path is DEFERRED else Path(path)
        assert path not in self.manifest, f"[{path}] already added"
        self.GetType(dtype)
        self.manifest[path] = dtype
        set_identity(path)
        self.AddParentsTo(path, [self.Get(p) for p in parents])
        self._invalidate_endpoint_cache()
        return path

    def AddItem(self, path: Path|str|_DeferredPath, dtype: str, parents: Iterable[Path]|None=None):
        self._refuse_if_pinned("AddItem")
        return self._register(path, dtype, parents, self._mint_leaf_id)

    def RegisterItem(
        self,
        path: Path|str,
        dtype: str,
        *,
        instance_id: str,
        origin: str = "leaf",
        lineage_payload: bytes|None = None,
        parents: Iterable[Path]|None = None,
    ):
        def _set(p: Path):
            self.instance_meta[p] = {
                "instance_id": instance_id,
                "origin": origin,
                "lineage_payload": lineage_payload,
                "fork_id": self.fork_id,
            }
        self._refuse_if_pinned("RegisterItem")
        assert origin in {"leaf", "lineage", "imported"}, f"bad origin {origin!r}"
        return self._register(path, dtype, parents, _set)

    def SetLineageInstance(
        self,
        path: Path,
        *,
        instance_id: str,
        lineage_payload: bytes,
        origin: str = "lineage",
    ) -> None:
        self._refuse_if_pinned("SetLineageInstance")
        assert origin in {"lineage", "imported"}, (
            f"origin must be lineage or imported, got {origin!r}"
        )
        self.instance_meta[path] = {
            "instance_id": instance_id,
            "origin": origin,
            "lineage_payload": lineage_payload,
        }

    def AddValue(self, name: str, value: str|dict, dtype: str, parents: Iterable[Path]|None=None):
        # The file is written before it is registered: a leaf id is derived from
        # the file's stat, and registering first would find nothing there and
        # fall back to a random id.
        self._refuse_if_pinned("AddValue")
        path = Path(name)
        if isinstance(value, dict):
            value = json.dumps(value)
        with open(self.location/path, "w") as f:
            f.write(value)
        return self.AddItem(path=path, dtype=dtype, parents=parents)

    def _invalidate_endpoint_cache(self):
        self._endpoint_cache.clear()

    def Invalidate(self, paths: Iterable[Path]|None = None) -> dict:
        """Say that the data behind these items has changed.

        A leaf's identity is the absolute path it sits at and the mtime of the
        top node -- one stat, whatever the size of what is there, which is what
        keeps a 24 GB reference from costing a tree walk on every run. The
        price is that a change below the top node is invisible, and this is the
        lever for it: touch the path, re-mint through the same formula the
        agent uses, and every cache key built on the old id stops matching.
        """
        self._refuse_if_pinned("Invalidate")
        targets = (
            list(self.manifest) if paths is None
            else [Path(p) for p in paths]
        )
        moved: dict[str, dict[str, str]] = {}
        skipped: dict[str, str] = {}
        for path in targets:
            if path not in self.manifest:
                skipped[str(path)] = "not in the library"
                continue
            entry = self.instance_meta.get(path) or {}
            if entry.get("origin", "leaf") != "leaf":
                skipped[str(path)] = (
                    "produced by a run; its identity is its lineage"
                )
                continue
            abs_path = self._abs(path)
            if is_deferred(abs_path):
                skipped[str(path)] = "deferred; there is nothing to touch yet"
                continue
            try:
                # Follows symlinks, because the id does. Strictly forward, so
                # an item touched twice inside one clock tick still moves --
                # an invalidate that leaves the id where it was is worse than
                # no invalidate at all.
                st = abs_path.stat()
                bump = max(time.time_ns(), st.st_mtime_ns + 1)
                os.utime(abs_path, ns=(bump, bump))
            except OSError as e:
                # Minting a random id for something this host cannot see is how
                # a false hit gets built. Report it instead.
                skipped[str(path)] = f"not reachable from this host: {e}"
                continue
            old_id = entry.get("instance_id", "")
            moved[str(path)] = {
                "from": old_id, "to": self._mint_leaf_id(path)
            }
        if moved:
            self.Save()
        return {
            "library": str(self.location),
            "moved": moved,
            "skipped": skipped,
        }

    def Remove(self, path: Path):
        self._refuse_if_pinned("Remove")
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
        meta = self.instance_meta.pop(old, None)
        if meta is None:
            return
        if meta.get("origin", "leaf") != "leaf":
            self.instance_meta[new] = meta
        else:
            self._mint_leaf_id(new)

    def Rename(self, path: Path, new: Path, _save=True):
        self._refuse_if_pinned("Rename")
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
        self._refuse_if_pinned("RenameByParent")
        rename_plan: list[tuple[Path, Path]] = []

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

        renamed_old_paths = {old for old, _ in rename_plan}
        occupied_paths = {p for p in self.manifest if p not in renamed_old_paths}
        final_plan: list[tuple[Path, Path]] = []
        seen: dict[Path, list[int]] = {}
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

        completed: list[tuple[Path, Path]] = []
        try:
            for old, new in final_plan:
                abs_old = old if old.is_absolute() else self.location / old
                abs_new = new if new.is_absolute() else self.location / new
                abs_new.parent.mkdir(parents=True, exist_ok=True)
                abs_old.rename(abs_new)
                completed.append((old, new))
        except Exception:
            for orig, renamed in completed:
                abs_renamed = renamed if renamed.is_absolute() else self.location / renamed
                abs_orig = orig if orig.is_absolute() else self.location / orig
                if abs_renamed.exists():
                    abs_renamed.rename(abs_orig)
            raise

        old_to_new = {old: new for old, new in final_plan}
        for old, new in final_plan:
            self.manifest[new] = self.manifest[old]
            del self.manifest[old]
            if old in self.parents:
                self.parents[new] = self.parents[old]
                del self.parents[old]
            self._migrate_instance_meta(old, new)
        for parent_list in self.parents.values():
            for pm in parent_list:
                if pm.path in old_to_new:
                    pm.path = old_to_new[pm.path]
        self._invalidate_endpoint_cache()
        self.Save()

    def AddParentsTo(self, path: Path|str, parents: Iterable[DataInstance]):
        if all(False for _ in parents):
            return
        p = Path(path)
        current = self.parents.get(p, [])
        seen = {f"{x.library_key}/{x.path}" for x in current}
        def _get_k(d: DataInstance):
            return f"{d.parent_lib.GetKey()}/{d.path}"
        current += [self.ParentMetadata(p.dtype, p.dtype_name, p.parent_lib.GetKey(), p.path) for p in parents if _get_k(p) not in seen]
        self.parents[p] = current
        self._invalidate_endpoint_cache()

    def SetParentsOf(self, path: Path|str, parents: Iterable[DataInstance]):
        p = Path(path)
        if p in self.parents:
            del self.parents[p]
            self._invalidate_endpoint_cache()
        self.AddParentsTo(p, parents)

    #: Top-level index keys that are ABOUT the library rather than part of what
    #: it is. `remote_src` is where a copy came from; `pinned` is a stat stamp
    #: over the same manifest. Letting either into the key would move the
    #: library key -- and so every task key built on it -- when nothing about
    #: the data changed, which for `pinned` would re-break the plan stability
    #: pinning exists to buy: a legitimate re-stamp must be invisible here.
    #: `frozen` is the pre-rename spelling of the same block; an index written
    #: under it must key the same as one written now, or the rename moves every
    #: task key built on a library nobody re-pinned.
    _KEY_EXCLUDED_TOP_LEVEL = ("remote_src", "pinned", "frozen")

    def _calculate_key(self, _raw_override=None):
        src = _raw_override if _raw_override is not None else self.Pack()
        me_d = {k: v for k, v in src.items() if k not in self._KEY_EXCLUDED_TOP_LEVEL}
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
        self._refuse_if_pinned("PruneTypes")
        used_type_names = self._used_type_names()
        if whitelist is None: whitelist = set()
        used_type_names |= {x for x in whitelist if isinstance(x, str)}
        # Matched on properties, not on the node hash: a Dependency minted by
        # AddRequirement(example=e) carries the caller's parents, so it hashes
        # differently from the library Endpoint it was cloned from.
        wl_props = {
            frozenset(x.properties)
            for x in whitelist if not isinstance(x, str)
        }
        for namespace, lib in list(self.types.items()):
            keep = {
                k for k, v in lib.types.items()
                if f"{namespace}::{k}" in used_type_names or frozenset(v.properties) in wl_props
            }
            if len(keep) == 0:
                del self.types[namespace]
            else:
                new = DataTypeLibrary.Unpack(lib.Pack())
                new.types = {k: v for k, v in lib.types.items() if k in keep}
                self.types[namespace] = new
        if save:
            # Load re-reads every file under _metadata/types/ and takes each one
            # as a namespace, so a prune that only narrows memory is undone by
            # the next load -- and a file left by an older build with a wider
            # --types is a namespace nobody declared. What survives here is what
            # the directory holds.
            #
            # The unlink lives in PruneTypes rather than in _persist because
            # _ensure_saved() calls _persist on every PrepTransfer, and stripping
            # a source library as a side effect of preparing to send it is not
            # what that call means.
            types_dir = self.location/self._path_to_types
            if types_dir.is_dir():
                for f in types_dir.iterdir():
                    if f.is_dir() or f.suffix != self._metadata_ext: continue
                    if f.stem in self.types: continue
                    f.unlink(missing_ok=True)
            self.Save(update_types=True)

    def _used_type_names(self) -> set[str]:
        # Index `parents:` entries name types too, and Unpack dereferences them
        # before any manifest entry is read.
        names = set(self.manifest.values())
        names |= {p.name for lst in self.parents.values() for p in lst}
        return names

    def AsView(self, mask: set[Path], invert=False):
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
        for p in sorted(self._mask):
            inst = self._original.Get(p)
            yield p, inst.dtype_name, inst.dtype

    def _prune_whitelist(self) -> set:
        return set()

    def PruneTypes(self, save: bool=True, whitelist: set|None=None):
        wl = set(whitelist) if whitelist else set()
        return self._original.PruneTypes(save=save, whitelist=wl|self._prune_whitelist())

    def PrepTransfer(self, dest: Source, mover: Logistics|None=None, image_root: Path|None=None):
        # The masked half of the library, and only it. Materialized once and
        # queued as a single transfer, so the SSH executor still opens one
        # rsync per library rather than one per file.
        o = self._original
        o._ensure_saved()
        for p, _name, _dtype in self.Iterate():
            assert p.is_absolute() or (o.location/p).exists(), f"file not found [{p}]"
        drop = {p for p in o.manifest if p not in self._mask}
        if image_root is None:
            image_root = Path(tempfile.mkdtemp(prefix="msm.image."))
        image_root = Path(image_root)
        skipped = o.MaterializeImage(image_root, drop)
        # The manifest is never narrowed -- the library key is a hash of it --
        # so every type the manifest names has to survive. The mask narrows the
        # whitelist instead, which is where a transform library's 28 namespaces
        # collapse to the handful its kept transforms declare.
        # Not check_pinned_stamps: the image was written a moment ago and the
        # witness is about the library it came from, not about this copy.
        image = type(o).Load(image_root, check_pinned_stamps=False)
        if not image.is_pinned:
            image.PruneTypes(save=True, whitelist=self._prune_whitelist())
        Log.Info(
            f"staging [{o.location.name}] as [{len(self._mask)}] of"
            f" [{len(o.manifest)}] entries; [{skipped}] skipped"
        )
        if mover is None:
            mover = Logistics()
        mover.QueueTransfer(src=Source.FromLocal(image_root), dest=dest)
        return mover

    def SaveAs(self, dest: Source, label: str|None=None):
        with TemporaryDirectory(prefix="msm.image.") as tmp:
            mover = self.PrepTransfer(dest, image_root=Path(tmp)/self._original.location.name)
            res = mover.ExecuteTransfers(label=label)
        assert len(res.completed) == 1, f"move failed"
        return res
