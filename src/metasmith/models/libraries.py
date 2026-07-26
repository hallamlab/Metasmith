from __future__ import annotations
import os, sys
import subprocess
import shutil
from pathlib import Path
import yaml
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Callable, Iterable
from importlib import reload, __import__
import math
import re
import shlex
import tempfile
import time
from datetime import timedelta
import json

from ..serialization import IsText
from ..env import ContainerDef, Environment, Runtime
from ..env.dispatch_scan import EnvScan, ScanFile
from ..coms.terminals import RemoveLeadingIndent
from ..coms.ipc import GenerateId
from ..env import Shell
from .solver import Dependency, Endpoint, Transform
from .remote import Logistics, Source, SourceType
from ..hashing import KeyGenerator
from ..logging import Log
from ..constants import VERSION, MODULE_PATH, AgentPaths

def yaml_safe_load(p: Path):
    MAX = 5
    for i in range(MAX):
        try:
            with open(p) as f:
                s = f.read()
            # assert len(s) > 0, f"DataTypeLibrary at [{path}] is empty"
            d = yaml.safe_load(s)
        except OSError as e:
            # Under SLURM array fan-out the shared /msm_home bind can shed reads
            # with errno 108 (ESHUTDOWN, "transport endpoint shutdown"); retry the
            # same backoff we use for empty parses instead of dropping the task.
            Log.Warn(f"{i+1} of {MAX}, error reading yaml [{p}]: {e}")
            time.sleep(1)
            continue
        if d is not None: return d
        Log.Warn(f"{i+1} of {MAX}, failed to load yaml [{p}]")
        time.sleep(1)
    assert False, f"failed to load yaml [{p}]"

@dataclass
class DataTypeOntology:
    name: str
    version: str
    doi: str
    strict: bool

    def Pack(self):
        d = {}
        for k, v in self.__dict__.items():
            if k.startswith("_"): continue
            d[k] = v
        return d

    @classmethod
    def Unpack(cls, d: dict):
        return cls(**d)

class DataTypeOntologies:
    EDAM = DataTypeOntology(
        name = "EDAM",
        version = "1.25",
        doi = "https://doi.org/10.1093/bioinformatics/btt113",
        strict = False,
    )

# caches by absolute path
# _dataTypeLibrary_cache: dict[Path, DataTypeLibrary] = {}
# _dataTypeLibrary_history: list[str] = []

@dataclass
class DataTypeLibrary:
    schema: str = "v1"
    ontology: DataTypeOntology = field(default_factory=lambda: DataTypeOntologies.EDAM)
    types: dict[str, Endpoint] = field(default_factory=dict)

    # def __post_init__(self):
    #     if self.source is None: return
    #     _dataTypeLibrary_cache[self.source] = self

    def __getitem__(self, key: str) -> Endpoint:
        return self.types[key]

    def __setitem__(self, key: str, value: Endpoint):
        assert isinstance(value, Endpoint)
        assert isinstance(key, str)
        self.types[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self.types

    def __iter__(self):
        for k, v in self.types.items():
            yield k, v

    def __len__(self) -> int:
        return len(self.types)

    @classmethod
    def Unpack(cls, d: dict):
        raw_types = {}
        def pluralize(vv):
            def _fix(_v):
                if isinstance(_v, set): return _v
                if isinstance(_v, list): return set(_v)
                return {_v}
            return {k:_fix(v) for k, v in vv.items()}
        for type_name, type_raw in d["types"].items():
            extends = type_raw.get("extends", [])
            if isinstance(extends, str): extends = [extends]
            assert Endpoint.PROPERTY_FIELD in type_raw, f"[{type_name}] is missing [{Endpoint.PROPERTY_FIELD}]"
            props = type_raw[Endpoint.PROPERTY_FIELD]
            if isinstance(props, list) or isinstance(props, set):
                props = set(props)
                for pk in extends:
                    props |= raw_types[pk][Endpoint.PROPERTY_FIELD]
                raw_types[type_name] = props
            else:
                props = {}
                todo = [
                    raw_types[pk][Endpoint.PROPERTY_FIELD]
                    for pk in extends
                ]+[
                    pluralize(type_raw[Endpoint.PROPERTY_FIELD])
                ]
                for vk, vv in [entry for _props in todo for entry in _props.items()]:
                    props[vk] = props.get(type_name, set())|set(vv)
                props = {k:list(v) for k, v in props.items()}
            raw_types[type_name] = {Endpoint.PROPERTY_FIELD:props}
        params: dict = dict(
            types={k: Endpoint.Unpack(v) for k, v in raw_types.items()},
        )
        if "schema" in d:
            params["schema"] = str(d["schema"])
        if "ontology" in d:
            params["ontology"] = DataTypeOntology.Unpack(d["ontology"])
        return cls(**params)

    @classmethod
    def Load(cls, path: Source|str|Path) -> DataTypeLibrary:
        def _load(path: Path):
            return cls.Unpack(yaml_safe_load(path))

        if isinstance(path, Source):
            src = path
            if src.type not in {SourceType.DIRECT, SourceType.SYMLINK}:
                with tempfile.TemporaryDirectory() as tmpdir:
                    tmpdir = Path(tmpdir)
                    mover = Logistics()
                    _, salt = KeyGenerator.FromStr(src.address)
                    dest = Source.FromLocal(tmpdir/f"metasmith_datatypes.{salt}")
                    mover.QueueTransfer(src, dest)
                    res = mover.ExecuteTransfers()
                    assert len(res.completed) == 1, f"failed to load datatype library from [{src.address}], [{res.errors}]"
                    return _load(dest.GetPath())
            else:
                return _load(src.GetPath())
        else:
            return _load(Path(path))

    def Pack(self):
        return dict(
            schema=self.schema,
            ontology=self.ontology.Pack(),
            types={k: v.Pack() for k, v in self.types.items()},
        )

    def Save(self, path: Path):
        with open(path, "w") as f:
            yaml.safe_dump(self.Pack(), f)

@dataclass
class DataInstance:
    path: Path
    dtype: Endpoint
    dtype_name: str
    parent_lib: DataInstanceLibrary

    def __post_init__(self):
        self.RecalculateKey()
        # assert self.path.is_absolute(), f"path must be absolute [{self.path}]"

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DataInstance) and self.instance_id == other.instance_id

    def RecalculateKey(self):
        self._hash, self.instance_id = KeyGenerator.FromStr("".join([
            str(self.path),
            self.dtype_name,
            self.parent_lib.GetKey(),
        ]), l=10)
        # Legacy typed key is kept for backward compatibility when reading old
        # task serializations that referenced DataInstances by the old key.
        _, self.legacy_key = KeyGenerator.FromStr("".join([
            str(self.path),
            self.dtype.key,
            self.dtype_name,
        ]), l=8)
        self._key = self.instance_id
        return self._key

    def WithDType(self, dtype: Endpoint, dtype_name: str | None = None):
        return self.__class__(
            path=self.path,
            dtype=dtype,
            dtype_name=self.dtype_name if dtype_name is None else dtype_name,
            parent_lib=self.parent_lib,
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
        return dict(
            path=str(self.path),
            type=f"{self.parent_lib.GetKey()}::{self.dtype_name}",
            # type=f"{self.dtype_name}",
            type_id=self.dtype.key,
            instance_id=self.instance_id,
        )

    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataInstanceLibrary]):
        lib_key, namespace, dtype_name = raw["type"].split("::")
        lib = libraries[lib_key]
        dtype = lib.types[namespace][dtype_name]

        inst = cls(
            path=Path(raw["path"]),
            dtype=dtype,
            dtype_name=f"{namespace}::{dtype_name}",
            parent_lib=lib,
        )
        if "instance_id" in raw:
            # Preserve compatibility with newer serializations.
            inst.instance_id = raw["instance_id"]
            inst._key = inst.instance_id
            inst._hash, _ = KeyGenerator.FromStr(inst.instance_id, l=10)
        return inst

class DataInstanceLibrary:
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
        self.parents: dict[Path, list[DataInstanceLibrary.ParentMetadata]] = {}
        self._endpoint_cache: dict[Path, Endpoint] = {}
        if isinstance(location, DataInstanceLibrary):
            other = location
            self.location = other.location
            self.manifest = other.manifest
            self.types = other.types
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
            if cache_key not in _desc_cache:
                _desc_cache[cache_key] = _get_all_descendants(ancestors | {path})
            siblings = _desc_cache[cache_key]
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

    def AddItem(self, path: Path|str, dtype: str, parents: Iterable[Path]|None=None):
        if parents is None:
            parents = []
        for p in parents:
            assert p in self.manifest
            # if not p.is_absolute(): p = self.location/p
            # assert p.exists(), f"parent [{p}] doesn't exist"
        path = Path(path)
        assert path not in self.manifest, f"[{path}] already added"
        type_model = self.GetType(dtype) # check if datatype exists
        self.manifest[path] = dtype
        self.AddParentsTo(path, [self.Get(p) for p in parents])
        self._invalidate_endpoint_cache()
        return path

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
        self._invalidate_endpoint_cache()

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

    def Pack(self):
        def _pack_instance(path, dtype_name):
            grandparents = set()
            for p in self.parents.get(path, []):
                for gp in self.parents.get(p.path, []):
                    grandparents.add(gp.path)
            d_parents = {}
            for p in self.parents.get(path, []):
                if p.path in grandparents: continue
                p.library_key
                k = f"{p.library_key}@{p.path}"
                v = p.name
                d_parents[k] = v
            d = dict(
                type=dtype_name,
            )
            if len(d_parents) > 0:
                d["parents"] = dict(sorted(d_parents.items(), key=lambda t:t[0]))
            return d
        man = {str(k):_pack_instance(k, v) for k, v in self.manifest.items()}
        man = dict(sorted(man.items(), key=lambda t: t[0]))
        packed = dict(
            schema=self.schema,
            manifest=man,
            remote_src=self.remote_src.Pack() if self.remote_src is not None else None,
        )
        return {k:v for k, v in packed.items() if v is not None}

    @classmethod
    def Unpack(cls, location: Path, raw: dict, dtypes: dict[str, DataTypeLibrary], check_integrity: bool=False):
        if "manifest" not in raw:
            raise ValueError(
                f"library index at [{location/cls._path_to_meta/(cls._index_name+cls._metadata_ext)}] "
                f"is malformed: missing 'manifest' key. "
                f"Was this directory compiled with `metasmith build`?"
            )
        manifest = {}
        for k, v in raw["manifest"].items():
            type_name = v["type"]
            if check_integrity:
                assert (location/k).exists(), f"[{k}], does not exist"
            cls._get_type(type_name, dtypes) # check if datatype exists
            manifest[Path(k)] = type_name
        lib = cls(
            location=location,
        )
        lib.schema = raw["schema"]
        lib.manifest = manifest
        remote_src = raw.get("remote_src")
        lib.remote_src = Source.Unpack(remote_src) if remote_src is not None else None
        # First pass: Build immediate parents for all items
        for k, v in raw["manifest"].items():
            parents: dict[Path, DataInstanceLibrary.ParentMetadata] = {}
            for p_key, p_name in v.get("parents", {}).items():
                lib_key, p_path_str = p_key.split("@", maxsplit=1)
                p_path = Path(p_path_str)
                namespace, dtype_name = p_name.split("::")
                _lib = dtypes[namespace]
                dtype = _lib.types[dtype_name]
                parents[p_path] = DataInstanceLibrary.ParentMetadata(
                    dtype=dtype,
                    name=p_name,
                    library_key=lib_key,
                    path=p_path,
                )
            if len(parents) > 0:
                lib.parents[Path(k)] = list(parents.values())

        # Second pass: Memoized transitive closure for full ancestor aggregation
        ancestor_cache: dict[Path, dict[Path, DataInstanceLibrary.ParentMetadata]] = {}

        def _get_all_ancestors(k_path: Path) -> dict[Path, DataInstanceLibrary.ParentMetadata]:
            if k_path in ancestor_cache:
                return ancestor_cache[k_path]
            ancestors: dict[Path, DataInstanceLibrary.ParentMetadata] = {}
            for p in lib.parents.get(k_path, []):
                ancestors[p.path] = p
                for gp_path, gp in _get_all_ancestors(p.path).items():
                    if gp_path not in ancestors:
                        ancestors[gp_path] = gp
            ancestor_cache[k_path] = ancestors
            return ancestors

        for k in raw["manifest"].keys():
            k_path = Path(k)
            if k_path not in lib.parents:
                continue
            lib.parents[k_path] = list(_get_all_ancestors(k_path).values())

        return lib

    def Save(self, update_types=True):
        ext = self._metadata_ext
        types_path = self.location/self._path_to_types
        types_path.mkdir(parents=True, exist_ok=True)
        for namespace, types_lib in self.types.items():
            local_path = types_path/(namespace+ext)
            if not update_types and local_path.exists(): continue
            types_lib.Save(local_path)

        metadata_path = self.location/self._path_to_meta
        metadata_path.mkdir(parents=True, exist_ok=True)
        index_path = metadata_path/(self._index_name+ext)
        index_path.parent.mkdir(parents=True, exist_ok=True)
        with open(index_path, "w") as f:
            yaml.dump(self.Pack(), f)

    @classmethod
    def Load(cls, path: Path|str, check_integrity=False):
        path = Path(path)
        ext = cls._metadata_ext
        meta_path = path/cls._path_to_meta
        types_path = path/cls._path_to_types
        index_path = meta_path/(cls._index_name+ext)
        assert path.exists(), f"path [{path}] does not exist"
        assert index_path.exists(), f"index file [{index_path}] does not exist"

        dtypes = {}
        for p in types_path.iterdir():
            if p.is_dir(): continue
            if p == index_path: continue
            k = p.relative_to(types_path).with_suffix("")
            k = str(k)
            dtypes[k] = DataTypeLibrary.Load(p)

        d = yaml_safe_load(index_path)
        self = cls.Unpack(location=path, raw=d, dtypes=dtypes, check_integrity=check_integrity)
        self.types = dtypes
        self._calculate_key(_raw_override=d)
        return self

    def PrepTransfer(self, dest: Source, mover: Logistics|None=None):
        self.Save()
        for p, name, dtype in self.Iterate():
            assert p.is_absolute() or (self.location/p).exists(), f"file not found [{p}]"
        if mover is None:
            mover = Logistics()
        mover.QueueTransfer(
            src=Source.FromLocal(self.location),
            dest=dest,
        )
        return mover

    def SaveAs(self, dest: Source, label: str|None=None):
        mover = self.PrepTransfer(dest)
        res = mover.ExecuteTransfers(label=label)
        assert len(res.completed) == 1, f"move failed"
        return res

    @classmethod
    def LoadFrom(cls, src: Source, dest: Path|str, as_image=True, on_exist: str = "skip", label: str|None=None, resolve_symlinks: bool=True):
        assert isinstance(src, Source)
        assert on_exist in {"skip", "error", "clear", "update"}
        if not isinstance(dest, Path):
            dest = Path(dest)

        def _transfer():
            mover = Logistics()
            if as_image:
                _src = src/cls._path_to_meta
                _dest = dest/cls._path_to_meta
            else:
                _src, _dest = src, dest
            mover.QueueTransfer(
                src=_src,
                dest=Source.FromLocal(_dest),
            )
            res = mover.ExecuteTransfers(label=label, resolve_symlinks=resolve_symlinks)
            assert len(res.completed) == 1, f"move failed"
        if dest.exists():
            if on_exist == "error":
                raise FileExistsError(f"[{dest}] already exists")
            elif on_exist == "update":
                _transfer()
            elif on_exist == "clear":
                Log.Warn("clearing previously loaded library")
                shutil.rmtree(dest)
                _transfer()
            elif on_exist == "skip":
                pass
        else:
            _transfer()

        lib = cls.Load(dest, check_integrity=False)
        if as_image:
            lib.remote_src = src
            lib.Save()
        return lib

    def Consolidate(self):
        digits = len(f"{len(self.manifest)}")
        new_paths: dict[Path, Path] = {}
        for i, p in enumerate(self.manifest):
            if not p.is_absolute(): continue
            local_link = Path(f"{i+1:0{digits}}_{p.name}")
            new_paths[p] = local_link
            lp = self.location/local_link
            if lp.exists(): continue
            lp.symlink_to(p, p.is_dir())
        # self.manifest = {new_paths.get(k, k):v for k, v in self.manifest.items()}
        return new_paths

    def ActualizeRemote(self, extern_dest: Source|None=None, label: str|None=None):
        if self.remote_src is None:
            return self
        _lib = None
        try:
            _lib = self.Load(self.location, check_integrity=True)
            return _lib
        except AssertionError:
            pass
        if _lib is None: # so that errors don't stack
            mover = Logistics()
            if extern_dest is None:
                extern_dest = Source.FromLocal(self.location)
            mover.QueueTransfer(
                src=self.remote_src,
                dest=extern_dest,
            )
            res = mover.ExecuteTransfers(label=label)
            assert len(res.completed) == 1, f"failed to load library from [{self.remote_src}]; [{res.errors}]"
        _lib = self.Load(self.location, check_integrity=True)
        return _lib
    
    def LocalizeContents(self):
        to_move = {}
        for path in self.manifest:
            if not path.is_absolute(): continue
            k = path.name
            to_move[k] = to_move.get(k, [])+[path]
        mover = Logistics()
        moved: list[tuple[Path, Path]] = []
        for dest, srcs in to_move.items():
            dest = Path(dest)
            plural = len(srcs)>1
            for i, src in enumerate(srcs):
                if plural:
                    dest_path = self.location/f"{dest.stem}_{i+1}{dest.suffix}"
                else:
                    dest_path = self.location/f"{dest.stem}{dest.suffix}"
                mover.QueueTransfer(src=Source.FromLocal(src), dest=Source.FromLocal(dest_path))
                moved.append((Path(src), dest_path.relative_to(self.location)))
        mover.ExecuteTransfers()
        for src, dest in moved:
            self.manifest[dest] = self.manifest[src]
            del self.manifest[src]
            if src in self.parents:
                self.parents[dest] = self.parents[src]
                del self.parents[src]
        return moved

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

class TransformInstanceLibraryView(DataInstanceLibraryView):
    """Masked view of a TransformInstanceLibrary. Mirrors DataInstanceLibrary.AsView:
    mask is a set of relative .py paths; invert=True flips include/exclude."""
    _original: "TransformInstanceLibrary"

    def IterateTransforms(self):
        for p in self._mask:
            tr = self._original.GetTransform(p)
            assert tr is not None, p
            yield p, tr

    def GetTransform(self, path: str|Path, reload: bool=False):
        p = Path(path)
        if p.suffix != ".py":
            p = p.with_suffix(".py")
        assert p in self._mask, f"transform [{p}] is hidden by view mask"
        return self._original.GetTransform(p, reload=reload)

    @property
    def types(self):
        return self._original.types

    @property
    def location(self):
        return self._original.location

    def GetType(self, name: str):
        return self._original.GetType(name)

    def GetName(self, endpoint):
        return self._original.GetName(endpoint)

@dataclass
class Size:
    value_gb: float
    strict: bool=False

    def __str__(self) -> str:
        return self.AsNextflowFormat()

    @classmethod
    def TB(cls, val: float):
        return cls(value_gb=val*1024)

    @classmethod
    def GB(cls, val: float, strict: bool=False):
        return cls(value_gb=val)

    @classmethod
    def MB(cls, val: float, strict: bool=False):
        return cls(value_gb=val/1024)

    @classmethod
    def KB(cls, val: float, strict: bool=False):
        return cls(value_gb=val/(1024**2))
    
    def SetStrict(self):
        self.strict=True
        return self

    def AsNextflowFormat(self):
        return f"'{self.value_gb:0.2f} GB'"

class Duration:
    def __init__(self, days: float=0, seconds: float=0, microseconds: float=0,
                milliseconds: float=0, minutes: float=0, hours: float=0, weeks: float=0) -> None:
        self._delta = timedelta(
            days=days, seconds=seconds, microseconds=microseconds,
            milliseconds=milliseconds, minutes=minutes, hours=hours, weeks=weeks
        )
        self.strict=False
    
    def __str__(self) -> str:
        return self.AsNextflowFormat()

    def SetStrict(self):
        self.strict=True
        return self

    def AsNextflowFormat(self):
        delta = self._delta
        total_seconds = delta.total_seconds()
        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        microseconds = delta.microseconds
        sd = f"{days}day{'s' if days!=1 else ''}"
        sh = f"{hours}hours"
        sm = f"{minutes}minutes"
        ss = f"{seconds}seconds"
        s = [x for x, v in zip([sd, sh, sm, ss], [days, hours, minutes, seconds]) if v>0]
        if len(s) == 0: s = [ss]
        return f"'{' '.join(s)}'"

class Gpus(Enum):
    # A pure toggle: whether the transform's tool needs a GPU, and how badly.
    # Deliberately carries no count and no device type -- how many devices a
    # given VRAM ask resolves to, and what a device is called, are facts about
    # the *host*, not the tool. Those live on `Gpu` (the run-side declaration).
    NONE = "none"
    OPTIONAL = "optional"
    REQUIRED = "required"

# Label attached at stage time to every process whose transform declared a GPU.
# Follows the existing `label 'x<name>x'` convention (see `xlocalx` in slurm.nf).
GPU_LABEL = "gpu"

@dataclass
class Gpu:
    """What a GPU *is* on the target host — the run-side half of the contract.

    Declared once per run via `Agent.RunWorkflow(gpus=...)`. This is the only
    place device vocabulary appears: per-device VRAM, the site's device/gres
    type token, how many devices a node has, and the scheduler flag shape used
    to ask for them. A transform never names any of these.

    `flag` is the request syntax including its separator, so the count appends
    directly: `--gpus-per-node=` -> `--gpus-per-node=2`, `--gres=gpu:` ->
    `--gres=gpu:2` (or `--gres=gpu:a100:2` when `type` is set).
    """
    memory: Size|None = None
    type: str|None = None
    count: int|None = None
    flag: str = "--gpus-per-node="
    # Scheduler flags a GPU step needs beyond the device count -- typically the
    # GPU partition, since a site's default partition has no cards. These go on
    # GPU steps only, which is what distinguishes them from
    # `params.process.clusterOptionsExtra` (every step). Sockeye needs
    # ["--partition=gpu"].
    extra: list[str] = field(default_factory=list)

    def DevicesFor(self, required: Size|None) -> int:
        # How many of *this* device it takes to total `required` VRAM. No ask
        # (or no declared per-device memory to divide by) means one device --
        # the transform said it wants a GPU without saying how much.
        if required is None or self.memory is None: return 1
        if self.memory.value_gb <= 0: return 1
        return max(1, math.ceil(required.value_gb / self.memory.value_gb))

    def MakeRequestFlag(self, devices: int) -> str:
        req = f"{self.flag}{self.type}:{devices}" if self.type else f"{self.flag}{devices}"
        return " ".join([req, *self.extra])

@dataclass
class Resources:
    cpus: int|None = None
    memory: Size|None = None
    duration: Duration|None = None
    # GPU need. `gpus` is the toggle; `gpu_memory` is the TOTAL VRAM the tool
    # needs, which is the unit a tool actually cares about. Unlike the three
    # fields above, `gpu_memory` is NOT a Nextflow directive -- Nextflow has no
    # VRAM concept -- so AsNextflowFormat never renders it. It is metasmith-side
    # input to the run-time device-count computation and to the protocol.
    gpus: Gpus = Gpus.NONE
    gpu_memory: Size|None = None

    @property
    def wants_gpu(self) -> bool:
        return self.gpus is not Gpus.NONE

    def AsNextflowFormat(self, is_config=False):
        def _parse_res(r:int|Duration|Size|None, var: str, field: str, norm: str, strict: str=""):
            if r is None: return None
            rval = str(r)
            if not isinstance(r, int):
                is_strict = r.strict
            else:
                is_strict = False
            if is_strict:
                val = strict.replace(var, rval)
            else:
                val = norm.replace(var, rval)
            joiner = " = " if is_config else " " # why is nextflow inconsistent like this??
            return f"{field}{joiner}{val}"
        # return [x for x in [
        #     _parse_res(self.cpus, "<x>", "cpus", "<x>"),
        #     _parse_res(
        #         self.memory, "<x>", "memory",
        #         "{"+f" task.attempt==1? <x> : 2*(<x> as MemoryUnit) "+"}",
        #         "<x>",
        #     ),
        #     _parse_res(
        #         self.duration, "<x>", "time",
        #         "{"+f" task.attempt==1? <x> : 2*(<x> as Duration) "+"}",
        #         "<x>",
        #     ),
        # ] if x is not None]
        return [x for x in [
            _parse_res(self.cpus, "<x>", "cpus", "<x>"),
            _parse_res(
                self.memory, "<x>", "memory",
                "{"+f" (2**(task.attempt-1)) * (<x> as MemoryUnit) "+"}",
                "<x>",
            ),
            _parse_res(
                self.duration, "<x>", "time",
                "{"+f" (2**(task.attempt-1)) * (<x> as Duration) "+"}",
                "<x>",
            ),
        ] if x is not None]

# this should function like a view provided by the parent library
@dataclass
class TransformInstance:
    protocol: Callable[[ExecutionContext], ExecutionResult|list[ExecutionResult]]
    model: Transform
    group_by: Dependency
    name: str|None = None
    resources: Resources|None = None
    batch_size: int = 1
    labels: list[str] = field(default_factory=list)
    _path: Path = field(default_factory=Path)
    _key: str = ""
    _hash: int = -1
    # Which worlds this transform declared it can run in, read statically off
    # its own source at load. Set by `Load`; None when the source could not be
    # scanned, which is distinct from "declared nothing" and must not be read
    # as a portability answer.
    _env_scan: "EnvScan|None" = None
    # The Dependency each arm named as its `env=`, resolved through the module
    # globals. Lets stage time find the env *resource* behind an arm and read
    # which of `container:` / `conda:` it actually carries.
    _env_deps: list[Dependency] = field(default_factory=list)

    def __post_init__(self):
        assert self.batch_size>0, self.model
        assert self.group_by in self.model.requires, self.model
        for k, vt in [
            ("protocol", Callable),
            ("model", Transform),
        ]:
            v = getattr(self, k)
            assert isinstance(v, vt), f"[{k}] must be of type [{vt}] but got [{type(v)}]"
        # assert len(self.output_signature) == len(self.model.produces), f"output signature length must match model produces length [{len(self.output_signature)} != {len(self.model.produces)}]"
        # for sig_group, m_group in zip(self.output_signature, self.model.produces):
        #     for d, p in sig_group.items():
        #         assert isinstance(d, Dependency), f"output signature key must be of type [Dependency] but got [{type(d)}]"
        #         assert d in m_group, f"output signature value must be added to model"
        #     for dep in m_group:
        #         assert dep in sig_group, f"model output missing in signature [{dep}]"
        TransformInstance._last_loaded_transform = self

    def GetKey(self):
        return self._key # from definition file upon load

    def __hash__(self) -> int:
        return self._hash # from definition file upon load

    @classmethod
    def Load(cls, parent_lib: Path, definition: Path) -> TransformInstance|None:
        cls._last_loaded_transform: TransformInstance | None = None

        original_path_var = sys.path
        sys.path = [str(parent_lib/definition.parent)]+sys.path
        try:
            m = __import__(f"{definition.stem}")
            reload(m)
            assert cls._last_loaded_transform is not None
            tr = cls._last_loaded_transform
            tr.name = definition.stem
            tr._path = definition
            # Read the ExecWithEnv declarations off the source now, while the
            # file and the module namespace are both in hand. `env=` is written
            # as a bare name bound to a Dependency at module scope, so the
            # identifier the scan returns resolves straight through the module.
            try:
                tr._env_scan = ScanFile(parent_lib/definition)
            except (OSError, SyntaxError) as e:
                Log.Warn(f"could not scan env declarations in [{definition}]: {e}")
                tr._env_scan = None
            if tr._env_scan is not None:
                seen: list[Dependency] = []
                for chain in tr._env_scan.chains:
                    for name in chain.envs:
                        if name is None: continue
                        d = getattr(m, name, None)
                        if isinstance(d, Dependency) and d not in seen:
                            seen.append(d)
                tr._env_deps = seen
            # with open(definition) as f:
            #     raw = "".join(f.readlines())
            #     h, k = KeyGenerator.FromStr(raw, l=5)
            #     tr._hash, tr._key = h, k
            # use the transform model hash, 
            # since updates to script should be able to use the existing nxf cache
            tr._hash, tr._key = tr.model.hash, tr.model.key
            return cls._last_loaded_transform
        finally:
            sys.path = original_path_var

class TransformInstanceLibrary(DataInstanceLibrary):
    def __init__(self, location: Path|str|DataInstanceLibrary) -> None:
        super().__init__(location)
        if "transforms" not in self.types:
            transform_types = DataTypeLibrary(types={
                "transform":        Endpoint({"metasmith", "transform"}),
                "example input":    Endpoint({"metasmith", "example input"}),
                "example output":   Endpoint({"metasmith", "example output"}),
            })
            self.AddTypeLibrary(namespace="transforms", lib=transform_types)
        self._transform_cache: dict[Path, TransformInstance] = {}

    def PruneTypes(self, save: bool=True):
        indirect_whitelist: list[Dependency] = []
        for path, tr in self.IterateTransforms():
            indirect_whitelist += tr.model.requires
            indirect_whitelist += [i for g in tr.model.produces for i in g]
        def _in(x: Dependency):
            return any(x.properties == d.properties for g in self.types.values() for d in g.types.values())
        super().PruneTypes(save=save, whitelist={x for x in indirect_whitelist if _in(x)})

    def AddStub(self, path: Path|str, exist_ok: bool=True):
        path = Path(path)
        assert not path.is_absolute(), f"path must be relative"
        path = self.location/path
        example = MODULE_PATH/"models/_example_transform.py"
        if path.suffix != ".py":
            path = path.parent/(path.name+".py")
        if path.exists():
            if not exist_ok:
                raise FileExistsError(f"file exists [{path}]")
        else:
            shutil.copy(example, path, follow_symlinks=True)
        self.AddItem(path.relative_to(self.location), "transforms::transform")
        # results = self.AddBulk([(example, path, "transforms::transform")], on_exist="skip" if exist_ok else "error")
        # assert len(results) == 1, f"failed to add transform at [{path}]"
        self.Save()
        inst = TransformInstance.Load(self.location, path)
        return inst

    @classmethod
    def ResolveParentLibrary(cls, transform_definition_file: Path|str):
        path = Path(transform_definition_file)
        for p in path.parents:
            if (p/DataInstanceLibrary._path_to_meta).exists():
                return cls.Load(p)
        assert False

    def __getitem__(self, transform: Path|str):
        return self.GetTransform(transform)

    def GetTransform(self, path: Path|str, reload=False) -> TransformInstance:
        path = Path(path)
        if path.suffix != ".py":
            path = path.with_suffix(".py")
        if reload or path not in self._transform_cache:
            tr = TransformInstance.Load(self.location, path)
            assert tr is not None
            self._transform_cache[path] = tr
        return self._transform_cache[path]

    def IterateTransforms(self):
        for k, dtype_name, dtype in self.Iterate():
            tr = self.GetTransform(k)
            assert tr is not None, (dtype_name, k)
            yield k, tr

    def AsView(self, mask: set[Path], invert: bool=False):
        """if invert=True, then items in mask are excluded"""
        return TransformInstanceLibraryView(self, mask, invert)

    @classmethod
    def Load(cls, path: Path|str):
        return cls(DataInstanceLibrary.Load(path))

    @classmethod
    def LoadFrom(cls, src: Source, dest: Path, label: str|None=None):
        return cls(DataInstanceLibrary.LoadFrom(src, dest, label=label))

# ContextPath has moved to metasmith.models.paths; re-export to preserve
# the existing `from metasmith.models.libraries import ContextPath` form.
from .paths import ContextPath, PathMap  # noqa: E402,F401

@dataclass
class ContextData:
    input_group: list[ContextPath]
    endpoint: Endpoint
    type_name: str
    path: ContextPath = field(init=False)

    def __post_init__(self) -> None:
        assert len(self.input_group)>0
        self.path = self.input_group[0]

@dataclass
class ExecutionResult:
    success: bool = True
    manifest: list[dict[Dependency, Path]] = field(default_factory=list)

class ExecutionFailed(Exception):
    pass

def ResolveEnvImage(content: str, runtime: Runtime, source: str|Path="<env>") -> str:
    """Resolve a generic env-declaration file's content to the image / env-name
    the active runtime should use.

    The generic format is a YAML mapping with an optional ``container:`` (a
    ``docker://…`` URI, used by the container runtimes) and/or an optional
    ``conda:`` (a conda/mamba env name, used by ``Runtime.MAMBA``). Selection is
    by the single global runtime; a missing key for the selected runtime is a
    hard error naming the file.

    Legacy resources (``*.oci`` whose whole content is a bare URI) parse as a
    YAML scalar, not a mapping, and are treated verbatim as the container image
    so existing container runs keep working unchanged.
    """
    try:
        parsed = yaml.safe_load(content)
    except yaml.YAMLError:
        parsed = None
    if isinstance(parsed, dict):
        key = "conda" if runtime == Runtime.MAMBA else "container"
        value = parsed.get(key)
        assert value, (
            f"env declaration [{source}] has no '{key}:' entry for runtime "
            f"[{runtime.value}] (keys present: {sorted(parsed)})"
        )
        return str(value).strip()
    # legacy bare-URI (*.oci) or unparseable content -> use verbatim as the image
    return content.strip()

CONTAINER_ARM = "ifContainerDo"
VIRTUAL_ENV_ARM = "ifVirtualEnvDo"

# Names whose meaning belongs to the framework, not the transform. Overwriting
# any of these from a protocol reshapes the environment metasmith just built
# (PATH/LD_LIBRARY_PATH), relocates the workdir the bounce script cd'd into
# (PWD), or redirects scratch the runtime already agreed on (TMPDIR, HOME).
# Tool-native names like GTDBTK_DATA_PATH are exactly what exports are for.
_RESERVED_EXPORTS = frozenset({"PATH", "HOME", "LD_LIBRARY_PATH", "TMPDIR", "PWD"})

def _validate_exports(exports: dict[str, "str|Path"]|None) -> dict[str, str]:
    if not exports: return {}
    out: dict[str, str] = {}
    for k, v in exports.items():
        k = str(k)
        assert k not in _RESERVED_EXPORTS, (
            f"export [{k}] is reserved by the framework; "
            f"reserved names are {sorted(_RESERVED_EXPORTS)}"
        )
        assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", k), (
            f"export name [{k}] is not a valid shell identifier"
        )
        out[k] = str(v)
    return out


class EnvDispatch:
    """The chain returned by :meth:`ExecutionContext.ExecWithEnv`.

    Holds no command of its own — each arm is dispatched the instant it is
    declared, if it matches. What it does hold is the record of which arms the
    transform declared, which is what makes the "no arm matched" failure
    reportable and what the stage-time portability manifest reads.
    """

    def __init__(self, context: "ExecutionContext"):
        self._context = context
        self.declared: list[str] = []
        self._matched: str|None = None

    @property
    def matched(self) -> str|None:
        return self._matched

    def _dispatch(self, arm: str, applies: bool, **kw):
        self.declared.append(arm)
        if not applies:
            Log.Info(f"skipping [{arm}] (does not apply to this runtime)")
            return self
        assert self._matched is None, (
            f"[{arm}] and [{self._matched}] both apply to this runtime; "
            "an ExecWithEnv chain must have exactly one matching arm"
        )
        self._matched = arm
        self._context._ExecInEnv(**kw)
        return self

    def ifContainerDo(
        self,
        env: Dependency,
        cmd: str,
        shell: str="bash",
        binds: list[tuple[Path|str, Path|str]]|None=None,
        args: list[str]|None=None,
        exports: dict[str, str|Path]|None=None,
        history: bool=True,
    ) -> "EnvDispatch":
        """How this step runs when the tool lives in a container image.

        `binds` are host->container mount pairs and `args` are extra flags for
        the runtime's own run command; both are meaningless without a mount
        namespace, which is why they live here and not on the venv arm.
        """
        return self._dispatch(
            CONTAINER_ARM, self._context._crosses_boundary,
            image=env, cmd=cmd, shell=shell, binds=binds, args=args,
            exports=exports, history=history,
        )

    def ifVirtualEnvDo(
        self,
        env: Dependency,
        cmd: str,
        shell: str="bash",
        exports: dict[str, str|Path]|None=None,
        history: bool=True,
    ) -> "EnvDispatch":
        """How this step runs when the tool is a package set on PATH.

        There is no mount namespace here, so there is no `binds` — accepting one
        could only mean ignoring it, which is the bug this API replaced. Hand the
        tool its paths directly (every ContextPath view is the host path under
        this runtime) or through `exports`.
        """
        return self._dispatch(
            VIRTUAL_ENV_ARM, not self._context._crosses_boundary,
            image=env, cmd=cmd, shell=shell, exports=exports, history=history,
        )


# work as if batch of 1 item
# until explicitly batch iterated
@dataclass
class ExecutionContext:
    _inputs: list[dict[Dependency, ContextData]]
    _get_output_paths: Callable[[Dependency, int, int], ContextPath]
    external_shell: Shell # relay shell for container runtimes, local shell otherwise
    external_cwd: Path
    external_agent_home: Path
    # The environment a *tool* runs in on this host. Private: a protocol has no
    # business branching on the runtime, and everything that used to require it
    # (GPU flags, bind dialect, whether there is a boundary at all) is answered
    # by the env package. Never `native` -- native describes whether metasmith
    # itself is containerized, which says nothing about the tool's own image.
    _environment: Runtime|Environment = Runtime.DOCKER
    params: dict = field(default_factory=dict)
    _batch_index: int = 0
    _detected_gpus: list|None = None
    _env_dispatches: list["EnvDispatch"] = field(default_factory=list)

    def __post_init__(self):
        # Accept a bare Runtime for the many construction sites that only have
        # one; normalise to an Environment so routing has a single shape.
        if isinstance(self._environment, Runtime):
            self._environment = Environment(image="", runtime=self._environment)

    @property
    def _crosses_boundary(self) -> bool:
        # Whether a tool launched from here lands on the other side of a
        # container boundary. The single question every arm dispatch turns on.
        assert isinstance(self._environment, Environment)
        return self._environment.needs_relay

    def _tool_environment(self, image: str, **kw) -> Environment:
        # The container half is rebuilt per call (each tool has its own image,
        # workdir and binds); runtime/native carry over from the template.
        container = replace(self._environment.container, **kw) if kw else self._environment.container
        return replace(self._environment, image=image, container=container)

    def GetMeta(self, key: Dependency):
        d = self._inputs[self._batch_index]
        if key in d:
            return d[key]
        raise KeyError(f"key [{key}:{key.key}] not found in [{set(str(x)+':'+x.key for x in d.keys())}]")

    def Input(self, key: Dependency):
        return self.GetMeta(key).path
    
    def InputGroup(self, key: Dependency):
        return self.GetMeta(key).input_group

    def Output(self, key: Dependency, i: int=0):
        return self._get_output_paths(key, i, self._batch_index)  

    def AsBatch(self):
        while self._batch_index < len(self._inputs):
            yield self
            self._batch_index += 1
        self._batch_index = 0

    def LocalShell(self, cmd: str):
        cmd = RemoveLeadingIndent(cmd)
        Log.Info(f"invoked local shell, calling subprocess.run() with:")
        for line in cmd.split("\n"):
            Log.Info(f"    {line}")
        subprocess.run(cmd, shell=True, executable='/bin/bash')

    def DeclaredGpus(self) -> tuple[Gpus, Size|None]:
        # What this step *asked* for at stage time. Staged by the generator into
        # the step meta file; absent (-> Gpus.NONE) for any step that declared
        # no GPU and for workspaces staged before GPU support existed.
        raw = self.params.get("gpus")
        if not isinstance(raw, dict): return Gpus.NONE, None
        try:
            toggle = Gpus(raw.get("gpus", Gpus.NONE.value))
        except ValueError:
            toggle = Gpus.NONE
        mem = raw.get("gpu_memory_gb")
        return toggle, None if mem is None else Size.GB(mem)

    def DetectGpus(self, refresh: bool=False) -> list[Size]:
        """Per-device VRAM of the GPUs this task actually got, on the exec host.

        Probes through `external_shell`, which is the relay for container
        runtimes and the local shell for mamba/native -- so the answer is about
        the machine the tool will run on, under every runtime. Reports what was
        *allocated*, not what was asked for: under a partial SLURM allocation or
        a MIG slice (where CUDA_VISIBLE_DEVICES is a MIG-<uuid> rather than an
        index) those differ, and the allocated figure is the one a tool sizing
        its own offload needs. A host with no nvidia-smi is a valid empty
        answer, not an error.

        Memoized: the answer cannot change within a task, and GetContainerModel
        consults it on every ExecWithEnv call. Pass refresh=True to probe
        again.
        """
        if self._detected_gpus is not None and not refresh:
            return list(self._detected_gpus)
        FLAG = "msm_gpu"
        probe = (
            'command -v nvidia-smi >/dev/null 2>&1 && '
            f'nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits 2>/dev/null'
            f' | sed "s/^/{FLAG} /" || true'
        )
        try:
            res = self.external_shell.Exec(probe, history=True)
        except Exception as e:
            Log.Warn(f"gpu detection failed: {e}")
            self._detected_gpus = []
            return []
        found: list[Size] = []
        for line in res.out:
            line = line.strip()
            if not line.startswith(FLAG): continue
            val = line[len(FLAG):].strip()
            try:
                found.append(Size.MB(float(val)))
            except ValueError:
                continue
        self._detected_gpus = found
        return list(found)

    def GetContainerModel(self, image: Dependency, binds: list[tuple[Path|str, Path|str]]|None=None, args: list[str]|None=None):
        path = self._inputs[self._batch_index][image].path
        if IsText(path.local):
            with open(path.local) as f:
                content = f.read()
            # Generic env declaration: select container: / conda: by the global
            # runtime (legacy bare-URI *.oci files resolve verbatim). __post_init__
            # has already normalised `_environment` to an Environment, so its
            # `runtime` is the single global runtime this agent was launched with.
            image_path = ResolveEnvImage(content, self._environment.runtime, path.local)
        else:
            image_path = str(path.external)
    
        # Probe whether this runtime crosses a container boundary. When it
        # does not (mamba/native), paths are identity: the tool runs on the
        # host filesystem in the real cwd, so there is no /ws remap and no
        # binds to compute. The PathMap views collapse to equal.
        _probe = self._tool_environment(str(image_path))

        _binds: list[tuple[Path, Path]] = []
        # Accumulating implicit input binds is dead work with no boundary --
        # and a half-computed bind list is exactly what invites a future
        # reader to "just use it" and reintroduce the silent-drop bug.
        for batch_item in (self._inputs if _probe.needs_relay else []):
            for _, v in list(batch_item.items()):
                for p in v.input_group:
                    if p.container.is_relative_to(AgentPaths.HOME_ROOT): continue
                    src = p.external.parent
                    if not p.container.is_absolute():
                        dest = src
                    else:
                        dest = p.container.parent
                    if not src.is_absolute() or not dest.is_absolute(): continue

                    found = False
                    for i, (a, b) in enumerate(_binds):
                        ac = Path(os.path.commonpath([a, src]))
                        bc = Path(os.path.commonpath([b, dest]))
                        THRES = 3 # '/', '1', '2' >> /1/2
                        if len(ac.parts)>=THRES:
                            found = True
                            break
                    if found:
                        _binds[i] = ac, bc
                    else:
                        _binds.append((src, dest))
        if binds is None: binds = []
        if _probe.needs_relay:
            container_ws = AgentPaths.WORK_ROOT
            binds += [
                ('${TMPDIR-"/tmp"}', '${TMPDIR-"/tmp"}'),
                (self.external_agent_home, AgentPaths.HOME_ROOT),
                (self.external_cwd, container_ws),
            ]
            binds += sorted([(s, d) for s, d in _binds])
        else:
            # No mount namespace to bind into. Callers must not reach here with
            # binds -- ExecWithEnv routes them through ifContainerDo, which only
            # runs under a container runtime.
            assert not binds, (
                f"binds are meaningless without a container boundary "
                f"(runtime [{_probe.runtime.name}]): {binds}"
            )
            container_ws = self.external_cwd

        extra_args = list(args) if args else []
        # A step that declared a GPU gets its runtime's GPU flags for free --
        # the transform author never writes `--nv` / `--gpus all`, and never
        # branches on the runtime to pick the dialect. Transforms that still
        # pass them by hand keep working: framework flags whose leading token is
        # already present in `args=` are dropped rather than duplicated.
        #
        # Gated on a device actually being present, not merely declared: a
        # Gpus.OPTIONAL step is expected to land on CPU-only hosts, and there
        # `docker run --gpus all` fails outright ("could not select device
        # driver"), turning a graceful fallback into a dead task.
        declared, _ = self.DeclaredGpus()
        if declared is not Gpus.NONE and self.DetectGpus():
            gpu_args = _probe.MakeGpuArgs()
            if gpu_args and gpu_args[0] not in extra_args:
                extra_args = gpu_args + extra_args

        env = self._tool_environment(
            str(image_path),
            workdir = container_ws,
            binds = binds,
            cache = self.external_agent_home/AgentPaths.CONTAINER_CACHE,
        )
        env.extra_args = extra_args
        return env

    def ExecWithEnv(self) -> "EnvDispatch":
        """Declare how this step invokes its tool in each world.

        A container is a filesystem layout with an entrypoint; a conda env is a
        package set on PATH. They are not interchangeable -- a third of the tool
        library has no conda form, and where both exist the invocation often
        differs. So the transform declares each world it supports and metasmith
        runs the one that matches the agent::

            context.ExecWithEnv() \\
                .ifContainerDo(env=dep, cmd=..., binds=[...], args=[...]) \\
                .ifVirtualEnvDo(env=dep, cmd=..., exports={...})

        Arms are declarations evaluated in place, not a sequence: the matching
        one runs the moment it is called and the other is recorded and skipped,
        so writing side effects between arms makes their order observable. There
        is no terminal call; a chain whose every arm was skipped is caught by the
        framework after the protocol returns (see `UnmatchedEnvDispatches`),
        because a silently-empty step is worse than a loud one.

        Either arm may be omitted. A container-only tool simply has no
        `ifVirtualEnvDo`, which is what makes "can this run without containers?"
        a question the tooling can answer statically.
        """
        d = EnvDispatch(self)
        self._env_dispatches.append(d)
        return d

    def UnmatchedEnvDispatches(self) -> list["EnvDispatch"]:
        """The `ExecWithEnv()` chains this execution reached that ran nothing.

        Read by `bootstrap.ExecuteStep` after the protocol returns: a chain that
        declares only a container arm on a mamba agent would otherwise no-op its
        way to a step that reports success and produces nothing.
        """
        return [d for d in self._env_dispatches if not d._matched]

    def _ExecInEnv(self, image: Dependency, cmd: str, shell="bash", binds: list[tuple[Path|str, Path|str]]|None=None, args: list[str]|None=None, exports: dict[str, str|Path]|None=None, history: bool=True):
        env = self.GetContainerModel(image, binds, args)
        assert env.container.workdir is not None # for typing
        use_cache = False
        cached_path = env.GetLocalPath()
        if cached_path is not None:
            FLAG = "cached image exists"
            sandbox_path = env.GetSandboxPath()
            res = self.external_shell.Exec(
                f'( [ -e {cached_path} ] || [ -d {sandbox_path} ] ) && echo "{FLAG}"',
                history=True,
            )
            if FLAG in res.out:
                use_cache = True

        cmd = RemoveLeadingIndent(cmd)
        Log.Info(f"executing container [{env.image}] using [{env.runtime.name}]")
        h, k = KeyGenerator.FromStr(cmd)
        _bounce_script = Path(f"./_metasmith/.bounce.{k}")
        # Whoever set up this cwd may or may not have made the internals dir: the
        # relay bootstrap does (it deploys the relay there), the relay-free
        # bootstrap and direct-run do not. Owning it here means the arm works the
        # same under every runtime instead of each caller remembering.
        _bounce_script.parent.mkdir(parents=True, exist_ok=True)
        exit_codef = Path(f"exitcode.{GenerateId()}")
        with open(_bounce_script, "w") as f:
            script = [
                f"cd {env.container.workdir}",
                "on_exit() {",
                f"    echo $? > {exit_codef}",
                "}",
                "trap on_exit EXIT",
                "set -e",
                # Exports ride in the bounce script rather than a per-runtime
                # flag (`-e` / `--env` / nothing), so one mechanism serves every
                # runtime and the tool sees its own vocabulary either way.
                *(f"export {k}={shlex.quote(str(v))}" for k, v in _validate_exports(exports).items()),
                cmd,
            ]
            f.write("\n".join(script))
        Log.Info(f"command with bounce at [{_bounce_script}]:")
        for line in cmd.split("\n"):
            Log.Info(f"    {line}")
        Log.Info(f"binds:")
        for s, d in env.container.binds:
            Log.Info(f"    {s} -> {d}")
        _container_start = f"{env.MakeRunCommand(local=use_cache)} {shell}"
        Log.Info(f"-> container start: [{_container_start}]")
        BREAK_LENGTH = 60
        msg = "-> container ->"
        Log.Info(msg+"-"*(BREAK_LENGTH-len(msg)))
        result = self.external_shell.Exec(
            f"{_container_start} {env.container.workdir/_bounce_script}",
            timeout=None, history=history
        )
        try:
            with open(exit_codef) as f:
                exit_code = f.readline().strip()
                exit_code = int(exit_code)
        except:
            exit_code = 1
        msg = f"<- container exit [{exit_code}] <-"
        Log.Info(msg+"-"*(BREAK_LENGTH-len(msg)))
        if exit_codef.exists(): exit_codef.unlink()
        if exit_code != 0:
            Log.Error("a non-zero exit code ocurred while running script in container")
            time.sleep(5)
            sys.exit(exit_code)
        # sresult = self.external_shell.Exec(_container_start, timeout=None, history=history)
        # eresult = self.external_shell.Exec("[ -n $APPTAINER_CONTAINER ] || [ -e /.dockerenv ] && exit", timeout=None, history=history)
        return result
