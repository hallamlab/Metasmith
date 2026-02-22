from __future__ import annotations
import os, sys
import subprocess
import shutil
from pathlib import Path
import yaml
from dataclasses import dataclass, field
from typing import Callable, Iterable
from importlib import reload, __import__
import tempfile
import time
from datetime import timedelta
import json

from ..serialization import IsText
from ..coms.containers import ContainerRuntime, Container
from ..coms.terminals import RemoveLeadingIndent
from ..coms.via_file_watcher import RemoteShell, GenerateId
from .solver import Dependency, Endpoint, Transform
from .remote import Logistics, Source, SourceType
from ..hashing import KeyGenerator
from ..logging import Log
from ..constants import VERSION, MODULE_PATH, AgentPaths

def yaml_safe_load(p: Path):
    MAX = 5
    for i in range(MAX):
        with open(p) as f:
            s = '\n'.join(f.readlines())
            # assert len(s) > 0, f"DataTypeLibrary at [{path}] is empty"
            d = yaml.safe_load(s)
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

    def RecalculateKey(self):
        self._hash, self._key = KeyGenerator.FromStr("".join([
            str(self.path),
            self.dtype.key,
            self.dtype_name,
            # self.parent_lib.GetKey(),
        ]), l=8)
        return self._key

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
        )

    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataInstanceLibrary]):
        lib_key, namespace, dtype_name = raw["type"].split("::")
        lib = libraries[lib_key]
        dtype = lib.types[namespace][dtype_name]

        return cls(
            path=Path(raw["path"]),
            dtype=dtype,
            dtype_name=f"{namespace}::{dtype_name}",
            parent_lib=lib,
        )

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
        _seen.add(path)

        e_name = self.manifest[path]
        e = self.GetType(e_name)
        if path in self.parents:
            parent_endpoints = set()
            for parent_meta in self.parents[path]:
                parent_ep = self._build_endpoint_with_lineage(parent_meta.path, _seen)
                parent_endpoints.add(parent_ep)
            e = Endpoint(e.properties, parent_endpoints)
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

        def _get_all_descendants(ancestor_paths: set[Path]) -> set[Path]:
            """Get all items that have any of the given paths as an ancestor."""
            descendants = set()
            for item_path in self.manifest.keys():
                item_ancestors = _get_all_ancestors(item_path)
                if item_ancestors & ancestor_paths:  # If they share any ancestor
                    descendants.add(item_path)
            return descendants

        for path, name in self.manifest.items():
            if not _accept(name): continue
            ancestors = _get_all_ancestors(path)
            # Include index, ancestors, and all siblings (items sharing ancestors)
            siblings = _get_all_descendants(ancestors | {path})
            yield DataInstanceLibraryView(original=self, mask={path} | ancestors | siblings)

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
        return path

    def AddValue(self, name: str, value: str|dict, dtype: str, parents: Iterable[Path]|None=None):
        path = Path(name)
        if isinstance(value, dict):
            value = json.dumps(value)
        path = self.AddItem(path=path, dtype=dtype, parents=parents) # perform checks first
        with open(self.location/path, "w") as f:
            f.write(value)
        return path

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
        if _save: self.Save()

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

    def _calculate_key(self):
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

        # Second pass: Aggregate grandparents (now all immediate parents are populated)
        for k in raw["manifest"].keys():
            k_path = Path(k)
            if k_path not in lib.parents:
                continue
            ancestors: dict[Path, DataInstanceLibrary.ParentMetadata] = {}
            for p in lib.parents[k_path]:
                ancestors[p.path] = p
                # Recursively collect all ancestors
                for gp in lib.parents.get(p.path, []):
                    if gp.path not in ancestors:
                        ancestors[gp.path] = gp
            lib.parents[k_path] = list(ancestors.values())

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
        self._calculate_key()
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

    def Get(self, path: str|Path):
        p = Path(path)
        assert p in self._mask
        return self._original.Get(path)
    
    def Iterate(self):
        for p in self._mask:
            inst = self._original.Get(p)
            yield p, inst.dtype_name, inst.dtype

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

@dataclass
class Resources:
    cpus: int|None = None
    memory: Size|None = None
    duration: Duration|None = None

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

    @classmethod
    def Load(cls, path: Path|str):
        return cls(DataInstanceLibrary.Load(path))

    @classmethod
    def LoadFrom(cls, src: Source, dest: Path, label: str|None=None):
        return cls(DataInstanceLibrary.LoadFrom(src, dest, label=label))

@dataclass
class ContextPath:
    local: Path
    external: Path
    container: Path

@dataclass
class ContextData:
    input_group: list[ContextPath]
    endpoint: Endpoint
    type_name: str
    path: ContextPath = field(default_factory=lambda: ContextPath(Path(), Path(), Path()))
    
    def __post_init__(self) -> None:
        assert len(self.input_group)>0
        self.path = self.input_group[0]

@dataclass
class ExecutionResult:
    success: bool = True
    manifest: list[dict[Dependency, Path]] = field(default_factory=list)

class ExecutionFailed(Exception):
    pass

# work as if batch of 1 item
# until explicitly batch iterated
@dataclass
class ExecutionContext:
    _inputs: list[dict[Dependency, ContextData]]
    _get_output_paths: Callable[[Dependency, int, int], ContextPath]
    external_shell: RemoteShell # since metasmith will bootstrap into its own container
    external_cwd: Path
    external_agent_home: Path
    container_runtime: ContainerRuntime
    params: dict = field(default_factory=dict)
    _batch_index: int = 0

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

    def GetContainerModel(self, image: Dependency, binds: list[tuple[Path|str, Path|str]]|None=None):
        path = self._inputs[self._batch_index][image].path
        if IsText(path.local):
            with open(path.local) as f:
                image_path = f.read().strip() # using the uri
        else:
            image_path = str(path.external)
    
        _binds: list[tuple[Path, Path]] = []
        for _, v in list(self._inputs[self._batch_index].items()):
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
        container_ws = Path("/ws")
        binds += [
            ('${TMPDIR-"/tmp"}', '${TMPDIR-"/tmp"}'),
            (self.external_agent_home, AgentPaths.HOME_ROOT),
            (self.external_cwd, container_ws),
        ]
        binds += sorted([(s, d) for s, d in _binds])
        
        container = Container(
            image = str(image_path),
            workdir = container_ws,
            runtime = self.container_runtime,
            binds = binds,
            container_cache = self.external_agent_home/AgentPaths.CONTAINER_CACHE,
        )
        return container

    def ExecWithContainer(self, image: Dependency, cmd: str, shell="bash", binds: list[tuple[Path|str, Path|str]]|None=None, history: bool=True):
        container = self.GetContainerModel(image, binds)
        assert container.workdir is not None # for typing
        use_cache = False
        cached_path = container.GetLocalPath()
        if cached_path is not None:
            FLAG = "cached image exists"
            res = self.external_shell.Exec(f'[ -e {cached_path} ] && echo "{FLAG}"', history=True)
            if FLAG in res.out:
                use_cache = True

        cmd = RemoveLeadingIndent(cmd)
        Log.Info(f"executing container [{container.image}] using [{container.runtime.name}]")
        h, k = KeyGenerator.FromStr(cmd)
        _bounce_script = Path(f"./_metasmith/.bounce.{k}")
        exit_codef = Path(f"exitcode.{GenerateId()}")
        with open(_bounce_script, "w") as f:
            script = [
                "cd /ws",
                "on_exit() {",
                f"    echo $? > {exit_codef}",
                "}",
                "trap on_exit EXIT",
                "set -e",
                cmd,
            ]
            f.write("\n".join(script))
        Log.Info(f"command with bounce at [{_bounce_script}]:")
        for line in cmd.split("\n"):
            Log.Info(f"    {line}")
        Log.Info(f"binds:")
        for s, d in container.binds:
            Log.Info(f"    {s} -> {d}")
        _container_start = f"{container.MakeRunCommand(local=use_cache)} {shell}"
        Log.Info(f"-> container start: [{_container_start}]")
        BREAK_LENGTH = 60
        msg = "-> container ->"
        Log.Info(msg+"-"*(BREAK_LENGTH-len(msg)))
        result = self.external_shell.Exec(
            f"{_container_start} {container.workdir/_bounce_script}",
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
