from __future__ import annotations
from enum import Enum
import os, sys
import shutil
from pathlib import Path
import yaml
from dataclasses import dataclass, field
from typing import Callable, Iterable
from importlib import reload, __import__
import re

from .solver import Dependency, Endpoint, Transform
from .remote import GlobusSource, Logistics, Source, SourceType
from ..hashing import KeyGenerator
from ..logging import Log
from ..constants import VERSION

# @dataclass
# class DataTypeOntology:
#     name: str
#     version: str
#     doi: str
#     strict: bool

#     def Pack(self):
#         d = {}
#         for k, v in self.__dict__.items():
#             if k.startswith("_"): continue
#             d[k] = v
#         return d
    
#     @classmethod
#     def Unpack(cls, d: dict):
#         return cls(**d)

# class DataTypeOntologies:
#     EDAM = DataTypeOntology(
#         name = "EDAM",
#         version = 1.25,
#         doi = "https://doi.org/10.1093/bioinformatics/btt113",
#         strict = False,
#     )

# caches by absolute path
# _dataTypeLibrary_cache: dict[Path, DataTypeLibrary] = {}
# _dataTypeLibrary_history: list[str] = []
@dataclass
class DataTypeLibrary:
    # ontology: DataTypeOntology = field(default_factory=lambda: DataTypeOntologies.EDAM)
    types: dict[str, Endpoint] = field(default_factory=dict)
    # schema: str = VERSION

    # def __post_init__(self):
    #     if self.source is None: return
    #     _dataTypeLibrary_cache[self.source] = self

    def __getitem__(self, key: str) -> Endpoint:
        return self.types[key]
    
    def __setitem__(self, key: str, value: Endpoint):
        assert isinstance(value, Endpoint)
        assert isinstance(key, str)
        self.types[key] = value
    
    def __in__(self, key: str) -> bool:
        return key in self.types
    
    def __iter__(self):
        for k, v in self.types.items():
            yield k, v
    
    def __len__(self) -> int:
        return len(self.types)

    # @classmethod
    # def Proxy(cls, alt: str, lib: DataTypeLibrary):
    #     _dataTypeLibrary_cache[str(alt)] = lib

    @classmethod
    def Unpack(cls, d: dict):
        return cls(
            types={k: Endpoint.Unpack(v) for k, v in d["types"].items()},
        )

    @classmethod
    def Load(cls, path: Source|str|Path) -> DataTypeLibrary:
        if isinstance(path, Source):
            path = path.address
        # todo, urls
        _path = Path(path)
        with open(_path) as f:
            d = yaml.safe_load(f)
        return cls.Unpack(d)

    def Pack(self):
        return {
            "types": {k: v.Pack() for k, v in self.types.items()},
        }

    def Save(self, path: Path):
        with open(path, "w") as f:
            yaml.safe_dump(self.Pack(), f)

# @dataclass
# class DataInstance:
#     source: Source
#     type: Endpoint
#     _key: str = None
#     _hash: int = None

#     def __post_init__(self):
#         if self._key is not None: return
#         props = sorted(self.type.properties)
#         self._hash, self._key = KeyGenerator.FromStr(str(self.source.address)+''.join(props), l=12)

#     def __hash__(self) -> int:
#         return self._hash

#     def Clone(self):
#         return DataInstance(
#             source=self.source.Clone(),
#             type=self.type.Clone(),
#             _key=self._key,
#             _hash=self._hash,
#         )

#     @classmethod
#     def Unpack(cls, d: dict):
#         return cls(
#             source=Source.Unpack(d["source"]),
#             type=Endpoint.Unpack(d["type"]),
#         )
    
#     def Pack(self, parents=False):
#         return {
#             "source": self.source.Pack(),
#             "type": self.type.Pack(parents=parents, save_hash=True),
#         }

@dataclass
class DataInstanceLibrary:
    location: Path
    manifest: dict[Path, Endpoint] = field(default_factory=dict)
    schema: str = VERSION
    _path_to_index: Path = Path("./metadata/index.yml")
    _path_to_types: Path = Path("./metadata/types.yml")

    def __post_init__(self):
        if not self.location.exists():
            self.location.mkdir(parents=True)
        else:
            assert self.location.is_dir(), f"[{self.location}] must be a directory"

    def __getitem__(self, key: Endpoint):
        if isinstance(key, str) and key.startswith("/"):
            key = key[1:]
        key = Path(key)
        return self.manifest[key]

    def __in__(self, key: str) -> bool:
        key = str(key)
        return key in self.manifest

    def __iter__(self):
        for k, inst in self.manifest.items():
            yield k, inst

    def __len__(self):
        return len(self.manifest)

    def Add(self, items: list[tuple[Path|str, Path|str, Endpoint]], method: SourceType=SourceType.DIRECT, on_exist: str="skip"):
        assert method in {SourceType.DIRECT, SourceType.SYMLINK}
        assert on_exist in {"skip", "replace", "error"}
        mover = Logistics()
        items = [(Path(src), Path(dest), dtype) for src, dest, dtype in items]
        seen = {v for v in self.manifest.values()}
        for src, dest, dtype in items:
            assert src.exists(), f"[{src}] does not exist"
            assert not dest.is_absolute(), f"destination [{dest}] must be relative"
            assert dtype not in seen, f"datatype [{dtype}] already registered and so would not be distinguishable"
            mover.QueueTransfer(
                src = Source.FromLocal(src),
                dest = Source(address=self.location/dest, type=method),
            )
        moved = mover.ExecuteTransfers()
        completed = {str(Path(s.address)) for s, d in moved}
        report: list[Path] = []
        for src, dest, dtype in items:
            k = str(src)
            if k not in completed:
                Log.Error(f"failed to add [{src}]")
                continue
            self.manifest[dest] = dtype
            report.append(dest)
        return report
    
    def CopyTo(self, dest: Source, label: str=None):
        mover = Logistics()
        mover.QueueTransfer(
            src=Source.FromLocal(self.location),
            dest=dest,
        )
        res = mover.ExecuteTransfers(label=label)
        assert len(res) == 1, f"move failed"

    @classmethod
    def DownloadFrom(cls, src: Source, dest: Path, label: str=None):
        mover = Logistics()
        mover.QueueTransfer(
            src=src,
            dest=Source.FromLocal(dest),
        )
        res = mover.ExecuteTransfers(label=label)
        assert len(res) == 1, f"move failed"
        return cls.Load(dest)

    def Pack(self):
        return dict(
            schema=self.schema,
            manifest={str(k):v.key for k, v in self.manifest.items()},
        )

    @classmethod
    def Unpack(cls, location: Path, raw: dict, dtypes: DataTypeLibrary):
        manifest = {}
        for k, v in raw["manifest"].items():
            if not (location/k).exists():
                Log.Error(f"skipping [{k}], does not exist")
                continue
            manifest[Path(k)] = dtypes[v]
        return cls(
            location=location,
            schema=raw["schema"],
            manifest=manifest,
        )

    def Save(self):
        types_path = self.location/self._path_to_types
        dtypes = DataTypeLibrary()
        for _, e in self.manifest.items():
            dtypes[e.key] = e
        types_path.parent.mkdir(parents=True, exist_ok=True)
        with open(types_path, "w") as f:
            yaml.dump(dtypes.Pack(), f)

        index_path = self.location/self._path_to_index
        index_path.parent.mkdir(parents=True, exist_ok=True)
        with open(index_path, "w") as f:
            yaml.dump(self.Pack(), f)
        
    
    @classmethod
    def Load(cls, path: Path|str):
        path = Path(path)
        _index_path = path/cls._path_to_index
        _types_path = path/cls._path_to_types
        assert path.exists(), f"path [{path}] does not exist"
        assert _index_path.exists(), f"index file [{_index_path}] does not exist"
        assert _types_path.exists(), f"types file [{_types_path}] does not exist"

        dtypes = DataTypeLibrary.Load(_types_path)
        with open(_index_path) as f:
            d = yaml.safe_load(f)
            self = cls.Unpack(location=path, raw=d, dtypes=dtypes)
        return self

@dataclass
class TransformInstance:
    protocol: Callable[[ExecutionContext], ExecutionResult]
    model: Transform
    output_signature: dict[Dependency, Path]
    _source: Source = None
    _used_libraries: set[str] = field(default_factory=list)
    _hash: int = None
    _key: str = None

    def __post_init__(self):
        for k, vt in [
            ("protocol", Callable),
            ("model", Transform),
            ("output_signature", dict),
        ]:
            v = getattr(self, k)
            assert isinstance(v, vt), f"[{k}] must be of type [{vt}] but got [{type(v)}]"
        for d, p in self.output_signature.items():
            assert isinstance(p, Path), f"output signature key must be of type [Path] but got [{type(p)}]"
            assert isinstance(d, Dependency), f"output signature value must be of type [Dependency] but got [{type(d)}]"
            assert d in self.model.produces, f"output signature value must be added to model"
        for dep in self.model.produces:
            assert dep in self.output_signature, f"model output missing in signature [{dep}]"
        TransformInstance._last_loaded_transform = self

    def __hash__(self) -> int:
        return self._hash

    @classmethod
    def Load(cls, definition: Path) -> TransformInstance|None:
        cls._last_loaded_transform: TransformInstance = None

        original_path_var = sys.path
        sys.path = [str(definition.parent)]+sys.path
        try:
            m = __import__(f"{definition.stem}")
            reload(m)
            if cls._last_loaded_transform is not None:
                tr = cls._last_loaded_transform
                with open(definition) as f:
                    raw = "".join(f.readlines())
                    h, k = KeyGenerator.FromStr(raw, l=5)
                    tr._hash, tr._key = h, k
                tr._source = Source(address=definition, type=SourceType.DIRECT)
                tr._used_libraries = set(_dataTypeLibrary_history)
                return cls._last_loaded_transform
            _dataTypeLibrary_history.clear()
        finally:
            sys.path = original_path_var

@dataclass
class TransformInstanceLibrary:
    manifest: dict[Path, TransformInstance] = field(default_factory=dict)
    type_lib_aliases: dict[Path, list[str]] = field(default_factory=dict)
    _by_keys: dict[str, TransformInstance] = field(default_factory=dict)
    _TYPELIB_SAVE = "_type_libraries"
    _ALIASES_SAVE = "_aliases.yml"

    def __post_init__(self):
        self._by_keys = {t._key: t for t in self.manifest.values()}

    def __getitem__(self, key: str) -> TransformInstance:
        return self.manifest[key]
    
    def GetByKey(self, key: str) -> TransformInstance:
        return self._by_keys[key]
    
    def __iter__(self):
        for path, tr in self.manifest.items():
            yield path, tr

    def __len__(self):
        return sum(len(s) for s in self.manifest.values())

    def Gather(self, folder: Path, overwrite: bool=False):
        if isinstance(folder, str): folder = Path(folder)
        assert overwrite or not folder.exists(), f"folder [{folder}] already exists"
        if overwrite and folder.exists():
            shutil.rmtree(folder)
        folder.mkdir(parents=True)
        mover = Logistics()
        type_libs = set()
        added_names = set()
        for _, inst in self.manifest.items():
            src = inst._source
            dest = Path(src.address).name
            assert dest not in added_names, f"duplicate transform name [{dest}]"
            added_names.add(dest)
            dest = Source(folder/dest, SourceType.DIRECT)
            mover.QueueTransfer(src, dest)
            type_libs.update(inst._used_libraries)

        type_lib_dir = folder/self._TYPELIB_SAVE
        type_lib_dir.mkdir(parents=True, exist_ok=True)
        type_lib_aliases = {}
        for lib_path in type_libs:
            lib_path = Path(lib_path)
            mover.QueueTransfer(
                Source(lib_path, SourceType.DIRECT),
                Source(type_lib_dir/lib_path.name, SourceType.DIRECT),
            )
            type_lib_aliases[lib_path.name] = [str(lib_path)]
        with open(type_lib_dir/self._ALIASES_SAVE, "w") as f:
            yaml.dump(type_lib_aliases, f)

        errs = mover.ExecuteTransfers()
        for e in errs:
            Log.Error(f"transfer error: [{e}]")
        new = self.Load(folder)
        return new

    @classmethod
    def _load_section(cls, path: Path):
        path = path.resolve()
        assert path.is_dir(), f"[{path}] for TransformInstanceLibrary not a directory"
        section = {}
        for p in path.glob("**/*.py"):
            if p.is_dir(): continue
            k = p.relative_to(path)
            inst = TransformInstance.Load(p)
            if inst is None:
                Log.Warn(f"could not load [{k}] from [{path}]")
            else:
                section[p.resolve()] = inst
        return section
    
    def Import(self, path: str|Path|Iterable[Path|str]):
        if isinstance(path, Path) or isinstance(path, str):
            path = [path]
        for p in path:
            if isinstance(p, str): p = Path(p)
            type_lib_path = p/self._TYPELIB_SAVE
            alias_path = type_lib_path/self._ALIASES_SAVE
            if alias_path.exists():
                with open(alias_path) as f:
                    aliases = yaml.safe_load(f)
                for lib_name, aliases in aliases.items():
                    if lib_name not in aliases: self.type_lib_aliases[lib_name] = set()
                    added_aliases = self.type_lib_aliases[lib_name]
                    for a in aliases:
                        if a in added_aliases: continue
                        DataTypeLibrary.Proxy(a, DataTypeLibrary.Load((type_lib_path/lib_name).resolve()))
                        added_aliases.add(a)
                    self.type_lib_aliases[lib_name] = added_aliases
            self.manifest.update(self._load_section(p))
            self._by_keys.update({t._key: t for t in self.manifest.values()})

    def Update(self, other: TransformInstanceLibrary):
        for lib_name, aliases in other.type_lib_aliases.items():
            if lib_name not in aliases: self.type_lib_aliases[lib_name] = set()
            added_aliases = self.type_lib_aliases[lib_name]
            self.type_lib_aliases[lib_name] = added_aliases|set(aliases)
        self.manifest.update(other.manifest)
        self._by_keys.update(other._by_keys)

    @classmethod
    def Load(cls, path: Path|Iterable[Path]) -> TransformInstanceLibrary:
        lib = cls()
        lib.Import(path)
        return lib

@dataclass
class ExecutionContext:
    input: list[DataInstance]
    output: list[DataInstance]
    transform_key: str
    work_dir: Path
    _shell: Callable = None

    def __post_init__(self):
        self._input_map: dict[str, DataInstance] = {x.type.key:x for x in self.input}
        self._output_map: dict[str, DataInstance] = {x.type.key:x for x in self.output}

    def _get_by_properties(self, instances: list[DataInstance], e: Endpoint) -> DataInstance|None:
        for x in instances:
            if x.type.Signature() != e.Signature(): continue
            return x

    def GetInput(self, e: Endpoint):
        return self._get_by_properties(self.input, e)

    def GetOutput(self, e: Endpoint):
        return self._get_by_properties(self.output, e)

    def Shell(self, cmd: str, timeout: int|float|None=None) -> tuple[list[str], list[str]]:
        assert self._shell is not None, "shell not set"
        self._shell(cmd, timeout)

    @classmethod
    def Unpack(cls, d: dict):
        return cls(
            input=[DataInstance.Unpack(v) for v in d["input"]],
            output=[DataInstance.Unpack(v) for v in d["output"]],
            transform_key=d["transform_key"],
            work_dir=Path(d["work_dir"]),
        )
    
    @classmethod
    def Load(cls, path: Path):
        with open(path) as f:
            d = yaml.safe_load(f)
        return cls.Unpack(d)
    
    def Pack(self):
        return {
            "input": [v.Pack(parents=True) for v in self.input],
            "output": [v.Pack(parents=True) for v in self.output],
            "transform_key": self.transform_key,
            "work_dir": str(self.work_dir),
        }
    
    def Save(self, path: Path):
        with open(path, "w") as f:
            yaml.dump(self.Pack(), f)

@dataclass
class ExecutionResult:
    success: bool = False
