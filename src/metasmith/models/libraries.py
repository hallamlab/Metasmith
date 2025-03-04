from __future__ import annotations
import os, sys
from pathlib import Path
import yaml
from dataclasses import dataclass, field
from typing import Callable, Iterable
from importlib import metadata, reload, __import__

from ..coms.ipc import LiveShell
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
    
    def __contains__(self, key: str) -> bool:
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

class DataInstanceLibrary:
    schema: str = VERSION
    _path_to_meta: Path = Path("./_metadata")
    _index_name: str = "index"
    _metadata_ext: str = ".yml"

    def __init__(self, location: Path) -> None:
        if not location.exists():
            location.mkdir(parents=True)
        else:
            assert location.is_dir(), f"[{location}] must be a directory"
        self.location = location
        self.manifest: dict[Path, str] = {}
        self.types: dict[str, DataTypeLibrary] = {}

    def AddTypeLibrary(self, namespace: str, lib: DataTypeLibrary|Source):
        assert namespace not in self.types, f"namespace [{namespace}] already exists"
        if isinstance(lib, DataTypeLibrary):
            self.types[namespace] = lib
        else:
            mover = Logistics()
            ext = self._metadata_ext
            namespace = namespace.replace(".yaml", "").replace(ext, "")
            meta_path = self.location/self._path_to_meta
            meta_path.mkdir(parents=True, exist_ok=True)
            lib_path = meta_path/(namespace+ext)
            lib_dest = Source(address=lib_path, type=SourceType.DIRECT)
            mover.QueueTransfer(
                src=lib,
                dest=lib_dest,
            )
            moved = mover.ExecuteTransfers()
            assert len(moved) == 1, f"failed to add type library [{namespace}]"
            self.types[namespace] = DataTypeLibrary.Load(lib_dest.address)
        return self.types[namespace]

    def Add(self, items: list[tuple[Path|str, Path|str, str]], method: SourceType=SourceType.DIRECT, on_exist: str="skip"):
        assert method in {SourceType.DIRECT, SourceType.SYMLINK}
        assert on_exist in {"skip", "replace", "error"}
        mover = Logistics()
        items = [(Path(src), Path(dest), dtype) for src, dest, dtype in items]
        # seen = {v for v in self.manifest.values()}
        completed = set()
        for src, dest, dtype in items:
            assert isinstance(dtype, str), f"datatype must be a string of <namespace>::<type> but got [{type(dtype)}]"
            src, dest = Path(src), Path(dest)
            assert src.exists(), f"[{src}] does not exist"
            assert not dest.is_absolute(), f"destination [{dest}] must be relative"
            # assert dtype not in seen, f"an instance of datatype [{dtype}] is already registered and so would not be distinguishable"
            namespace, dtype = dtype.split("::")
            assert namespace in self.types, f"namespace [{namespace}] not found"
            lib = self.types[namespace]
            assert dtype in lib, f"datatype [{dtype}] not found in [{namespace}]"
            dest_path = self.location/dest
            if dest_path.exists():
                if on_exist == "skip":
                    completed.add(str(src))
                    continue
                elif on_exist == "error":
                    raise FileExistsError(f"destination [{dest}] already exists")
            mover.QueueTransfer(
                src = Source.FromLocal(src),
                dest = Source(address=dest_path, type=method),
            )
        moved = mover.ExecuteTransfers()
        completed |= {str(Path(s.address)) for s, d in moved}
        report: list[Path] = []
        for src, dest, dtype in items:
            k = str(src)
            if k not in completed:
                Log.Error(f"failed to add [{src}]")
                continue
            self.manifest[dest] = dtype
            report.append(dest)
        return report

    def Pack(self):
        return dict(
            schema=self.schema,
            manifest={str(k):str(v) for k, v in self.manifest.items()},
        )

    @classmethod
    def Unpack(cls, location: Path, raw: dict, dtypes: dict[str, DataTypeLibrary]):
        manifest = {}
        for k, v in raw["manifest"].items():
            if not (location/k).exists():
                Log.Error(f"skipping [{k}], does not exist")
                continue
            namespace, dtype = v.split("::")
            assert namespace in dtypes, f"namespace [{namespace}] not found"
            lib = dtypes[namespace]
            assert dtype in lib, f"datatype [{dtype}] not found in [{namespace}]"
            manifest[Path(k)] = v
        lib = cls(
            location=location,
        )
        lib.schema = raw["schema"]
        lib.manifest = manifest
        return lib

    def Save(self):
        ext = self._metadata_ext
        metadata_path = self.location/self._path_to_meta
        metadata_path.mkdir(parents=True, exist_ok=True)
        for namespace, types_lib in self.types.items():
            local_path = metadata_path/(namespace+ext)
            types_lib.Save(local_path)

        index_path = metadata_path/(self._index_name+ext)
        index_path.parent.mkdir(parents=True, exist_ok=True)
        with open(index_path, "w") as f:
            yaml.dump(self.Pack(), f)
    
    @classmethod
    def Load(cls, path: Path|str):
        path = Path(path)
        ext = cls._metadata_ext
        meta_path = path/cls._path_to_meta
        index_path = meta_path/(cls._index_name+ext)
        assert path.exists(), f"path [{path}] does not exist"
        assert index_path.exists(), f"index file [{index_path}] does not exist"

        dtypes = {}
        for p in path.glob(f"**/*{ext}"):
            if p.is_dir(): continue
            if p == index_path: continue
            k = p.relative_to(meta_path).with_suffix("")
            k = str(k)
            dtypes[k] = DataTypeLibrary.Load(p)

        with open(index_path) as f:
            d = yaml.safe_load(f)
            self = cls.Unpack(location=path, raw=d, dtypes=dtypes)
        self.types = dtypes
        return self

    def SaveAs(self, dest: Source, label: str=None):
        self.Save()
        mover = Logistics()
        mover.QueueTransfer(
            src=Source.FromLocal(self.location),
            dest=dest,
        )
        res = mover.ExecuteTransfers(label=label)
        assert len(res) == 1, f"move failed"

    @classmethod
    def LoadFrom(cls, src: Source, dest: Path, label: str=None):
        mover = Logistics()
        mover.QueueTransfer(
            src=src,
            dest=Source.FromLocal(dest),
        )
        res = mover.ExecuteTransfers(label=label)
        assert len(res) == 1, f"move failed"
        return cls.Load(dest)
    
@dataclass
class TransformInstance:
    protocol: Callable[[ExecutionContext], ExecutionResult]
    model: Transform
    output_signature: dict[Dependency, Path]

    def __post_init__(self):
        for k, vt in [
            ("protocol", Callable),
            ("model", Transform),
            ("output_signature", dict),
        ]:
            v = getattr(self, k)
            assert isinstance(v, vt), f"[{k}] must be of type [{vt}] but got [{type(v)}]"
        for k in list(self.output_signature.keys()):
            self.output_signature[k] = Path(self.output_signature[k])
        for d, p in self.output_signature.items():
            assert isinstance(d, Dependency), f"output signature value must be of type [Dependency] but got [{type(d)}]"
            assert d in self.model.produces, f"output signature value must be added to model"
        for dep in self.model.produces:
            assert dep in self.output_signature, f"model output missing in signature [{dep}]"
        TransformInstance._last_loaded_transform = self

    def __hash__(self) -> int:
        return self._hash # from definition file upon load

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
                return cls._last_loaded_transform
        finally:
            sys.path = original_path_var

class TransformInstanceLibrary:
    def __init__(self, location: Path) -> None:
        self.lib = DataInstanceLibrary(location)
        transform_types = DataTypeLibrary(dict(
            transform=Endpoint({"metasmith", "transform"}),
            example_input=Endpoint({"metasmith", "example_input"}),
            example_output=Endpoint({"metasmith", "example_output"}),
        ))
        self.lib.AddTypeLibrary("transforms", transform_types)

    def AddStub(self, path: Path|str, exist_ok: bool=False):
        path = Path(path)
        assert not path.is_absolute(), f"path must be relative"
        HERE = Path(__file__).parent
        example = HERE/"_example_transform.py"
        if path.suffix != ".py":
            path = path.parent/(path.name+".py")
        results = self.lib.Add([(example, path, "transforms::transform")], on_exist="skip" if exist_ok else "error")
        self.Save()
        return self.lib.location/results[0]

    @classmethod
    def ResolveParentLibrary(cls, transform_definition_file: Path|str):
        path = Path(transform_definition_file)
        for p in path.parents:
            if (p/DataInstanceLibrary._path_to_meta).exists():
                return cls.Load(p)

    def GetType(self, address: str):
        namespace, dtype = address.split("::")
        assert namespace in self.lib.types, f"namespace [{namespace}] not found"
        lib = self.lib.types[namespace]
        assert dtype in lib, f"datatype [{dtype}] not found in [{namespace}]"
        return lib[dtype]
    
    def GetTransform(self, path: Path|str):
        path = self.lib.location/path
        if path.suffix != ".py":
            path = path.with_suffix(".py")
        return TransformInstance.Load(path)

    def Save(self):
        self.lib.Save()

    @classmethod
    def Load(cls, path: Path|str):
        self = cls(path)
        self.lib = DataInstanceLibrary.Load(path)
        return self
    
    def SaveAs(self, dest: Source, label: str=None):
        self.lib.SaveAs(dest, label=label)

    @classmethod
    def LoadFrom(cls, src: Source, dest: Path, label: str=None):
        return cls.Load(DataInstanceLibrary.LoadFrom(src, dest, label=label))

@dataclass
class ExecutionContext:
    inputs: dict[str, Path]
    work_dir: Path
    shell: LiveShell

    @classmethod
    def Unpack(cls, d: dict, work_dir: Path, shell: LiveShell):
        return cls(
            input={Endpoint.Unpack(k): Path(v) for k, v in d["input"]},
            work_dir=work_dir,
            shell=shell,
        )
    
    @classmethod
    def Load(cls, path: Path):
        with open(path) as f:
            d = yaml.safe_load(f)
        return cls.Unpack(d)
    
    def Pack(self):
        return {
            "input": {k.Pack(): str(v) for k, v in self.inputs.items()},
        }
    
    def Save(self, path: Path):
        with open(path, "w") as f:
            yaml.dump(self.Pack(), f)

@dataclass
class ExecutionResult:
    success: bool = False
