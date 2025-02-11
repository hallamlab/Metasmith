from __future__ import annotations
import os, sys
import shutil
from pathlib import Path
import yaml
from dataclasses import dataclass, field
from typing import Callable, Iterable
from importlib import reload, __import__

from .solver import Dependency, Endpoint, Namespace, Transform
from ..hashing import KeyGenerator
from ..logging import Log
from ..constants import VERSION

# @dataclass
# class DataType:
#     properties: dict[str, str|list[str]]
#     lineage: list[DataType] = field(default_factory=list)
    
#     def __post_init__(self):
#         self._sort_properties()

#     def __hash__(self) -> int:
#         if not hasattr(self, "_hash"):
#             self._hash = str_hash(''.join(self.AsProperties()))
#         return self._hash

#     def _sort_properties(self):
#         self.properties = {k: v for k, v in sorted(self.properties.items())}

#     def AsProperties(self):
#         return set(f"{k}={','.join(v) if isinstance(v, list) else v}".lower() for k, v in self.properties.items())
    
#     def IsA(self, other: DataType) -> bool:
#         return self.AsProperties().issubset(other.AsProperties())

#     def WithLineage(self, lineage: Iterable[DataType]) -> DataType:
#         m = self.Clone()
#         m.lineage = [x.Clone() for x in lineage]
#         return m

#     def Clone(self):
#         return DataType(
#             properties={k: v.copy() if isinstance(v, list) else v for k, v in self.properties.items()},
#             lineage=[x.Clone() for x in self.lineage],
#         )

#     @classmethod
#     def Unpack(cls, d: dict):
#         m = cls(
#             properties=d["properties"],
#         )
#         if "lineage" in d:
#             m.lineage = [cls.Unpack(x) for x in d["lineage"]]
#         return m

#     def Pack(self):
#         d = {
#             "properties": self.properties,
#         }
#         if len(self.lineage)>0:
#             d["lineage"] = [x.Pack() for x in self.lineage]
#         return d

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
    
    @classmethod
    def Unpack(cls, d: dict):
        return cls(**d)

class DataTypeOntologies:
    EDAM = DataTypeOntology(
        name = "EDAM",
        version = 1.25,
        doi = "https://doi.org/10.1093/bioinformatics/btt113",
        strict = False,
    )

# caches by absolute path
_library_cache: dict[Path, DataTypeLibrary] = {}
@dataclass
class DataTypeLibrary:
    types: dict[str, Endpoint] = field(default_factory=dict)
    source: Path|None = None
    ontology: dict = field(default_factory= lambda: DataTypeOntologies.EDAM)
    schema: str = VERSION

    def __post_init__(self):
        if self.source is None: return
        _library_cache[self.source] = self

    def __getitem__(self, key: str) -> Endpoint:
        return self.types[key]
    
    def __setitem__(self, key: str, value: Endpoint):
        assert isinstance(value, Endpoint)
        assert isinstance(key, str)
        self.types[key] = value
    
    def __in__(self, key: str) -> bool:
        return key in self.types
    
    def __len__(self) -> int:
        return len(self.types)

    def Subset(self, keys: Iterable[str]) -> DataTypeLibrary:
        ss = DataTypeLibrary(schema=self.schema, ontology=self.ontology)
        ss.types = {k: v.Clone() for k, v in self.types.items() if k in keys}
        return ss

    @classmethod
    def Load(cls, path: Path|str) -> DataTypeLibrary:
        # todo, urls
        if isinstance(path, str):            
            path = Path(path)
        assert path.is_absolute(), f"path to [{cls}] must be absolute"
        if path in _library_cache:
            return _library_cache[path]
        with open(path) as f:
            d = yaml.safe_load(f)
        lib = cls(
            source=path,
            schema=d["schema"],
            ontology=DataTypeOntology.Unpack(d["ontology"])
        )
        types: dict[str, Endpoint] = {}
        for k, v in d["types"].items():
            types[k] = Endpoint.Unpack(v)
        lib.types = types
        return lib

    def Save(self):
        with open(self.source, "w") as f:
            yaml.safe_dump(dict(
                schema=self.schema,
                ontology=self.ontology.Pack(),
                types={k: v.Pack() for k, v in self.types.items()},
            ), f)

@dataclass
class DataInstance:
    source: Path
    type: Endpoint

    def __hash__(self) -> int:
        if not hasattr(self, "_hash"):
            _hash, _key = KeyGenerator.FromStr(str(self.source.resolve())+''.join(self.type.properties))
            self._hash: int = _hash
        return self._hash

    @classmethod
    def Unpack(cls, d: dict):
        return cls(
            source=Path(d["source"]),
            type=Endpoint.Unpack(d["type"]),
        )
    
    def Pack(self):
        return {
            "source": str(self.source),
            "type": self.type.Pack(),
        }
    
@dataclass
class DataInstanceLibrary:
    source: Path = Path("./")
    manifest: dict[Path, DataInstance] = field(default_factory=dict)
    schema: str = VERSION
    _index_name: str = "info.yml"

    def __getitem__(self, key: Path|str) -> DataInstance:
        if isinstance(key, str):
            key = Path(key)
        return self.manifest[key]

    def __in__(self, key: Path) -> bool:
        if key.is_relative_to(self.source):
            key = key.relative_to(self.source)
        return key in self.manifest and Path(key.name) in self.manifest

    def __iter__(self):
        for path, inst in self.manifest.items():
            yield path, inst

    def _index_path(self):
        return self.source/self._index_name

    def ImportDataInstance(self, source: Path|str, type: Endpoint, local_path: Path=None, copy: bool=False, overwrite: bool=False):
        source = Path(source)
        assert source.exists(), f"path doesn't exist [{source}]"
        if local_path is None:
            local_path = self.source/source.name
        else:
            assert not local_path.is_absolute(), "local_path must be relative"
            local_path = self.source/local_path
        if local_path.exists():
            if overwrite:
                local_path.unlink()
            else:
                raise FileExistsError(f"already exists [{local_path}]")
        self.source.mkdir(parents=True, exist_ok=True)
        if copy:
            shutil.copy(source, local_path)
        else:
            os.symlink(source, local_path)
        local_path = local_path.relative_to(self.source)
        self.manifest[local_path] = DataInstance(local_path, type)

    @classmethod
    def Load(cls, source: Path|str):
        source = Path(source)
        with open(source/cls._index_name) as f:
            d = yaml.safe_load(f)
        manifest = {Path(k):DataInstance(Path(k), Endpoint.Unpack(v)) for k, v in d["manifest"].items()}
        return DataInstanceLibrary(source, manifest)

    def Save(self):
        d = {str(k):v.type.Pack() for k, v in self.manifest.items()}
        with open(self._index_path(), "w") as f:
            yaml.dump(dict(
                schema=self.schema,
                manifest=d,
            ), f)

    def Get(self, like: Endpoint) -> DataInstance:
        for k, v in self.manifest.items():
            if v.type.IsA(like):
                return v
        raise KeyError(f"no instance like [{like}] in [{self.source}]")
    
@dataclass
class TransformInstance:
    protocol: Callable[[ExecutionContext], ExecutionResult]
    model: Transform
    output_signature: dict[Dependency, Path]
    _source: Path = None

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

    def GetOutput(self, dep: Dependency) -> DataInstance:
        if dep not in self.model.produces: return


    @classmethod
    def Load(cls, definition: Path) -> TransformInstance|None:
        cls._last_loaded_transform: TransformInstance = None

        original_path_var = sys.path
        sys.path = [str(definition.parent)]+sys.path
        try:
            m = __import__(f"{definition.stem}")
            reload(m)
            if cls._last_loaded_transform is not None:
                cls._last_loaded_transform._source = definition
                return cls._last_loaded_transform
        finally:
            sys.path = original_path_var

@dataclass
class TransformInstanceLibrary:
    manifest: dict[Path, dict[Path, TransformInstance]]

    def __getitem__(self, key: str) -> TransformInstance:
        return self.manifest[key]
    
    def __iter__(self):
        for root, section in self.manifest.items():
            for path, tr in section.items():
                yield root/path, tr

    def __len__(self):
        return sum(len(s) for s in self.manifest.values())
    
    @classmethod
    def _Load_section(cls, path: Path):
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
                section[k] = inst
        return section

    def Import(self, path: Path|Iterable[Path]):
        if isinstance(path, Path):
            path = [path]
        for p in path:
            self.manifest[p] = self._Load_section(p)

    @classmethod
    def Load(cls, path: Path|Iterable[Path]) -> TransformInstanceLibrary:
        if isinstance(path, Path):
            path = [path]
        manifest = {}
        for root in path:
            manifest[root] = cls._Load_section(root)
        return cls(manifest)

@dataclass
class ExecutionContext:
    inputs: dict[str, DataInstance]
    outputs: dict[str, DataInstance]
    transform_definition: Path
    type_libraries: list[Path]
    _shell: Callable = None

    def Shell(self, cmd: str, timeout: int|float|None=None) -> tuple[list[str], list[str]]:
        assert self._shell is not None, "shell not set"
        self._shell(cmd, timeout)

    @classmethod
    def Unpack(cls, d: dict):
        return cls(
            inputs={k:DataInstance.Unpack(v) for k, v in d["inputs"].items()},
            outputs={k:DataInstance.Unpack(v) for k, v in d["outputs"].items()},
            transform_definition=Path(d["transform_definition"]),
            type_libraries=[Path(p) for p in d["type_libraries"]],
        )
    
    def Pack(self):
        return {
            "inputs": {k:v.Pack() for k, v in self.inputs.items()},
            "outputs": {k:v.Pack() for k, v in self.outputs.items()},
            "transform_definition": str(self.transform_definition),
            "type_libraries": [str(p) for p in self.type_libraries],
        }

@dataclass
class ExecutionResult:
    success: bool = False
