from __future__ import annotations
from enum import Enum
import os, sys
import shutil
from pathlib import Path
import yaml
from dataclasses import dataclass, field
from typing import Callable, Iterable
from importlib import reload, __import__

from .solver import Dependency, Endpoint, Transform
from .remote import Source
from ..hashing import KeyGenerator
from ..logging import Log
from ..constants import VERSION

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
        version = 1.25,
        doi = "https://doi.org/10.1093/bioinformatics/btt113",
        strict = False,
    )

# caches by absolute path
_dataTypeLibrary_cache: dict[Path, DataTypeLibrary] = {}
_dataTypeLibrary_history: list[str] = []
@dataclass
class DataTypeLibrary:
    types: dict[str, Endpoint] = field(default_factory=dict)
    source: Path|None = None
    ontology: DataTypeOntology = field(default_factory= lambda: DataTypeOntologies.EDAM)
    schema: str = VERSION

    def __post_init__(self):
        if self.source is None: return
        _dataTypeLibrary_cache[self.source] = self

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

    def Subset(self, keys: Iterable[str]) -> DataTypeLibrary:
        ss = DataTypeLibrary(schema=self.schema, ontology=self.ontology)
        ss.types = {k: v.Clone() for k, v in self.types.items() if k in keys}
        return ss

    @classmethod
    def Proxy(cls, alt: str, lib: DataTypeLibrary):
        _dataTypeLibrary_cache[str(alt)] = lib

    @classmethod
    def Load(cls, path: str) -> DataTypeLibrary:
        path = str(path)
        _dataTypeLibrary_history.append(path)
        if path in _dataTypeLibrary_cache: return _dataTypeLibrary_cache[path]
        # todo, urls
        _path = Path(path)
        assert _path.is_absolute(), f"path to [{cls}] must be absolute"
        with open(_path) as f:
            d = yaml.safe_load(f)
        lib = cls(
            source=_path,
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
    source: Source
    type: Endpoint
    _key: str = None
    _hash: int = None

    def __post_init__(self):
        self._hash, self._key = KeyGenerator.FromStr(str(self.source)+''.join(self.type.properties))

    def __hash__(self) -> int:
        return self._hash

    @classmethod
    def Unpack(cls, d: dict):
        return cls(
            source=d["source"],
            type=Endpoint.Unpack(d["type"]),
        )
    
    def Pack(self, parents=False):
        return {
            "source": self.source,
            "type": self.type.Pack(parents=parents),
        }

@dataclass
class DataInstanceLibrary:
    source: Path = Path("./")
    manifest: dict[str, DataInstance] = field(default_factory=dict)
    schema: str = VERSION
    _index_name: str = "info.yml"

    def __post_init__(self):
        if not isinstance(self.source, Path): self.source = Path(self.source)

    def __getitem__(self, key: Path|str) -> DataInstance:
        key = str(key)
        return self.manifest[key]
    
    def __setitem__(self, key: str, value: DataInstance):
        assert isinstance(key, str)
        assert isinstance(value, DataInstance)
        self.manifest[key] = value

    def __in__(self, key: str) -> bool:
        key = str(key)
        return key in self.manifest

    def __iter__(self):
        for path, inst in self.manifest.items():
            yield self.source/path, inst

    def __len__(self):
        return len(self.manifest)

    def _index_path(self):
        return self.source/self._index_name

    # def ResolveAll(self, like: Endpoint, method: SourceType=SourceType.SYMLINK, overwrite: bool=False):
    #     self.source.mkdir(parents=True, exist_ok=True)

    #     if method in {SourceType.SYMLINK, SourceType.COPY}:
    #         source = Path(source)
    #         assert source.exists(), f"path doesn't exist [{source}]"
    #         if local_path is None:
    #             local_path = self.source/source.name
    #         else:
    #             assert not local_path.is_absolute(), "local_path must be relative"
    #             local_path = self.source/local_path
    #         if local_path.exists():
    #             if overwrite:
    #                 local_path.unlink()
    #             else:
    #                 raise FileExistsError(f"already exists [{local_path}]")
    #         self.source.mkdir(parents=True, exist_ok=True)
    #         if method == SourceType.COPY:
    #             shutil.copy(source, local_path)
    #         else:
    #             os.symlink(source, local_path)
    #         _key, _path = str(local_path.relative_to(self.source)), local_path.resolve()
    #     else:
    #         _key, _path = local_path.relative_to(self.source), local_path.resolve()

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
    _used_libraries: set[str] = field(default_factory=list)

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
                cls._last_loaded_transform._used_libraries = set(_dataTypeLibrary_history)
                return cls._last_loaded_transform
            _dataTypeLibrary_history.clear()
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
    input: list[DataInstance]
    output: list[DataInstance]
    transform_definition: Path
    type_libraries: list[Path]
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
            transform_definition=Path(d["transform_definition"]),
            type_libraries=[Path(p) for p in d["type_libraries"]],
        )
    
    def Pack(self):
        return {
            "input": [v.Pack(parents=True) for v in self.input],
            "output": [v.Pack(parents=True) for v in self.output],
            "transform_definition": str(self.transform_definition),
            "type_libraries": [str(p) for p in self.type_libraries],
        }

@dataclass
class ExecutionResult:
    success: bool = False
