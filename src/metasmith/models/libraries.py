from __future__ import annotations
import sys
from pathlib import Path
import yaml
from datetime import datetime as dt
from dataclasses import dataclass, field
from typing import Callable, Iterable
from importlib import reload, __import__
from hashlib import sha256

from metasmith.models.solver import Endpoint

from ..logging import Log

class NotImplementedException(Exception):
    pass

def str_hash(s):
    return int(sha256(s.encode("utf-8", "replace")).hexdigest(), 16)
    
_data_type_cache: dict[str, DataType] = {}
@dataclass
class DataType:
    name: str
    properties: dict[str, str|list[str]]
    library: DataTypeLibrary
    ancestors: list[DataType] = field(default_factory=list)
    
    def __post_init__(self):
        _data_type_cache[self.name] = self

    def __hash__(self) -> int:
        if not hasattr(self, "_hash"):
            self._hash = str_hash(''.join(self.AsProperties()))
        return self._hash

    def WithAncestors(self, ancestors: Iterable[DataType]):
        return DataType(self.name, self.properties, self.library, list(ancestors))

    @classmethod
    def SetFromDict(cls, raw: dict[str, str|list[str]]):
        return set(f"{k}={','.join(v) if isinstance(v, list) else v}" for k, v in raw.items())

    def AsProperties(self):
        return self.SetFromDict(self.properties)
    
    @classmethod
    def Unpack(cls, d: dict):
        an = d.get("ancestors")
        if an is not None:
            an = [_data_type_cache[a] for a in an]
        return cls(
            name=d["name"],
            properties=d["properties"],
            library=DataTypeLibrary.Load(d["library"]),
            ancestors=an,
        )

    def Pack(self):
        an = [a.name for a in self.ancestors]
        return {
            "name": self.name,
            "properties": self.properties,
            "library": str(self.library.key),
        } | ({"ancestors": an} if len(an)>0 else {})

_library_cache: dict[str, DataTypeLibrary] = {}
@dataclass
class DataTypeLibrary:
    key: str
    source: Path
    schema: str
    ontology: dict
    types: dict[str, DataType] = field(default_factory=dict)

    def __getitem__(self, key: str) -> DataType:
        return self.types[key]
    
    def __in__(self, key: str) -> bool:
        return key in self.types

    @classmethod
    def Load(cls, path_or_key: Path|str) -> DataTypeLibrary:
        # todo, urls
        if isinstance(path_or_key, str):
            if path_or_key in _library_cache:
                return _library_cache[path_or_key]
            path = Path(path_or_key)
        else:
            path = path_or_key
        key = path.stem
        if key in _library_cache:
            return _library_cache[key]
        path = path.resolve()
        with open(path) as f:
            d = yaml.safe_load(f)
        lib = cls(key=key, source=path, schema=d["schema"], ontology=d["ontology"])
        types: dict[str, DataType] = {}
        for k, v in d["types"].items():
            types[k] = DataType(
                name=k,
                properties=v,
                library=lib,
            )
        lib.types = types
        _library_cache[key] = lib
        return lib

_data_instance_cache: dict[Path, DataInstance] = {}
@dataclass
class DataInstance:
    source: Path
    type: DataType

    def __post_init__(self):
        _data_instance_cache[self.source] = self

    def __hash__(self) -> int:
        if not hasattr(self, "_hash"):
            self._hash = str_hash(str(self.source.resolve())+''.join(self.type.AsProperties()))
        return self._hash

    @classmethod
    def Unpack(cls, d: dict):
        return cls(
            source=Path(d["source"]),
            type=DataType.Unpack(d["type"]),
        )
    
    def Pack(self):
        return {
            "source": str(self.source),
            "type": self.type.Pack(),
        }

@dataclass
class DataInstanceLibrary:
    description: str
    types_library: DataTypeLibrary
    manifest: dict[str, DataInstance] = field(default_factory=dict)
    time_created: dt = field(default_factory=lambda: dt.now())
    time_modified: dt = field(default_factory=lambda: dt.now())

    def __getitem__(self, key: str) -> DataInstance:
        return self.manifest[key]
    
    @classmethod
    def Load(cls, path: Path):
        if not hasattr(cls, "_loaded_libraries"):
            cls._loaded_libraries = {}
        path = path.resolve()
        if path in cls._loaded_libraries:
            return cls._loaded_libraries[path]

        with open(path) as f:
            d = yaml.safe_load(f)

        class_attributes = set(cls.__annotations__.keys())
        TYPE_LIB = "types_library"
        d[TYPE_LIB] = DataTypeLibrary.Load(Path(d[TYPE_LIB]))
        for k, v in d.items():
            assert k in class_attributes, f"unexpected field [{k}]"
            if k == "manifest":
                manifest = {}
                for kk, vv in v.items():
                    type = d[TYPE_LIB][vv["type"]]
                    manifest[kk] = DataInstance(
                        source=Path(vv["source"]),
                        type=type,
                    )
                d[k] = manifest

        inst = cls(**d)
        cls._loaded_libraries[path] = inst
        return inst

    def Dump(self, path: Path):
        self.time_modified = dt.now()
        with open(path, "w") as f:
            d = {}
            for k, v in self.__dict__.items():
                if k.startswith("_"): continue
                if callable(v): continue
                if k == "types_library":
                    v = dict(source=str(v.path), key=v.key)
                elif k == "manifest":
                    v = {kk: vv.Pack() for kk, vv in v.items()}
                d[k] = v
            yaml.safe_dump(d, f, indent=4)

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

@dataclass
class TransformInstance:
    protocol: Callable[[ExecutionContext], ExecutionResult]
    input_signature: set[DataType]
    output_signature: set[DataInstance]
    source: Path = None

    @classmethod
    def Load(cls, definition: Path) -> TransformInstance|None:
        cls._last_loaded_transform = None

        original_path_var = sys.path
        sys.path = [str(definition.parent)]+sys.path
        try:
            m = __import__(f"{definition.stem}")
            reload(m)
            if cls._last_loaded_transform is not None:
                cls._last_loaded_transform.source = definition
                return cls._last_loaded_transform
        finally:
            sys.path = original_path_var
            
    # this is called from within a transform definition
    @classmethod
    def Register(
        cls,
        # container: str|Path,
        protocol: Callable[[ExecutionContext], ExecutionResult],
        input_signature: set[DataType],
        output_signature: set[DataInstance],
    ):
        # assert isinstance(container, str) or isinstance(container, Path), "[container] must be a container url or path"
        assert isinstance(protocol, Callable), "[protocol] must be a function"
        assert isinstance(input_signature, set), "[input_signature] must be a set of DataType"
        assert isinstance(output_signature, set), "[output_signature] must be a set of DataInstance"

        cls._last_loaded_transform = cls(
            # container=container,
            protocol=protocol,
            input_signature=input_signature,
            output_signature=output_signature,
        )

@dataclass
class TransformInstanceLibrary:
    manifest: dict[Path, dict[Path, TransformInstance]]

    def __getitem__(self, key: str) -> DataType:
        return self.manifest[key]
    
    def __iter__(self):
        for root, section in self.manifest.items():
            for path, tr in section.items():
                yield root/path, tr

    def __len__(self):
        return sum(len(s) for s in self.manifest.values())

    @classmethod
    def Load(cls, path: Path|Iterable[Path], silent=True) -> TransformInstanceLibrary:
        if isinstance(path, Path):
            path = [path]
        failures = []
        manifest = {}
        for root in path:
            root = root.resolve()
            assert root.is_dir(), "TransformInstanceLibrary must be a directory"
            section = {}
            for p in root.glob("**/*.py"):
                if p.is_dir(): continue
                k = p.relative_to(root)

                inst = TransformInstance.Load(p)
                if inst is None:
                    if not silent: Log.Warn(f"could not load [{k}] from [{root}]")
                    failures.append(k)
                else:
                    section[k] = inst
                manifest[root] = section
        return cls(manifest)
