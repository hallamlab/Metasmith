from __future__ import annotations
import sys
from pathlib import Path
import yaml
from datetime import datetime as dt
from dataclasses import dataclass, field
from typing import Callable, Iterable
from importlib import reload, __import__
from hashlib import sha256

from ..logging import Log

class NotImplementedException(Exception):
    pass

def str_hash(s):
    return int(sha256(s.encode("utf-8", "replace")).hexdigest(), 16)
    
@dataclass
class DataType:
    name: str
    properties: dict[str, str]
    library: DataTypeLibrary
    
    def __hash__(self) -> int:
        if not hasattr(self, "_hash"):
            self._hash = str_hash(''.join(self.AsProperties()))
        return self._hash

    @classmethod
    def SetFromDict(cls, raw: dict[str, str]):
        return set(f"{k}={v}" for k, v in raw.items())

    def AsProperties(self):
        return self.SetFromDict(self.properties)
    
@dataclass
class DataTypeLibrary:
    path: Path
    schema: str
    ontology: dict
    types: dict[str, DataType] = field(default_factory=dict)

    def __getitem__(self, key: str) -> DataType:
        return self.types[key]
    
    def __in__(self, key: str) -> bool:
        return key in self.types

    @classmethod
    def Load(cls, path: Path) -> DataTypeLibrary:
        with open(path) as f:
            d = yaml.safe_load(f)
        lib = cls(path, d["schema"], d["ontology"])
        types = {}
        for k, v in d["types"].items():
            types[k] = DataType(
                name=k,
                properties=v,
                library=lib,
            )
        lib.types = types
        return lib

@dataclass
class DataInstance:
    source: Path
    type: DataType

    def __hash__(self) -> int:
        if not hasattr(self, "_hash"):
            self._hash = str_hash(str(self.source.resolve())+''.join(self.type.AsProperties()))
        return self._hash
    
    @classmethod
    def Register(cls, source: Path, type: DataType):
        return cls(source, type)
    
    def Pack(self):
        return {
            "source": str(self.source),
            "type": self.type.name,
            "properties": self.type.properties,
        }

@dataclass
class DataInstanceLibrary:
    description: str
    types_library: DataTypeLibrary
    manifest: dict[str, DataInstance] = field(default_factory=dict)
    time_created: dt = field(default_factory=lambda: dt.now())
    time_modified: dt = field(default_factory=lambda: dt.now())

    def __getitem__(self, key: str) -> DataType:
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
                    v = str(v.path)
                elif k == "manifest":
                    v = {kk: vv.Pack() for kk, vv in v.items()}
                d[k] = v
            yaml.safe_dump(d, f, indent=4)

@dataclass
class ExecutionContext:
    pass

@dataclass
class ExecutionResult:
    pass

@dataclass
class TransformInstance:
    container: str
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
        container: str|Path,
        protocol: Callable[[ExecutionContext], ExecutionResult],
        input_signature: set[DataType],
        output_signature: set[DataInstance],
    ):
        assert isinstance(container, str) or isinstance(container, Path), "[container] must be a container url or path"
        assert isinstance(protocol, Callable), "[protocol] must be a function"
        assert isinstance(input_signature, set), "[input_signature] must be a set of DataType"
        assert isinstance(output_signature, set), "[output_signature] must be a set of DataInstance"

        cls._last_loaded_transform = cls(
            container=container,
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
