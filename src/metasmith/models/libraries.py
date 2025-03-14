from __future__ import annotations
import shutil
import os, sys
from pathlib import Path
import yaml
from dataclasses import dataclass, field
from typing import Callable, Iterable
from importlib import metadata, reload, __import__

from ..serialization import IsText
from ..coms.containers import ContainerRuntime, Container
from ..coms.ipc import LiveShell, RemoveLeadingIndent
from .solver import Dependency, Endpoint, Transform
from .remote import GlobusSource, Logistics, Source, SourceType
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
# _dataTypeLibrary_cache: dict[Path, DataTypeLibrary] = {}
# _dataTypeLibrary_history: list[str] = []

@dataclass
class DataTypeLibrary:
    schema: str = VERSION
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
        params = dict(
            types={k: Endpoint.Unpack(v) for k, v in d["types"].items()},
        )
        if "schema" in d:
            params["schema"] = str(d["schema"])
        if "ontology" in d:
            params["ontology"] = DataTypeOntology.Unpack(d["ontology"])
        return cls(**params)

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
        self._hash, self._key = KeyGenerator.FromStr("".join([
            str(self.path),
            self.dtype.key,
            self.dtype_name,
            self.parent_lib.GetKey(),
        ]), l=8)

    def __hash__(self) -> int:
        return self._hash
    
    def GetDType(self) -> tuple[str, str]:
        return tuple(self.dtype_name.split("::"))

    def ResolvePath(self):
        return self.parent_lib.location/self.path

    def Pack(self):
        return dict(
            path=str(self.path),
            dtype=f"{self.parent_lib.GetKey()}::{self.dtype_name}",
        )
    
    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataTypeLibrary]):
        lib_key, namespace, dtype_name = raw["dtype"].split("::")
        lib = libraries[lib_key]
        dtype = lib.types[namespace][dtype_name]
        return cls(
            path=Path(raw["path"]),
            dtype=dtype,
            dtype_name=f"{namespace}::{dtype_name}",
            parent_lib=lib,
        )

class DataInstanceLibrary:
    schema: str = VERSION
    _path_to_meta: Path = Path("./_metadata")
    _path_to_types: Path = Path("./_metadata/types")
    _index_name: str = "index"
    _metadata_ext: str = ".yml"

    def __init__(self, location: Path|str|DataInstanceLibrary) -> None:
        self.manifest: dict[Path, str] = {}
        self.types: dict[str, DataTypeLibrary] = {}
        self._dtype2name = {}
        self.remote_src: Source|None = None
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

    def AddTypeLibrary(self, namespace: str, lib: DataTypeLibrary|Source):
        assert namespace not in self.types, f"namespace [{namespace}] already exists"
        if isinstance(lib, DataTypeLibrary):
            self.types[namespace] = lib
        else:
            mover = Logistics()
            ext = self._metadata_ext
            namespace = namespace.replace(".yml", "").replace(ext, "")
            meta_path = self.location/self._path_to_types
            meta_path.mkdir(parents=True, exist_ok=True)
            lib_path = meta_path/(namespace+ext)
            lib_dest = Source(address=lib_path, type=SourceType.DIRECT)
            mover.QueueTransfer(
                src=lib,
                dest=lib_dest,
            )
            res = mover.ExecuteTransfers()
            assert len(res.completed) == 1, f"failed to add type library [{namespace}]"
            self.types[namespace] = DataTypeLibrary.Load(lib_dest.address)
        return self.types[namespace]

    @classmethod
    def _get_type(self, name: str, types: dict[str, DataTypeLibrary]):
        namespace, name = name.split("::")
        assert namespace in types, f"namespace [{namespace}] not found"
        types_lib = types[namespace]
        assert name in types_lib, f"datatype [{name}] not found in [{namespace}]"
        return types_lib[name]

    def GetType(self, name: str):
        return self._get_type(name, self.types)
    
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
            yield k, v, self.GetType(v)

    def Add(self, items: list[tuple[Path|str, Path|str, str]], method: SourceType=SourceType.DIRECT, on_exist: str="skip"):
        """
        @items: list of (source, destination, datatype)
        """
        assert method in {SourceType.DIRECT, SourceType.SYMLINK}
        assert on_exist in {"skip", "error", "clear", "update"}
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
            self.GetType(dtype) # check if datatype exists
            dest_path = self.location/dest
            if dest_path.exists():
                if on_exist == "skip":
                    completed.add(str(src))
                    continue
                elif on_exist == "error":
                    raise FileExistsError(f"destination [{dest}] already exists")
                elif on_exist == "clear":
                    Log.Warn(f"clearing previous [{dest}]")
                    shutil.rmtree(dest_path)
                elif on_exist == "update":
                    pass # default of mover
            mover.QueueTransfer(
                src = Source.FromLocal(src),
                dest = Source(address=dest_path, type=method),
            )
        res = mover.ExecuteTransfers()
        completed |= {str(Path(s.address)) for s, d in res.completed}
        report: list[Path] = []
        for src, dest, dtype in items:
            k = str(src)
            if k not in completed:
                Log.Error(f"failed to add [{src}]")
                continue
            self.manifest[dest] = dtype
            report.append(dest)
        return report

    def _calculate_key(self):
        me_d = self.Pack()
        for k in ["remote_src"]:
            if k in me_d: del me_d[k]
        me = yaml.dump(me_d)
        dtypes = yaml.dump({k:v.Pack() for k, v in self.types.items()})
        self._hash, self._key = KeyGenerator.FromStr(me+dtypes, l=12)
        return self._key
    
    def GetKey(self):
        if not hasattr(self, "_key"):
            self._calculate_key()
        return self._key

    def __hash__(self) -> int:
        if not hasattr(self, "_hash"):
            self._calculate_key()
        return self._hash

    def Pack(self):
        return dict(
            schema=self.schema,
            manifest={str(k):str(v) for k, v in self.manifest.items()},
            remote_src=self.remote_src.Pack() if self.remote_src is not None else None,
        )

    @classmethod
    def Unpack(cls, location: Path, raw: dict, dtypes: dict[str, DataTypeLibrary], check_integrity: bool=False):
        manifest = {}
        for k, v in raw["manifest"].items():
            if check_integrity:
                assert (location/k).exists(), f"[{k}], does not exist"
            cls._get_type(v, dtypes) # check if datatype exists
            manifest[Path(k)] = v
        lib = cls(
            location=location,
        )
        lib.schema = raw["schema"]
        lib.manifest = manifest
        remote_src = raw.get("remote_src")
        lib.remote_src = Source.Unpack(remote_src) if remote_src is not None else None
        return lib

    def Save(self, update_types=False):
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

        with open(index_path) as f:
            d = yaml.safe_load(f)
            self = cls.Unpack(location=path, raw=d, dtypes=dtypes, check_integrity=check_integrity)
        self.types = dtypes
        return self

    def PrepTransfer(self, dest: Source, mover: Logistics=None):
        self.Save()
        if mover is None:
            mover = Logistics()
        mover.QueueTransfer(
            src=Source.FromLocal(self.location),
            dest=dest,
        )
        return mover

    def SaveAs(self, dest: Source, label: str=None):
        mover = self.PrepTransfer(dest)
        res = mover.ExecuteTransfers(label=label)
        assert len(res.completed) == 1, f"move failed"
        return res

    @classmethod
    def LoadFrom(cls, src: Source, dest: Path|str, as_image=True, on_exist: str = "skip", label: str=None):
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
            res = mover.ExecuteTransfers(label=label)
            assert len(res.completed) == 1, f"move failed"
        if dest.exists():
            if on_exist == "error":
                raise FileExistsError(f"[{dest}] already exists")
            elif on_exist == "update":
                _transfer()
            elif on_exist == "clear":
                Log.Warn("clearing previously loaded library")
                shutil.rmtree(dest)
            elif on_exist == "skip":
                pass
        else:
            _transfer()

        lib = cls.Load(dest, check_integrity=False)
        if as_image:
            lib.remote_src = src
            lib.Save()
        return lib
    
    def Actualize(self, extern_dest: Source=None, label: str=None):
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

# this should function like a view provided by the parent library
@dataclass
class TransformInstance:
    protocol: Callable[[ExecutionContext], ExecutionResult]
    model: Transform
    output_signature: dict[Dependency, Path]
    name: str = None

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

    def GetKey(self):
        return self._key # from definition file upon load

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
                tr.name = definition.stem
                with open(definition) as f:
                    raw = "".join(f.readlines())
                    h, k = KeyGenerator.FromStr(raw, l=5)
                    tr._hash, tr._key = h, k
                return cls._last_loaded_transform
        finally:
            sys.path = original_path_var

class TransformInstanceLibrary(DataInstanceLibrary):
    def __init__(self, location: Path|str|DataInstanceLibrary) -> None:
        super().__init__(location)
        if "transforms" not in self.types:
            transform_types = DataTypeLibrary(types=dict(
                transform=Endpoint({"metasmith", "transform"}),
                example_input=Endpoint({"metasmith", "example_input"}),
                example_output=Endpoint({"metasmith", "example_output"}),
            ))
            self.AddTypeLibrary("transforms", transform_types)
        self.Save()

    def AddStub(self, path: Path|str, exist_ok: bool=False):
        path = Path(path)
        assert not path.is_absolute(), f"path must be relative"
        HERE = Path(__file__).parent
        example = HERE/"_example_transform.py"
        if path.suffix != ".py":
            path = path.parent/(path.name+".py")

        results = self.Add([(example, path, "transforms::transform")], on_exist="skip" if exist_ok else "error")
        assert len(results) == 1, f"failed to add transform at [{path}]"
        inst = TransformInstance.Load(self.location/path)
        return inst

    @classmethod
    def ResolveParentLibrary(cls, transform_definition_file: Path|str):
        path = Path(transform_definition_file)
        for p in path.parents:
            if (p/DataInstanceLibrary._path_to_meta).exists():
                return cls.Load(p)
    
    def GetTransform(self, path: Path|str):
        path = self.location/path
        if path.suffix != ".py":
            path = path.with_suffix(".py")
        return TransformInstance.Load(path)
    
    def IterateTransforms(self):
        for k, v, dtype in self.Iterate():
            tr = self.GetTransform(k)
            assert tr is not None
            yield k, v, tr

    @classmethod
    def Load(cls, path: Path|str):
        return cls(DataInstanceLibrary.Load(path))

    @classmethod
    def LoadFrom(cls, src: Source, dest: Path, label: str=None):
        return cls(DataInstanceLibrary.LoadFrom(src, dest, label=label))

@dataclass
class ContextPath:
    local: Path
    external: Path
    container: Path

@dataclass
class ExecutionContext:
    inputs: dict[Endpoint, ContextPath]
    outputs: dict[Endpoint, ContextPath]
    external_shell: LiveShell
    external_cwd: Path
    container_runtime: ContainerRuntime

    def ExecWithContainer(self, image: Endpoint, cmd: str, binds: list[tuple[Path, Path]]=None):
        path = self.inputs[image]
        image = path.external
        if IsText(path.local):
            with open(path.local) as f:
                image = f.read() # using the uri
        
        _binds = set()
        for _, p in list(self.inputs.items())+list(self.outputs.items()):
            src = p.external.parent
            dest = p.container.parent
            _binds.add((src, dest))
        if binds is None: binds = []
        binds += sorted([(s, d) for s, d in _binds])
        binds += [
            (self.external_cwd, Path("/ws")),
        ]

        container_ws = Path("/ws")
        container = Container(
            image = image,
            workdir = container_ws,
            runtime = self.container_runtime,
            binds = binds,
        )

        cmd = RemoveLeadingIndent(cmd)
        Log.Info(f"executing container [{image}] using [{container.runtime}]")
        Log.Info(f"command:")
        for line in cmd.split("\n"):
            Log.Info(f"    {line}")
        Log.Info(f"binds:")
        for s, d in binds:
            Log.Info(f"    {s} -> {d}")
        _, _hash = KeyGenerator.FromStr(cmd, l=8)
        cmd_file = f"_metasmith/container_cmd.{_hash}"
        container_run = container.MakeRunCommand()
        with open(cmd_file, "w") as f:
            f.write(f"# {container_run}"+"\n")
            f.write(cmd)
        return self.external_shell.Exec(f"""\
            {container_run} bash {container_ws/cmd_file}
        """, timeout=None)

@dataclass
class ExecutionResult:
    success: bool = False
