from __future__ import annotations

import os
import shutil
import sys
import threading
from dataclasses import dataclass, field
from importlib import __import__, reload
from pathlib import Path
from typing import Callable

from ...constants import MODULE_PATH
from ...env.dispatch_scan import EnvScan, ScanFile
from ...hashing import KeyGenerator
from ...logging import Log
from ..remote import Source
from ..solver import Dependency, Endpoint, Transform
from .execution import ExecutionContext, ExecutionResult
from .instances import DataInstanceLibrary, DataInstanceLibraryView
from .resources import Resources
from .types import DataTypeLibrary


@dataclass
class TransformInstance:
    protocol: Callable[[ExecutionContext], ExecutionResult|list[ExecutionResult]]
    model: Transform
    group_by: Dependency
    name: str|None = None
    resources: Resources|None = None
    batch_size: int = 1
    labels: list[str] = field(default_factory=list)
    cacheable: bool = True
    output_signature: dict = field(default_factory=dict)
    _path: Path = field(default_factory=Path)
    _key: str = ""
    _hash: int = -1
    _env_scan: "EnvScan|None" = None
    _env_deps: list[Dependency] = field(default_factory=list)
    _protocol_source_hash: str = ""

    def __post_init__(self):
        assert self.batch_size>0, self.model
        assert self.group_by in self.model.requires, self.model
        for k, vt in [
            ("protocol", Callable),
            ("model", Transform),
        ]:
            v = getattr(self, k)
            assert isinstance(v, vt), f"[{k}] must be of type [{vt}] but got [{type(v)}]"
        TransformInstance._last_loaded_transform = self

    def GetKey(self):
        return self._key

    def __hash__(self) -> int:
        return self._hash

    _load_lock = threading.RLock()

    @classmethod
    def Load(cls, parent_lib: Path, definition: Path) -> TransformInstance|None:
        with cls._load_lock:
            return cls._LoadUnlocked(parent_lib, definition)

    @classmethod
    def _LoadUnlocked(cls, parent_lib: Path, definition: Path) -> TransformInstance|None:
        cls._last_loaded_transform: TransformInstance | None = None

        here = str(parent_lib/definition.parent)
        sys.path.insert(0, here)
        try:
            m = __import__(f"{definition.stem}")
            reload(m)
            assert cls._last_loaded_transform is not None
            tr = cls._last_loaded_transform
            tr.name = definition.stem
            tr._path = definition
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
            tr._hash, tr._key = tr.model.hash, tr.model.key
            try:
                src_text = (parent_lib / definition).read_text(
                    encoding="utf-8", errors="replace"
                )
                _, tr._protocol_source_hash = KeyGenerator.FromStr(src_text, l=12)
            except OSError:
                tr._protocol_source_hash = ""
            return cls._last_loaded_transform
        finally:
            try:
                sys.path.remove(here)
            except ValueError:
                pass

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

    def PruneTypes(self, save: bool=True, whitelist: set|None=None):
        # Every manifest entry of a transform library is `transforms::transform`,
        # so what the library actually needs is only visible by importing each
        # transform and reading the types it declares. A caller that already
        # holds those transforms passes the whitelist instead.
        if whitelist is None:
            whitelist = set()
            for _path, tr in self.IterateTransforms():
                whitelist |= set(tr.model.requires)
                whitelist |= {d for group in tr.model.produces for d in group}
        super().PruneTypes(save=save, whitelist=whitelist)

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
        self.Save()
        inst = TransformInstance.Load(self.location, path)
        return inst

    _parent_library_cache: dict[Path, tuple[tuple, "TransformInstanceLibrary"]] = {}

    @classmethod
    def _library_signature(cls, root: Path) -> tuple:
        meta = root/DataInstanceLibrary._path_to_meta
        try:
            st = meta.stat()
            stamp = [(meta.name, st.st_mtime_ns, st.st_size)]
        except OSError:
            return ()
        try:
            for e in os.scandir(root):
                if not e.name.endswith(".py"):
                    continue
                s = e.stat()
                stamp.append((e.name, s.st_mtime_ns, s.st_size))
        except OSError:
            return ()
        return tuple(sorted(stamp))

    @classmethod
    def ResolveParentLibrary(cls, transform_definition_file: Path|str):
        path = Path(transform_definition_file)
        for p in path.parents:
            if (p/DataInstanceLibrary._path_to_meta).exists():
                root = p.resolve()
                sig = cls._library_signature(root)
                hit = cls._parent_library_cache.get(root)
                if hit is not None and hit[0] == sig:
                    return hit[1]
                lib = cls.Load(p)
                cls._parent_library_cache[root] = (sig, lib)
                return lib
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
        return TransformInstanceLibraryView(self, mask, invert)

    @classmethod
    def Load(cls, path: Path|str, **kwargs):
        return cls(DataInstanceLibrary.Load(path, **kwargs))

    @classmethod
    def LoadFrom(cls, src: Source, dest: Path, label: str|None=None):
        return cls(DataInstanceLibrary.LoadFrom(src, dest, label=label))


class TransformInstanceLibraryView(DataInstanceLibraryView):
    _original: "TransformInstanceLibrary"

    def IterateTransforms(self):
        for p in sorted(self._mask):
            tr = self._original.GetTransform(p)
            assert tr is not None, p
            yield p, tr

    def GetTransform(self, path: str|Path, reload: bool=False):
        p = Path(path)
        if p.suffix != ".py":
            p = p.with_suffix(".py")
        assert p in self._mask, f"transform [{p}] is hidden by view mask"
        return self._original.GetTransform(p, reload=reload)

    def _prune_whitelist(self) -> set:
        # Read off the masked transforms, which the source library already
        # imported while planning -- the image's own copies are never loaded.
        wl: set[Dependency] = set()
        for _path, tr in self.IterateTransforms():
            wl |= set(tr.model.requires)
            wl |= {d for group in tr.model.produces for d in group}
        return wl

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
