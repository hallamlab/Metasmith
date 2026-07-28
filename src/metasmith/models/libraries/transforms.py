"""Transforms as the library sees them: one Python file per tool, loaded.

`TransformInstance.Load` is the reason planning is not reentrant. It imports a
definition by bare module name, mutates `sys.path`, calls `importlib.reload`,
and returns through a *class* attribute -- all process-global. Two concurrent
loads in one process clobber each other; the loud failure is `spec not found
for the module`, and the quiet one is a transform arriving as `None` because a
concurrent load reset the class attribute first. Every caller that imports a
transform must hold the same lock, which is more callers than just the ones
that plan.

Two digests come off a definition and they are not interchangeable.
`_key`/`_hash` stay equal to the model's topology, because the Nextflow process
name derives from them and a script edit should still reuse the work dir.
`_protocol_source_hash` digests the file's bytes, and it is what the *lineage*
cache folds in so that editing a protocol's body busts cross-run reuse even
when the I/O types are unchanged.
"""

from __future__ import annotations

import shutil
import sys
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
    cacheable: bool = True
    # backward-compat shim: `output_signature` was removed from the model but ~23
    # legacy std transforms still pass it. Accept-and-ignore so the full std
    # library imports cleanly (outputs derive from context.Output regardless).
    output_signature: dict = field(default_factory=dict)
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
    # R5 (F1 fix): stable digest of the transform's definition-file bytes.
    # Folded into the lineage cache signature so that editing a transform's
    # protocol (its command/logic) busts the cross-run cache even when the
    # I/O type topology is unchanged. Kept SEPARATE from _key/_hash (which
    # stay = model.key/model.hash for Nextflow process naming), decoupling
    # cache correctness from nxf process identity. Empty string when the
    # definition file was unreadable at Load time (degrades to topology-only).
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
            # _key/_hash stay = model topology so that updates to a script
            # still reuse the existing *Nextflow* work-dir cache (the process
            # name is derived from these). Do NOT fold protocol identity here.
            tr._hash, tr._key = tr.model.hash, tr.model.key
            # R5 (F1 fix): separately digest the definition-file bytes so the
            # *lineage* cache signature (workflow.py) can distinguish two
            # transforms that share an I/O type topology but differ in body.
            # Content only (not path) so byte-identical definitions at
            # different library roots still collide -> cross-run reuse holds.
            try:
                src_text = (parent_lib / definition).read_text(
                    encoding="utf-8", errors="replace"
                )
                _, tr._protocol_source_hash = KeyGenerator.FromStr(src_text, l=12)
            except OSError:
                tr._protocol_source_hash = ""
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
