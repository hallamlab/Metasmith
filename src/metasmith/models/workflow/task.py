from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable, Literal

import yaml

from ...hashing import KeyGenerator
from ..libraries import (
    DataInstance, DataInstanceLibrary, DataInstanceLibraryView,
    TransformInstanceLibrary,
)
from ..paths import DeferredPathError, is_deferred
from ..remote import Logistics, Source, SourceType
from .cache_decisions import compute_cache_decisions
from .nextflow_codegen import NextflowGenContext, apply_fs_strategy, prepare_nextflow
from .plan import WorkflowPlan


@dataclass
class WorkflowTask:
    ok: bool
    plan: WorkflowPlan
    data_libraries: list[DataInstanceLibrary] = field(default_factory=list)
    transform_libraries: list[TransformInstanceLibrary] = field(default_factory=list)

    def __post_init__(self):
        self._update_hash()

    def _update_hash(self):
        self._hash, self._key = self.plan._hash, self.plan._key

    def GetKey(self):
        return self._key

    def _apply_fs_strategy(self, context: NextflowGenContext) -> None:
        apply_fs_strategy(context)

    def _compute_cache_decisions(self, context: NextflowGenContext) -> dict[int, dict]:
        return compute_cache_decisions(self, context)

    def PrepareNextflow(self, context: NextflowGenContext):
        return prepare_nextflow(self, context)

    def _get_common_folders(self, folders:Iterable[Path]):
        roots: list[str] = []
        def join(a, b):
            return os.path.commonpath([a, b])

        def update(p: str):
            nonlocal roots
            bi, best, result = None, None, ""
            for i, r in enumerate(roots):
                x = join(r, p)
                if x == "/": continue
                score = len(r)-len(x)
                if best is None or score<best:
                    bi, best, result = i, score, x
            if bi is not None:
                roots[bi] = result
            else:
                roots.append(p)

        for path in folders:
            update(str(path))
        return [Path(p) for p in roots]

    def GetCommonInputFolders(self, method="external"):
        assert method in {"external", "internal", "all"}
        def should_keep(inst: DataInstance):
            match(method):
                case "external":
                    return inst.path.is_absolute()
                case "internal":
                    return not inst.path.is_absolute()
                case "all":
                    return True
        given = {inst.ResolvePath().parent for inst in self.plan.given if should_keep(inst)}
        return self._get_common_folders(given)

    def DeferredInputs(self) -> list[DataInstance]:
        return [inst for inst in self.plan.given if is_deferred(inst.path)]

    def RefuseIfDeferred(self) -> None:
        deferred = self.DeferredInputs()
        if not deferred:
            return
        rows = "\n".join(
            f"  - {inst.dtype_name}  ({inst.path})" for inst in deferred
        )
        raise DeferredPathError(
            f"cannot stage workflow [{self._key}]: {len(deferred)} input(s) have "
            f"no path yet:\n{rows}\n"
            f"These plan fine but there is nothing on disk to send. Point each row "
            f"at a real file before staging."
        )

    def Pack(self):
        return dict(
            ok=self.ok,
            key=self._key,
            data_libraries=[lib.GetKey() for lib in self.data_libraries],
            transform_libraries=[lib.GetKey() for lib in self.transform_libraries],
        )

    def LibraryMasks(self) -> dict[str, set[Path]]:
        # Per library key, the manifest entries this plan resolved against.
        # `plan.given` is already narrowed to the endpoints the plan consumes,
        # which is what makes this worth asking.
        masks: dict[str, set[Path]] = {}
        def note(lib, path):
            if lib is None or path is None: return
            masks.setdefault(lib.GetKey(), set()).add(Path(path))

        for step in self.plan.steps:
            note(step.transform_library, step.transform._path)
            for group in step.dependency_map.values():
                for inst in group:
                    note(inst.parent_lib, inst.path)
        for inst in self.plan.given:
            note(inst.parent_lib, inst.path)
        for target in self.plan.targets:
            note(target.instance.parent_lib, target.instance.path)

        libraries = {lib.GetKey(): lib for lib in self.data_libraries + self.transform_libraries}
        for key, lib in libraries.items():
            masks[key] = masks.get(key, set()) & set(lib.manifest)

        # Codegen dereferences a kept instance's ancestors unguarded, and an
        # ancestor may live in another library -- so the closure crosses keys.
        # Iterated to a fixpoint: `parents` is only guaranteed transitively
        # flattened on a library that came back through Load.
        changed = True
        while changed:
            changed = False
            for key, lib in libraries.items():
                for path in list(masks.get(key, set())):
                    for pm in lib.parents.get(path, []):
                        target = libraries.get(pm.library_key)
                        if target is None or pm.path not in target.manifest: continue
                        seen = masks.setdefault(pm.library_key, set())
                        if pm.path in seen: continue
                        seen.add(pm.path)
                        changed = True
        return masks

    def _to_stage(self, lib, masks: dict[str, set[Path]], prune: bool):
        if not prune:
            return lib
        mask = masks.get(lib.GetKey())
        if mask is None:
            return lib
        if isinstance(lib, DataInstanceLibraryView):
            mask = mask & lib._mask
            lib = lib._original
        return lib.AsView(mask)

    def SaveAs(self, dest: Source, partial: str|Literal[False]=False, prune: bool=True):
        assert partial in {"data_only", "transforms_only", False}
        masks = self.LibraryMasks() if prune else {}
        with TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            _task_path = temp_dir/"task.yml"
            with open(_task_path, "w") as f:
                yaml.dump(dict(
                    task=self.Pack(),
                    plan=self.plan.Pack(),
                ), f)
            _images = temp_dir.parent/f"{temp_dir.name}.images"
            _mover = Logistics()
            _mover.QueueTransfer(
                src=Source(address=str(temp_dir), type=SourceType.DIRECT),
                dest=dest,
            )
            def _queue(libs, kind: str):
                for lib in libs:
                    key = lib.GetKey()
                    staged = self._to_stage(lib, masks, prune)
                    if isinstance(staged, DataInstanceLibraryView):
                        _temp_mover = staged.PrepTransfer(
                            dest/f"{kind}/{key}", image_root=_images/kind/key,
                        )
                    else:
                        _temp_mover = staged.PrepTransfer(dest/f"{kind}/{key}")
                    _mover._queue.extend(_temp_mover._queue)
            try:
                if partial != "transforms_only":
                    _queue(self.data_libraries, "data")
                if partial != "data_only":
                    _queue(self.transform_libraries, "transforms")
                res = _mover.ExecuteTransfers(wait_for_complete=True)
            finally:
                shutil.rmtree(_images, ignore_errors=True)
            return res

    @classmethod
    def Load(cls, path: Path|str, alt_data_paths: list[Path|str]|None=None):
        path = Path(path)
        with open(path/"task.yml") as f:
            d = yaml.safe_load(f)
        raw_task = d["task"]
        raw_plan = d["plan"]

        _data_lib_paths = [Path(p) for p in alt_data_paths] if alt_data_paths else []
        _data_lib_paths += [path/"data"]
        def load_lib(lib_key: str):
            for d in _data_lib_paths:
                p = d/lib_key
                if p.exists():
                    return DataInstanceLibrary.Load(p)
            raise FileNotFoundError(f"could not find data library [{lib_key}], tried {_data_lib_paths}")
        data_libs = {n: load_lib(n) for n in raw_task["data_libraries"]}
        tr_libs = {n: TransformInstanceLibrary.Load(path/f"transforms/{n}") for n in raw_task["transform_libraries"]}
        _libraries: dict[str, DataInstanceLibrary] = data_libs|tr_libs
        plan =  WorkflowPlan.Unpack(raw_plan, _libraries)
        task = cls(
            ok=raw_task["ok"],
            plan=plan,
            data_libraries=[data_libs[n] for n in raw_task["data_libraries"]],
            transform_libraries=[tr_libs[n] for n in raw_task["transform_libraries"]],
        )
        # The key names the staged directory, so it is a fact about this bundle
        # rather than something to re-derive. `WorkflowPlan._update_hash` folds
        # the given ids in, and staging re-mints those from the host's own view
        # of the files -- without this, reloading a re-staged task would answer
        # with a key that no directory is called. Bundles written before the key
        # was recorded still recompute it, unchanged.
        pinned = raw_task.get("key")
        if pinned:
            plan._hash, _ = KeyGenerator.FromStr(pinned, l=8)
            plan._key = pinned
            task._update_hash()
        return task
