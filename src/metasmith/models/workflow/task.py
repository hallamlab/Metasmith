from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable, Literal

import yaml

from ..libraries import DataInstance, DataInstanceLibrary, TransformInstanceLibrary
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
            data_libraries=[lib.GetKey() for lib in self.data_libraries],
            transform_libraries=[lib.GetKey() for lib in self.transform_libraries],
        )

    def SaveAs(self, dest: Source, partial: str|Literal[False]=False):
        assert partial in {"data_only", "transforms_only", False}
        with TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            _task_path = temp_dir/"task.yml"
            with open(_task_path, "w") as f:
                yaml.dump(dict(
                    task=self.Pack(),
                    plan=self.plan.Pack(),
                ), f)
            _mover = Logistics()
            _mover.QueueTransfer(
                src=Source(address=str(temp_dir), type=SourceType.DIRECT),
                dest=dest,
            )
            if partial != "transforms_only":
                for lib in self.data_libraries:
                    _temp_mover = lib.PrepTransfer(dest/f"data/{lib.GetKey()}")
                    _mover._queue.extend(_temp_mover._queue)
            if partial != "data_only":
                for lib in self.transform_libraries:
                    _temp_mover = lib.PrepTransfer(dest/f"transforms/{lib.GetKey()}")
                    _mover._queue.extend(_temp_mover._queue)
            res = _mover.ExecuteTransfers(wait_for_complete=True)
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
        return cls(
            ok=raw_task["ok"],
            plan=plan,
            data_libraries=[data_libs[n] for n in raw_task["data_libraries"]],
            transform_libraries=[tr_libs[n] for n in raw_task["transform_libraries"]],
        )
