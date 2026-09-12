# `WorkflowStep.dependency_map` is bound as a property *below* the class body on
# purpose: the field is an `InitVar`, and letting `@dataclass` see a
# class-attribute default of the same name would shadow that declaration. The
# setter treats an empty dict as "not resolved yet", so an `Unpack` that has not
# yet run `_resolve_dependency_map` does not wipe explicitly-passed views.
#
# `_resolve_dependency_map` indexes each instance under all of `instance_id`,
# `_key` and `legacy_key`, which is what lets a plan written by an older version
# resolve against a library that has since re-minted ids.
from __future__ import annotations

import itertools
from dataclasses import dataclass, field, InitVar

from ...logging import Log
from ..libraries import (
    DataInstance, DataInstanceLibrary, TransformInstance, TransformInstanceLibrary,
)
from ..solver import Dependency
@dataclass
class WorkflowStep:
    order: int
    dependency_map: InitVar[dict[Dependency, list[DataInstance]]]
    transform: TransformInstance
    transform_library: TransformInstanceLibrary
    uses: list[DataInstance] = field(default_factory=list)
    produces: list[list[DataInstance]] = field(default_factory=list)
    _raw_dependency_map: dict|None = None
    _raw_instances: dict[str, DataInstance]|None = None

    def __post_init__(self, dependency_map: dict[Dependency, list[DataInstance]]):
        self._dependency_map: dict[Dependency, list[DataInstance]] = {}
        self.dependency_map = dependency_map

    def _get_dependency_map(self) -> dict[Dependency, list[DataInstance]]:
        return self._dependency_map

    def _set_dependency_map(self, value: dict[Dependency, list[DataInstance]]):
        self._dependency_map = value
        if value:
            self.RefreshViews()

    @property
    def group_by_instances(self):
        return self.dependency_map.get(self.transform.group_by, [])

    def RefreshViews(self):
        self.uses = [
            inst
            for dep in self.transform.model.requires
            for inst in self.dependency_map.get(dep, [])
        ]
        self.produces = [
            [
                inst
                for dep in dep_group
                for inst in self.dependency_map.get(dep, [])
            ]
            for dep_group in self.transform.model.produces
        ]

    def Pack(self):
        all_instances: dict[str, DataInstance] = {}
        for lst in self.dependency_map.values():
            for inst in lst:
                all_instances[inst.instance_id] = inst
        return dict(
            order=self.order,
            schema="v2",
            instances={k:v.Pack() for k, v in all_instances.items()},
            dependency_map={k.key:[v.instance_id for v in lst] for k, lst in self.dependency_map.items()},
            transform=f"{self.transform_library.GetKey()}::{self.transform._path}",
        )

    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataInstanceLibrary]):
        lib_key, transform_path = raw["transform"].split("::")
        lib = libraries[lib_key]
        assert isinstance(lib, TransformInstanceLibrary)
        tr = lib.GetTransform(transform_path)
        assert tr is not None
        raw_instances: dict[str, DataInstance]|None = None
        uses: list[DataInstance] = []
        produces: list[list[DataInstance]] = []
        if "instances" in raw:
            raw_instances = {k:DataInstance.Unpack(v, libraries) for k, v in raw["instances"].items()}
        else:
            uses = [DataInstance.Unpack(inst, libraries) for inst in raw.get("uses", [])]
            produces = [[DataInstance.Unpack(inst, libraries) for inst in g] for g in raw.get("produces", [])]
        return cls(
            order=raw["order"],
            dependency_map={},
            uses=uses,
            produces=produces,
            _raw_dependency_map = raw["dependency_map"],
            _raw_instances=raw_instances,
            transform=tr,
            transform_library=lib,
        )
    
    def _resolve_dependency_map(self):
        assert self._raw_dependency_map is not None
        if self._raw_instances is not None:
            data = {}
            for inst in self._raw_instances.values():
                for k in {inst.instance_id, inst._key, inst.legacy_key}:
                    data[k] = inst
        else:
            raw_data = itertools.chain(self.uses, [d for g in self.produces for d in g])
            data = {}
            for inst in raw_data:
                for k in {inst.instance_id, inst._key, inst.legacy_key}:
                    data[k] = inst
        tr = self.transform.model
        deps = {d.key:d for d in itertools.chain(tr.requires, [d for g in tr.produces for d in g])}
        dep_map: dict[Dependency, list[DataInstance]] = {}
        for dep_key, ids in self._raw_dependency_map.items():
            if dep_key not in deps:
                continue
            missing = [v for v in ids if v not in data]
            if len(missing)>0:
                Log.Warn(f"missing [{len(missing)}] DataInstances for dependency [{dep_key}] while unpacking workflow step [{self.order}]")
            dep_map[deps[dep_key]] = [data[v] for v in ids if v in data]
        self.dependency_map = dep_map

WorkflowStep.dependency_map = property(  # type: ignore[assignment]
    WorkflowStep._get_dependency_map,
    WorkflowStep._set_dependency_map,
)

@dataclass
class WorkflowTarget:
    name: str
    instance: DataInstance
    producing_step: WorkflowStep

    def Pack(self):
        return dict(
            name=self.name,
            instance=self.instance.Pack(),
            producing_step=dict(order=self.producing_step.order, name=self.producing_step.transform.name),
        )

    @classmethod
    def Unpack(cls, raw: dict, libraries: dict[str, DataInstanceLibrary], given: dict[str, DataInstance], steps: dict[int, WorkflowStep]):
        inst = DataInstance.Unpack(raw["instance"], libraries)
        producing_step = steps[raw["producing_step"]["order"]]
        return cls(
            name=raw["name"],
            instance=inst,
            producing_step=producing_step,
        )
