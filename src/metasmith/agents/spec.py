from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from ..logging import Log
from ..models.libraries import (
    DataInstanceLibrary, DataInstanceLibraryView,
    TransformInstanceLibrary, TransformInstanceLibraryView,
)
from ..models.solver import Dependency, Transform
from ..models.workflow import Case, WorkflowPlan, WorkflowTask
from .targets import TargetBuilder, TargetSpec


DataLibRef = str | Path | dict | DataInstanceLibrary
TransformLibRef = str | Path | TransformInstanceLibrary | TransformInstanceLibraryView
LibView = DataInstanceLibraryView | DataInstanceLibrary


def _as_transform_lib(ref: TransformLibRef):
    if isinstance(ref, (TransformInstanceLibrary, TransformInstanceLibraryView)):
        return ref
    return TransformInstanceLibrary.Load(Path(ref).resolve())


def _location(ref) -> str:
    if isinstance(ref, dict):
        return "<inline template library>"
    return str(ref.location) if hasattr(ref, "location") else str(ref)


class _DataLibs:
    def __init__(self) -> None:
        self._loaded: dict = {}

    def __call__(self, ref: DataLibRef) -> DataInstanceLibrary:
        if isinstance(ref, DataInstanceLibrary):
            return ref
        key = id(ref) if isinstance(ref, dict) else str(Path(ref).resolve())
        if key not in self._loaded:
            if isinstance(ref, dict):
                location = Path(tempfile.mkdtemp(prefix="msm-template-"))
                self._loaded[key] = DataInstanceLibrary.FromInline(ref, location)
            else:
                self._loaded[key] = DataInstanceLibrary.Load(Path(key))
        return self._loaded[key]


def case_records(raw: dict) -> list[dict]:
    if "cases" in raw:
        return list(raw.get("cases") or [])
    return [{k: raw[k] for k in Spec.FIELDS if k in raw and k != "name"}]


@dataclass
class Spec:
    input_library: DataLibRef
    target_types: list[str | dict] | TargetBuilder = field(default_factory=list)
    sample_type: str | None = None
    input_paths: list[str] | None = None
    shared_input_paths: list[str] = field(default_factory=list)
    name: str = "case"

    FIELDS = ("name", "sample_type", "input_paths", "shared_input_paths", "target_types")


    def Pack(self) -> dict:
        out = {
            "name": self.name,
            "sample_type": self.sample_type,
            "target_types": list(self.target_types),
            "shared_input_paths": [str(p) for p in self.shared_input_paths],
        }
        if self.input_paths is not None:
            out["input_paths"] = [str(p) for p in self.input_paths]
        return out

    @classmethod
    def Unpack(cls, raw: dict, input_library: DataLibRef) -> "Spec":
        paths = raw.get("input_paths")
        return cls(
            input_library=input_library,
            name=raw.get("name") or "case",
            target_types=list(raw.get("target_types") or []),
            sample_type=raw.get("sample_type"),
            input_paths=None if paths is None else list(paths),
            shared_input_paths=list(raw.get("shared_input_paths") or []),
        )


    def _views(self, load: _DataLibs) -> tuple[list[DataInstanceLibraryView], list[DataInstanceLibraryView]]:
        lib = load(self.input_library)
        where = _location(self.input_library)
        shared = {Path(p) for p in self.shared_input_paths}
        missing = sorted(str(p) for p in shared - set(lib.manifest))
        assert not missing, f"case [{self.name}]: shared inputs not in [{where}]: {', '.join(missing)}"

        if self.sample_type:
            samples = list(lib.AsSamples(self.sample_type, among=self.input_paths))
            assert samples, f"case [{self.name}]: no samples of type [{self.sample_type}] found in [{where}]"
            return samples, [DataInstanceLibraryView(lib, mask=shared)] if shared else []

        if self.input_paths is None:
            return [DataInstanceLibraryView(lib)], []
        named = {Path(p) for p in self.input_paths} | shared
        missing = sorted(str(p) for p in named - set(lib.manifest))
        assert not missing, f"case [{self.name}]: inputs not in [{where}]: {', '.join(missing)}"
        mask, queue = set(named), list(named)
        while queue:
            for parent in lib.parents.get(queue.pop(), []):
                if parent.path not in mask:
                    mask.add(parent.path)
                    queue.append(parent.path)
        return [DataInstanceLibraryView(lib, mask=mask)], []


@dataclass
class CaseViews:
    name: str
    samples: list[LibView]
    resources: list[LibView]
    targets: TargetBuilder | list


def Plan(
    cases: Iterable[Spec],
    transform_libraries: Iterable[TransformLibRef],
    resource_libraries: Iterable[DataLibRef] = (),
    max_iter: int = 512, max_refine: int|None = None, seed: int = 42,
) -> WorkflowTask:
    cases = list(cases)
    assert cases, "a plan needs at least one case"
    names = [c.name for c in cases]
    duplicated = sorted({n for n in names if names.count(n) > 1})
    assert not duplicated, f"case names must be unique; name each case: {duplicated}"
    load = _DataLibs()
    requests = []
    for case in cases:
        samples, resources = case._views(load)
        requests.append(CaseViews(case.name, samples, resources, case.target_types))
    return plan_views(
        requests,
        resources=[load(x) for x in resource_libraries],
        transforms=[_as_transform_lib(x) for x in transform_libraries],
        max_iter=max_iter, max_refine=max_refine, seed=seed,
    )


def _target_model(targets: TargetBuilder | list, transforms: list) -> tuple[Transform, list[str]]:
    if not isinstance(targets, TargetBuilder):
        tb = TargetBuilder()
        tb.AddAll(targets)
        targets = tb
    assert len(targets) > 0, "[targets] can not be empty"

    def _get_endpoint(dtype_name: str):
        # By the type, not by its namespace: a library carries only the
        # types its own transforms declare, so several hold `annotation`
        # and only one of them holds `annotation::kofamscan_results`.
        ns, name = dtype_name.split("::")
        seen_namespace = False
        for trlib in transforms:
            tlib = trlib.types.get(ns)
            if tlib is None: continue
            seen_namespace = True
            if name not in tlib: continue
            lpath = trlib.location
            loc = "..." + "/".join(lpath.parts[-3:]) if len(lpath.parts) > 3 else f"{lpath}"
            Log.Info(f"[{dtype_name}] resolved by [{loc}]")
            return trlib.GetType(dtype_name)
        if seen_namespace:
            raise AssertionError(
                f"no transform library declares [{dtype_name}]; the"
                f" namespace [{ns}] is present but nothing in it produces"
                f" or consumes [{name}]"
            )
        raise AssertionError(f"no transforms had the namespace [{ns}]")

    model = Transform()
    deps: dict[TargetSpec, Dependency] = {}
    names: list[str] = []
    for spec in targets.resolve():
        deps[spec] = model.AddRequirement(
            example=_get_endpoint(spec.dtype_name),
            parents={deps[p] for p in spec.parents},
        )
        names.append(spec.dtype_name)
    return model, names


def _view(lib: LibView) -> DataInstanceLibraryView:
    return lib if isinstance(lib, DataInstanceLibraryView) else DataInstanceLibraryView(lib)


def _original(lib: LibView) -> DataInstanceLibrary:
    return lib._original if isinstance(lib, DataInstanceLibraryView) else lib


def plan_views(
    requests: list[CaseViews],
    resources: Iterable[LibView],
    transforms: list[TransformInstanceLibrary | TransformInstanceLibraryView],
    max_iter: int = 512, max_refine: int|None = None, seed: int = 42,
) -> WorkflowTask:
    resources = list(resources)
    common = [_view(lib) for lib in resources]
    cases: list[Case] = []
    for r in requests:
        target, target_names = _target_model(r.targets, transforms)
        own = common + [_view(lib) for lib in r.resources]
        split = Case.ByShape(
            [[_view(s)] + own for s in r.samples],
            target=target, target_names=target_names, name=r.name,
        )
        if len(requests) > 1 and len(split) > 1:
            for c in split:
                c.name = f"{r.name}/{c.name}"
        cases += split
    plan = WorkflowPlan.Generate(
        cases=cases, transforms=transforms,
        max_iter=max_iter, max_refine=max_refine, seed=seed,
    )

    data_libs: list[DataInstanceLibrary] = []
    seen: set[int] = set()

    def add(libs):
        for lib in map(_original, libs):
            if id(lib) in seen: continue
            seen.add(id(lib))
            data_libs.append(lib)

    add(s for r in requests for s in r.samples)
    sample_library_count = len(data_libs)
    add(resources)
    add(lib for r in requests for lib in r.resources)
    return WorkflowTask(
        ok=bool(plan.steps) and len(plan.dropped_targets) == 0,
        plan=plan,
        data_libraries=data_libs,
        transform_libraries=transforms,
        sample_library_count=sample_library_count,
    )
