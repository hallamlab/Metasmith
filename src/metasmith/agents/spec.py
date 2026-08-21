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
from ..models.workflow import WorkflowPlan, WorkflowTask
from .targets import TargetBuilder, TargetSpec


DataLibRef = str | Path | dict | DataInstanceLibrary
TransformLibRef = str | Path | TransformInstanceLibrary | TransformInstanceLibraryView


def _as_data_lib(ref: DataLibRef) -> DataInstanceLibrary:
    if isinstance(ref, DataInstanceLibrary):
        return ref
    if isinstance(ref, dict):
        location = Path(tempfile.mkdtemp(prefix="msm-template-"))
        return DataInstanceLibrary.FromInline(ref, location)
    return DataInstanceLibrary.Load(Path(ref).resolve())


def _as_transform_lib(ref: TransformLibRef):
    if isinstance(ref, (TransformInstanceLibrary, TransformInstanceLibraryView)):
        return ref
    return TransformInstanceLibrary.Load(Path(ref).resolve())


def _location(ref) -> str:
    if isinstance(ref, dict):
        return "<inline template library>"
    return str(ref.location) if hasattr(ref, "location") else str(ref)


@dataclass
class Spec:
    input_library: DataLibRef
    target_types: list[str | dict] = field(default_factory=list)
    transform_libraries: list[TransformLibRef] = field(default_factory=list)
    resource_libraries: list[DataLibRef] = field(default_factory=list)
    sample_type: str | None = None
    shared_input_paths: list[str] = field(default_factory=list)

    FIELDS = (
        "sample_type", "target_types", "transform_libraries",
        "resource_libraries", "shared_input_paths",
    )


    def Pack(self, relative_to: Path | str | None = None) -> dict:
        root = Path(relative_to).resolve() if relative_to is not None else None

        def loc(ref) -> str:
            s = _location(ref)
            if root is None: return s
            p = Path(s)
            if not p.is_absolute(): return s
            try:
                return str(p.resolve().relative_to(root))
            except ValueError:
                return s

        def input_lib() -> str | dict:
            ref = self.input_library
            if isinstance(ref, DataInstanceLibrary) and root is not None:
                return ref.PackInline(root)
            if isinstance(ref, dict):
                types = {ns: loc(p) for ns, p in ref.get("types", {}).items()}
                return ref | {"types": types} if types else {
                    k: v for k, v in ref.items() if k != "types"
                }
            return loc(ref)

        return {
            "sample_type": self.sample_type,
            "target_types": list(self.target_types),
            "transform_libraries": [loc(x) for x in self.transform_libraries],
            "resource_libraries": [loc(x) for x in self.resource_libraries],
            "shared_input_paths": [str(p) for p in self.shared_input_paths],
            "input_library": input_lib(),
        }

    @classmethod
    def Unpack(
        cls, raw: dict, *,
        input_library: DataLibRef | None = None,
        root: Path | str | None = None,
    ) -> "Spec":
        base = Path(root).resolve() if root is not None else None

        def resolve(ref):
            if base is None or not isinstance(ref, (str, Path)): return ref
            p = Path(ref)
            return str(base / p) if not p.is_absolute() else str(ref)

        def resolve_input_lib(ref):
            if isinstance(ref, dict):
                return ref | {"types": {ns: resolve(p) for ns, p in ref.get("types", {}).items()}}
            return resolve(ref)

        lib = input_library if input_library is not None else raw.get("input_library")
        assert lib, "a spec needs an input library"
        return cls(
            input_library=lib if input_library is not None else resolve_input_lib(lib),
            target_types=list(raw.get("target_types") or []),
            transform_libraries=[resolve(x) for x in (raw.get("transform_libraries") or [])],
            resource_libraries=[resolve(x) for x in (raw.get("resource_libraries") or [])],
            sample_type=raw.get("sample_type"),
            shared_input_paths=list(raw.get("shared_input_paths") or []),
        )


    def Solve(self, max_iter: int = 256, max_refine: int = 256, seed: int = 42) -> WorkflowTask:
        data_lib = _as_data_lib(self.input_library)
        tr_libs = [_as_transform_lib(x) for x in self.transform_libraries]
        res_libs = [_as_data_lib(x) for x in self.resource_libraries]

        if self.sample_type:
            samples = list(data_lib.AsSamples(self.sample_type))
            assert samples, (
                f"no samples of type [{self.sample_type}] found in "
                f"[{_location(self.input_library)}]"
            )
        else:
            samples = [DataInstanceLibraryView(data_lib)]

        resources: list = list(res_libs)
        if self.shared_input_paths and self.sample_type:
            shared = {Path(p) for p in self.shared_input_paths}
            missing = sorted(str(p) for p in shared - set(data_lib.manifest))
            assert not missing, (
                f"shared inputs not in [{_location(self.input_library)}]: {', '.join(missing)}"
            )
            resources.append(DataInstanceLibraryView(data_lib, mask=shared))

        return self.SolveViews(
            samples=samples,
            resources=resources,
            transforms=tr_libs,
            targets=self.target_types,
            max_iter=max_iter, max_refine=max_refine, seed=seed,
        )

    @staticmethod
    def SolveViews(
        samples: Iterable[DataInstanceLibraryView | DataInstanceLibrary],
        resources: Iterable[DataInstanceLibraryView | DataInstanceLibrary],
        transforms: list[TransformInstanceLibrary | TransformInstanceLibraryView],
        targets: TargetBuilder | list,
        max_iter: int = 256, max_refine: int = 256, seed: int = 42,
    ) -> WorkflowTask:
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

        target_model = Transform()
        _spec2dep: dict[TargetSpec, Dependency] = {}
        target_names: list[str] = []
        for spec in targets.resolve():
            e = _get_endpoint(spec.dtype_name)
            d = target_model.AddRequirement(
                example=e, parents={_spec2dep[p] for p in spec.parents}
            )
            _spec2dep[spec] = d
            target_names.append(spec.dtype_name)

        res_views = [
            lib if isinstance(lib, DataInstanceLibraryView) else DataInstanceLibraryView(lib)
            for lib in resources
        ]
        _samples = [
            s if isinstance(s, DataInstanceLibraryView) else DataInstanceLibraryView(s)
            for s in samples
        ]
        plan = WorkflowPlan.Generate(
            given=[[sample] + res_views for sample in _samples],
            transforms=transforms,
            target_names=target_names,
            target_model=target_model,
            max_iter=max_iter, max_refine=max_refine, seed=seed,
        )
        data_libs: list[DataInstanceLibrary] = []
        seen: set[int] = set()
        for lib in [v._original for v in _samples] + [
            lib if isinstance(lib, DataInstanceLibrary) else lib._original
            for lib in resources
        ]:
            if id(lib) in seen: continue
            seen.add(id(lib))
            data_libs.append(lib)
        return WorkflowTask(
            ok=bool(plan.steps) and len(plan.dropped_targets) == 0,
            plan=plan,
            data_libraries=data_libs,
            transform_libraries=transforms,
        )
