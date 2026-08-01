"""A workflow before it is solved.

Metasmith had a serialization for a workflow *after* it was solved -- the task
bundle -- and none for before. Each veneer improvised one: the notebook by
calling `Agent.GenerateWorkflow` with libraries in hand, the web GUI by writing
a private `request.yml`. The two solve bodies behind those doors were the same
twenty lines and had already drifted, which is what this collapses.

`Spec` is that missing representation: the six things a solve needs and nothing
else. Notably *not* a workflow's name, when it was created, or what it was
forked from -- those belong to whoever is storing it, and a spec that carried
them would hand every shared copy the sender's workflow name to collide on.

It lives beside `TargetBuilder` rather than in `models/workflow/` because that
package is the post-solve world; a spec is the ask, not the answer. `Solve` is
where the ask becomes one.

Two entry points, because callers arrive with different things in hand:

* :meth:`Spec.Solve` -- from references. Paths to libraries, a sample type to
  split on. This is what a stored workflow and a template deserialize into.
* :meth:`Spec.SolveViews` -- from objects already sampled and viewed. This is
  the notebook's door, and `Agent.GenerateWorkflow` is now a call to it.

The first resolves down to the second, so the target model is built in exactly
one place.
"""

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
        # A template's inline input library (see Template.Save/`PackInline`).
        # Built fresh into a throwaway directory -- a solve never looks at
        # this library again once it has one, and nothing here is meant to
        # be committed, so a temp directory is exactly as permanent as it
        # needs to be.
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
    """What to build, stated before anything has worked out how.

    `input_library` and the two library lists take either a path or an already
    loaded library, so the same class serves a spec read off disk and one a
    notebook built in memory. :meth:`Pack` writes paths either way.
    """

    input_library: DataLibRef
    target_types: list[str | dict] = field(default_factory=list)
    transform_libraries: list[TransformLibRef] = field(default_factory=list)
    resource_libraries: list[DataLibRef] = field(default_factory=list)
    sample_type: str | None = None
    shared_input_paths: list[str] = field(default_factory=list)

    # The keys a spec owns on the wire and on disk. Everything else in a stored
    # workflow record -- its name, when it was made, what it was forked from --
    # belongs to the store, not to the ask.
    FIELDS = (
        "sample_type", "target_types", "transform_libraries",
        "resource_libraries", "shared_input_paths",
    )

    # -- serialization -----------------------------------------------------

    def Pack(self, relative_to: Path | str | None = None) -> dict:
        """The spec as plain data. Libraries render as their locations.

        `relative_to` renders every library reference relative to that root,
        which is what makes a spec portable: an absolute reference is one
        machine's `/home/someone/...` and arrives at a colleague naming nothing.
        A reference that does not live under the root is left absolute rather
        than silently rewritten -- what a store does about one is the store's
        own business (`Template.Save` reduces every reference to a name).
        """
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
            # A template's input library: small enough to embed as data
            # rather than reference as a directory. `PackInline` needs a
            # root to render its type references relative to, so a bare
            # `DataInstanceLibrary` here (never the case for a stored
            # workflow, which always names a real directory) only inlines
            # when one was given.
            if isinstance(ref, DataInstanceLibrary) and root is not None:
                return ref.PackInline(root)
            if isinstance(ref, dict):
                # a template stores no type map at all (`Template.Save`); an
                # empty one written back would be a key that says nothing
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
        """Read a spec out of a record that may carry more than a spec.

        `input_library` overrides what the record says, which is how a project
        store resolves its own relative directory name to a real path without
        the spec having to know the store exists.

        `root` is the other half of `Pack(relative_to=...)`: relative library
        references are resolved against it, so a spec written in one checkout
        loads in another.
        """
        base = Path(root).resolve() if root is not None else None

        def resolve(ref):
            if base is None or not isinstance(ref, (str, Path)): return ref
            p = Path(ref)
            return str(base / p) if not p.is_absolute() else str(ref)

        def resolve_input_lib(ref):
            # The inline form (a template's input library, see `PackInline`)
            # carries its own relative references -- just the type namespace
            # paths -- rather than being one itself.
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

    # -- solving -----------------------------------------------------------

    def Solve(self, max_iter: int = 256, max_refine: int = 256, seed: int = 42) -> WorkflowTask:
        """Resolve the references, split into samples, and solve.

        `sample_type` splits the input library into one run per item of that
        type. Left unset, the library is planned as it stands -- one sample
        holding everything in it -- which is the whole of what a plan needs;
        sampling is a way of branching it, not a precondition for having one.

        `shared_input_paths` names entries of the *input* library that every
        sample should see -- a reference database sitting beside the per-sample
        files. A sample mask is one index item's lineage, so anything outside it
        is invisible to the plan though still staged; and making the database an
        ancestor of the index instead would collapse every sample into one view.
        So it goes in alongside the resource libraries, which is where a shared
        thing belongs.
        """
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
        # Without a sample type the single view already holds everything, and
        # adding the same entries a second time offers the solver two of each.
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
        """Solve from libraries already in hand.

        The default search budget is 256 rather than the 1024 the notebook used
        to pass: that is what both shipped veneers were already getting, and the
        cost of the higher number is paid by every *failing* solve, which is the
        one a person is sitting in front of. Ask for more explicitly when a
        workflow needs it.
        """
        if not isinstance(targets, TargetBuilder):
            tb = TargetBuilder()
            tb.AddAll(targets)
            targets = tb
        assert len(targets) > 0, "[targets] can not be empty"

        def _get_endpoint(dtype_name: str):
            ns, _ = dtype_name.split("::")
            for trlib in transforms:
                if ns not in trlib.types: continue
                e = trlib.GetType(dtype_name)
                lpath = trlib.location
                loc = "..." + "/".join(lpath.parts[-3:]) if len(lpath.parts) > 3 else f"{lpath}"
                Log.Info(f"[{dtype_name}] resolved by [{loc}]")
                return e
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
        # Deduplicated by identity: a shared-input mask is a *view of the input
        # library*, so without this the library the samples came from is listed
        # twice and gets staged twice.
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
