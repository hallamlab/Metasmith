"""What a caller asks for, stated before any plan exists to satisfy it.

`TargetBuilder.Add` returns an opaque `TargetSpec` handle, and passing handles
back in `parents=` is what makes two targets of the same type distinct requests
rather than the duplicate `Add` rejects. Same type *and* same parents still
raises -- that is genuinely one request asked for twice.

`ResourceOverrides` lives here rather than with the GPU planner because it is
the same vocabulary: what the caller asked for, keyed by step index, name,
transform, or the `all`/`*` wildcards.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal

from ..models.libraries import Resources, TransformInstance

@dataclass(frozen=True)
class TargetSpec:
    dtype_name: str
    parents: tuple["TargetSpec", ...] = ()

class TargetBuilder:
    def __init__(self) -> None:
        self._items: list[TargetSpec] = []

    def Add(self, target_type: str, parents: Iterable[TargetSpec]|None=None) -> TargetSpec:
        assert "::" in target_type, f'expected @type to in the form of "namespace::type_name" but got [{target_type}]'
        parents = tuple(parents or ())
        # A type *name* passed where a handle belongs is the one way to defeat
        # the duplicate check without noticing: it builds a TargetSpec that
        # equals nothing already added, so the request sails through and the
        # caller believes it asserted something it did not. Checked here rather
        # than left to the reader, because the failure is silent and the symptom
        # -- a plan with a target missing -- surfaces far from the cause.
        for p in parents:
            assert isinstance(p, TargetSpec), (
                f'target [{target_type}] was given [{p!r}] as a parent, which is'
                ' a type name rather than a handle. Parents are the TargetSpec'
                ' values Add returns; pass the handle you got back from the'
                " parent's own Add."
            )
        spec = TargetSpec(target_type, parents)
        for existing in self._items:
            assert existing != spec, f'target [{target_type}] with identical parents already added'
        self._items.append(spec)
        return spec

    def AddAll(self, target_types: Iterable[str|dict]) -> list[TargetSpec]:
        """Declare a whole list at once, wiring the lineage links between them.

        The declarative form a stored spec is written in: an entry is either a
        bare type name or `{"type": ..., "parents": [i, ...]}`, where each `i`
        indexes an *earlier* entry in this same list. Forward references are
        refused rather than resolved in a second pass, because a target that
        names one has been written in an order its author did not intend.

        That positional form is why this is here and not left to the caller:
        `Add` speaks in handles, which serialize to nothing.
        """
        specs: list[TargetSpec] = []
        for i, target in enumerate(target_types):
            if isinstance(target, str):
                name, parents = target, ()
            else:
                name = target.get("type")
                assert name, f"target #{i + 1} has no type"
                parents = tuple(target.get("parents") or ())
            handles = []
            for p in parents:
                # Positions are stored 0-based and said 1-based, here as
                # everywhere else a target is named to a person -- one sentence
                # carrying both counts reads as an off-by-one in whichever half
                # you trust less.
                assert isinstance(p, int) and 0 <= p < len(specs), (
                    f"target #{i + 1} [{name}] names parent #{p + 1 if isinstance(p, int) else p}, "
                    f"which is not one of the {len(specs)} target(s) declared before it"
                )
                handles.append(specs[p])
            specs.append(self.Add(name, parents=handles or None))
        return specs

    def resolve(self) -> list[TargetSpec]:
        # Insertion order is causal: a parent must have been Add'd before its child,
        # since the child receives the parent's TargetSpec handle.
        return list(self._items)

    def __len__(self) -> int:
        return len(self._items)

ResourceOverrides = dict[int|Literal["all"]|Literal["*"]|str|TransformInstance, Resources]
