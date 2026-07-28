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
        spec = TargetSpec(target_type, tuple(parents or ()))
        for existing in self._items:
            assert existing != spec, f'target [{target_type}] with identical parents already added'
        self._items.append(spec)
        return spec

    def resolve(self) -> list[TargetSpec]:
        # Insertion order is causal: a parent must have been Add'd before its child,
        # since the child receives the parent's TargetSpec handle.
        return list(self._items)

    def __len__(self) -> int:
        return len(self._items)

ResourceOverrides = dict[int|Literal["all"]|Literal["*"]|str|TransformInstance, Resources]
