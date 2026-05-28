"""Library compilation — thin wrapper over models.build_libraries decomposed steps."""
from __future__ import annotations

from pathlib import Path

from ..models.build_libraries import (
    LoadTypeLibraries,
    CompileUniqueLibrary,
    CompileTransformLibrary,
    Build,
)


def load_types(type_dirs: list[str]) -> dict:
    """Load every *.yml/*.yaml from each type dir; report counts per namespace."""
    types = LoadTypeLibraries([Path(p) for p in type_dirs])
    return {ns: len(lib.types) for ns, lib in types.items()}


def compile_uniques(unique_dirs: list[str], type_dirs: list[str]) -> dict:
    """Compile each unique-resource dir into a DataInstanceLibrary."""
    types = LoadTypeLibraries([Path(p) for p in type_dirs])
    results = [CompileUniqueLibrary(Path(d), types) for d in unique_dirs]
    return {"types": {ns: len(lib.types) for ns, lib in types.items()}, "uniques": results}


def compile_transforms(transform_dirs: list[str], type_dirs: list[str]) -> dict:
    """Compile each transform dir; attaches the loaded types and prunes the unused."""
    types = LoadTypeLibraries([Path(p) for p in type_dirs])
    results = [CompileTransformLibrary(Path(d), types) for d in transform_dirs]
    return {"types": {ns: len(lib.types) for ns, lib in types.items()}, "transforms": results}


def build_all(
    type_dirs: list[str],
    transform_dirs: list[str],
    unique_dirs: list[str] | None = None,
) -> dict:
    """Run the full pipeline: types → uniques → transforms."""
    return Build(
        data_type_dirs=[Path(p) for p in type_dirs],
        transform_dirs=[Path(p) for p in transform_dirs],
        unique_dirs=[Path(p) for p in (unique_dirs or [])],
    )
