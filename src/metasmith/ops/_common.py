from __future__ import annotations

from pathlib import Path

from ..models.libraries import (
    DataInstanceLibrary,
    DataTypeLibrary,
    TransformInstanceLibrary,
    Endpoint,
)
from ..models.solver import Dependency


def load_type_lib(path: str | Path) -> DataTypeLibrary:
    return DataTypeLibrary.Load(Path(path))


def load_data_lib(path: str | Path) -> DataInstanceLibrary:
    return DataInstanceLibrary.Load(Path(path).resolve())


def load_transform_lib(path: str | Path) -> TransformInstanceLibrary:
    return TransformInstanceLibrary.Load(Path(path).resolve())


def collect_types(
    type_paths: list[str | Path] | None = None,
    transform_paths: list[str | Path] | None = None,
) -> dict[str, dict[str, Endpoint]]:
    result: dict[str, dict[str, Endpoint]] = {}
    for p in type_paths or []:
        lib = load_type_lib(p)
        result[Path(p).stem] = dict(lib.types)
    for p in transform_paths or []:
        tlib = load_transform_lib(p)
        for ns, dtlib in tlib.types.items():
            if ns == "transforms":
                continue
            result.setdefault(ns, {}).update(dtlib.types)
    return result


def resolve_type(
    type_name: str,
    type_paths: list[str | Path] | None = None,
    transform_paths: list[str | Path] | None = None,
) -> tuple[str, str, Endpoint]:
    assert "::" in type_name, f"expected format 'namespace::name', got [{type_name}]"
    ns, name = type_name.split("::", 1)
    all_types = collect_types(type_paths, transform_paths)
    assert ns in all_types, f"namespace [{ns}] not found"
    assert name in all_types[ns], f"type [{name}] not found in [{ns}]"
    return ns, name, all_types[ns][name]


def endpoint_to_dict(name: str, namespace: str, ep: Endpoint) -> dict:
    return {
        "namespace": namespace,
        "name": name,
        "full_name": f"{namespace}::{name}",
        "properties": ep.Pack()["properties"],
    }


def dep_info(dep: Dependency, lib: TransformInstanceLibrary) -> dict:
    for ns, dtlib in lib.types.items():
        if ns == "transforms":
            continue
        for tname, ep in dtlib.types.items():
            if dep.IsA(ep) and ep.IsA(dep):
                return {"type": f"{ns}::{tname}", "properties": dep.Pack()["properties"]}
    return {"properties": dep.Pack()["properties"]}
