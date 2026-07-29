"""Shared helpers for the ops package — loaders, type resolution, packing.

CLI → ops mapping invariants
============================

By default a ``metasmith <group> <verb>`` command resolves to exactly one
``metasmith.ops.<group>.<verb>``. Two groups are spelled differently on
each side (``transform``/``type`` on the CLI, ``transforms``/``types`` in
ops) and ``task`` reads from ``ops.workflow``; those are naming, not
exceptions. The real exceptions are three, recorded here so a grep for
"where does this command actually do its work" lands in one place:

* ``metasmith e2e`` (``coms/cli/e2e.py``) — no ops backing, by design. It
  is a test-harness signalling primitive: each verb writes ``CONTROL.json``
  into a sandbox cwd so the agentic outer loop in
  ``tests/e2e/agentic/harness/control.py`` can react. It operates on no
  metasmith state.

* ``metasmith run`` (``coms/cli/run.py``) — calls
  ``models.direct_run.RunTransform`` directly. This is the deliberate
  "skip the planner, skip Nextflow, run one transform" path used by
  transform authors; ``direct_run`` is its canonical single entry and an
  ``ops/run.py`` wrapper would only add a layer.

* ``get`` / ``lab`` / ``gui`` / ``api`` (``coms/cli/legacy.py``) — four
  commands carried over from the original ``cli.py``. They print logs or
  hand off to a subprocess rather than returning structured data, which is
  what ops/ exists to produce.

Any new command added to ``coms/cli/`` SHOULD route through ``ops/`` unless
it falls into one of those categories.
"""
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
    """Merge types from standalone type libs AND from transform libs.

    Returns {namespace: {name: Endpoint}}. Transform libs override standalone libs
    on conflict (last writer wins), matching the MCP server's prior behavior.
    """
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
    """Resolve 'namespace::name' against the merged type universe."""
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
    """Find the canonical type name for a Dependency within a transform library."""
    for ns, dtlib in lib.types.items():
        if ns == "transforms":
            continue
        for tname, ep in dtlib.types.items():
            if dep.IsA(ep) and ep.IsA(dep):
                return {"type": f"{ns}::{tname}", "properties": dep.Pack()["properties"]}
    return {"properties": dep.Pack()["properties"]}
