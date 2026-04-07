"""Metasmith MCP Server — exposes types, transforms, data libraries, and workflow planning."""
from __future__ import annotations

import argparse
import functools
import os
import json
import traceback
from dataclasses import dataclass, field
from pathlib import Path


from mcp.server.fastmcp import FastMCP

from ..models.libraries import (
    DataTypeLibrary,
    DataInstanceLibrary,
    TransformInstanceLibrary,
    Endpoint,
)
from ..models.solver import Transform, Dependency
from ..models.solver import Solution
from ..models.workflow import WorkflowPlan
from ..agents import TargetBuilder
from ..logging import Log

# ---------------------------------------------------------------------------
# Server state
# ---------------------------------------------------------------------------

@dataclass
class ServerState:
    type_paths: list[Path] = field(default_factory=list)
    data_paths: list[Path] = field(default_factory=list)
    transform_paths: list[Path] = field(default_factory=list)

    _type_libs: dict[str, DataTypeLibrary] = field(default_factory=dict)
    _data_libs: dict[str, DataInstanceLibrary] = field(default_factory=dict)
    _transform_libs: dict[str, TransformInstanceLibrary] = field(default_factory=dict)

    # -- lazy loaders --------------------------------------------------------

    def _ensure_type_libs(self):
        if self._type_libs:
            return
        for p in self.type_paths:
            ns = p.stem
            self._type_libs[ns] = DataTypeLibrary.Load(p)

    def _ensure_data_libs(self):
        if self._data_libs:
            return
        for p in self.data_paths:
            lib = DataInstanceLibrary.Load(p)
            self._data_libs[str(p)] = lib

    def _ensure_transform_libs(self):
        if self._transform_libs:
            return
        for p in self.transform_paths:
            lib = TransformInstanceLibrary.Load(p)
            self._transform_libs[str(p)] = lib

    @property
    def type_libs(self) -> dict[str, DataTypeLibrary]:
        self._ensure_type_libs()
        return self._type_libs

    @property
    def data_libs(self) -> dict[str, DataInstanceLibrary]:
        self._ensure_data_libs()
        return self._data_libs

    @property
    def transform_libs(self) -> dict[str, TransformInstanceLibrary]:
        self._ensure_transform_libs()
        return self._transform_libs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(fn):
    """Decorator that catches exceptions and returns structured error dicts."""
    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        try:
            return await fn(*args, **kwargs)
        except Exception as exc:
            return {"error": str(exc), "traceback": traceback.format_exc()}
    return wrapper


def _collect_all_types(state: ServerState) -> dict[str, dict[str, Endpoint]]:
    """Merge types from type libs AND transform libs into {namespace: {name: Endpoint}}."""
    result: dict[str, dict[str, Endpoint]] = {}
    for ns, lib in state.type_libs.items():
        result[ns] = dict(lib.types)
    for _path, tlib in state.transform_libs.items():
        for ns, dtlib in tlib.types.items():
            if ns == "transforms":
                continue
            if ns not in result:
                result[ns] = {}
            result[ns].update(dtlib.types)
    return result


def _endpoint_to_dict(name: str, namespace: str, ep: Endpoint) -> dict:
    return {
        "namespace": namespace,
        "name": name,
        "full_name": f"{namespace}::{name}",
        "properties": ep.Pack()["properties"],
    }


def _resolve_type(state: ServerState, type_name: str) -> tuple[str, str, Endpoint]:
    """Resolve 'namespace::name' to (namespace, name, Endpoint)."""
    assert "::" in type_name, f"expected format 'namespace::name', got [{type_name}]"
    ns, name = type_name.split("::", 1)
    all_types = _collect_all_types(state)
    assert ns in all_types, f"namespace [{ns}] not found"
    assert name in all_types[ns], f"type [{name}] not found in [{ns}]"
    return ns, name, all_types[ns][name]


# ---------------------------------------------------------------------------
# Create MCP server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "Metasmith",
    instructions="Explore Metasmith types, transforms, data libraries, and plan workflows",
)
STATE: ServerState = ServerState()


# ===== Data Type Tools =====================================================

@mcp.tool()
@_safe
async def list_types(namespace: str | None = None) -> list[dict]:
    """List all data types, optionally filtered by namespace."""
    all_types = _collect_all_types(STATE)
    results = []
    for ns, types in all_types.items():
        if namespace and ns != namespace:
            continue
        for name, ep in types.items():
            results.append(_endpoint_to_dict(name, ns, ep))
    return results


@mcp.tool()
@_safe
async def get_type(type_name: str) -> dict:
    """Get details of a specific data type (e.g. 'ncbi::assembly_accession')."""
    ns, name, ep = _resolve_type(STATE, type_name)
    packed = ep.Pack(parents=True)
    return {
        "name": name,
        "namespace": ns,
        "full_name": f"{ns}::{name}",
        "properties": packed["properties"],
        "parents": packed.get("parents", []),
    }


@mcp.tool()
@_safe
async def check_type_compatibility(source_type: str, target_type: str) -> dict:
    """Check if source_type is compatible with (can satisfy) target_type via structural subtyping."""
    _, _, src_ep = _resolve_type(STATE, source_type)
    _, _, tgt_ep = _resolve_type(STATE, target_type)
    compatible = src_ep.IsA(tgt_ep)
    if compatible:
        reason = f"{source_type} has all properties of {target_type}"
    else:
        missing = tgt_ep.properties - src_ep.properties
        reason = f"{source_type} is missing properties: {missing}"
    return {"compatible": compatible, "reason": reason}


# ===== Library Tools =======================================================

@mcp.tool()
@_safe
async def list_data_libraries() -> list[dict]:
    """List all loaded data instance libraries."""
    results = []
    for path, lib in STATE.data_libs.items():
        results.append({
            "path": path,
            "item_count": len(lib.manifest),
        })
    return results


@mcp.tool()
@_safe
async def inspect_data_library(library_path: str) -> dict:
    """Inspect a data instance library — shows items, type namespaces, and schema."""
    lib = STATE.data_libs[library_path]
    items = []
    for p, dtype_name, _ep in lib.Iterate():
        items.append({"path": str(p), "type_name": dtype_name})
    return {
        "path": library_path,
        "schema": lib.schema,
        "type_namespaces": list(lib.types.keys()),
        "item_count": len(lib.manifest),
        "items": items,
    }


@mcp.tool()
@_safe
async def list_data_items(library_path: str, type_filter: str | None = None) -> list[dict]:
    """List items in a data library, optionally filtered by type name."""
    lib = STATE.data_libs[library_path]
    results = []
    for p, dtype_name, _ep in lib.Iterate():
        if type_filter and dtype_name != type_filter:
            continue
        results.append({"path": str(p), "type_name": dtype_name})
    return results


@mcp.tool()
@_safe
async def show_item_lineage(library_path: str, item_path: str) -> dict:
    """Show item details and its parent lineage."""
    lib = STATE.data_libs[library_path]
    p = Path(item_path)
    inst = lib.Get(p)
    parents = []
    for pm in lib.parents.get(p, []):
        parents.append({
            "path": str(pm.path),
            "type_name": pm.name,
        })
    return {
        "path": str(inst.path),
        "type_name": inst.dtype_name,
        "properties": inst.dtype.Pack()["properties"],
        "parents": parents,
    }


# ===== Transform Tools =====================================================

@mcp.tool()
@_safe
async def list_transform_libraries() -> list[dict]:
    """List all loaded transform libraries."""
    results = []
    for path, lib in STATE.transform_libs.items():
        namespaces = [ns for ns in lib.types.keys() if ns != "transforms"]
        count = sum(1 for _ in lib.Iterate())
        results.append({
            "path": path,
            "transform_count": count,
            "type_namespaces": namespaces,
        })
    return results


@mcp.tool()
@_safe
async def list_transforms(library_path: str | None = None) -> list[dict]:
    """List transforms, optionally scoped to a specific library."""
    results = []
    libs = STATE.transform_libs
    if library_path:
        libs = {library_path: libs[library_path]}
    for lib_path, lib in libs.items():
        for tr_path, tr in lib.IterateTransforms():
            results.append({
                "name": tr.name,
                "path": str(tr_path),
                "library": lib_path,
            })
    return results


@mcp.tool()
@_safe
async def show_transform_contract(library_path: str, transform_path: str) -> dict:
    """Show a transform's contract — inputs, outputs, group_by, and resources."""
    lib = STATE.transform_libs[library_path]
    tr = lib.GetTransform(transform_path)

    def _dep_info(dep: Dependency) -> dict:
        # Find the type name for this dependency
        for ns, dtlib in lib.types.items():
            if ns == "transforms":
                continue
            for tname, ep in dtlib.types.items():
                if dep.IsA(ep) and ep.IsA(dep):
                    return {"type": f"{ns}::{tname}", "properties": dep.Pack()["properties"]}
        return {"properties": dep.Pack()["properties"]}

    inputs = [_dep_info(d) for d in tr.model.requires]
    outputs = []
    for group in tr.model.produces:
        outputs.append([_dep_info(d) for d in group])
    group_by = _dep_info(tr.group_by)
    resources = None
    if tr.resources:
        resources = {
            "cpus": tr.resources.cpus,
            "memory": str(tr.resources.memory) if tr.resources.memory else None,
            "duration": str(tr.resources.duration) if tr.resources.duration else None,
        }
    return {
        "name": tr.name,
        "path": str(tr._path),
        "inputs": inputs,
        "outputs": outputs,
        "group_by": group_by,
        "resources": resources,
        "labels": tr.labels,
    }


# ===== Workflow Tools ======================================================

@mcp.tool()
@_safe
async def plan_workflow(
    data_library: str,
    sample_type: str,
    target_types: list[str],
    transform_libraries: list[str],
    resource_libraries: list[str] | None = None,
) -> dict:
    """Plan a workflow: find a chain of transforms from sample_type to target_types.

    Args:
        data_library: path to the input data library (.xgdb)
        sample_type: the type name to split samples by (e.g. 'ncbi::assembly_accession')
        target_types: list of target type names to produce
        transform_libraries: list of transform library paths to use
        resource_libraries: optional list of resource library paths (containers, etc.)
    """
    data_lib = STATE.data_libs[data_library]
    samples = list(data_lib.AsSamples(sample_type))
    assert len(samples) > 0, f"no samples of type [{sample_type}] found in [{data_library}]"

    tr_libs = [STATE.transform_libs[p] for p in transform_libraries]
    res_libs = []
    if resource_libraries:
        for rp in resource_libraries:
            if rp in STATE.data_libs:
                res_libs.append(STATE.data_libs[rp])
            else:
                res_libs.append(DataInstanceLibrary.Load(rp))

    targets = TargetBuilder()
    for t in target_types:
        parents = set()
        # auto-link to previously added targets that share namespace
        targets.Add(t, parents)

    # Build target model (same pattern as Agent.GenerateWorkflow)
    def _get_endpoint(dtype_name: str):
        ns, _ = dtype_name.split("::")
        for trlib in tr_libs:
            if ns not in trlib.types:
                continue
            return trlib.GetType(dtype_name)
        assert False, f"no transforms had the namespace [{ns}]"

    target_model = Transform()
    _dtname2dep: dict[str, Dependency] = {}
    target_names: dict[Endpoint, str] = {}
    for dtype_name, parents in targets.resolve():
        e = _get_endpoint(dtype_name)
        assert e not in target_names, f"[{dtype_name}] is a duplicate of [{target_names[e]}]"
        d = target_model.AddRequirement(example=e, parents={_dtname2dep[p] for p in parents})
        _dtname2dep[dtype_name] = d
        target_names[e] = dtype_name

    from ..models.libraries import DataInstanceLibraryView
    res_views = [DataInstanceLibraryView(lib) for lib in res_libs]
    gen_result = WorkflowPlan.Generate(
        given=[
            [sample] + res_views
            for sample in samples
        ],
        transforms=tr_libs,
        target_names=target_names,
        target_model=target_model,
    )

    if isinstance(gen_result, Solution):
        return {
            "success": False,
            "message": "solver could not find a complete plan",
            "complete": gen_result.complete,
            "iterations": gen_result._iterations,
        }
    else:
        plan = gen_result
        return {
            "success": True,
            "steps": [step.Pack() for step in plan.steps],
            "targets": [t.Pack() for t in plan.targets],
            "step_count": len(plan.steps),
        }


# ===== Workflow Status Tools ===============================================

@mcp.tool()
@_safe
async def check_workflow(task_key: str, run: int | None = None) -> dict:
    """Check the status and logs of a workflow run (same-machine only).

    Args:
        task_key: the workflow task key
        run: optional 1-indexed run number (default: latest)
    """
    from ..agents import CheckWorkflow as _CheckWorkflow
    return _CheckWorkflow(task_key, run, quiet=True)


# ===== MCP Resources ======================================================

@mcp.resource("metasmith://types")
@_safe
async def resource_types() -> str:
    """All type namespaces."""
    all_types = _collect_all_types(STATE)
    namespaces = {ns: len(types) for ns, types in all_types.items()}
    return json.dumps(namespaces, indent=2)


@mcp.resource("metasmith://types/{namespace}")
@_safe
async def resource_types_namespace(namespace: str) -> str:
    """Types in a specific namespace."""
    all_types = _collect_all_types(STATE)
    assert namespace in all_types, f"namespace [{namespace}] not found"
    types = all_types[namespace]
    return json.dumps(
        [_endpoint_to_dict(name, namespace, ep) for name, ep in types.items()],
        indent=2,
    )


@mcp.resource("metasmith://types/{namespace}/{name}")
@_safe
async def resource_type_detail(namespace: str, name: str) -> str:
    """Full details of a type."""
    all_types = _collect_all_types(STATE)
    assert namespace in all_types, f"namespace [{namespace}] not found"
    assert name in all_types[namespace], f"type [{name}] not found in [{namespace}]"
    ep = all_types[namespace][name]
    packed = ep.Pack(parents=True)
    return json.dumps({
        "name": name,
        "namespace": namespace,
        "full_name": f"{namespace}::{name}",
        "properties": packed["properties"],
        "parents": packed.get("parents", []),
    }, indent=2)


@mcp.resource("metasmith://data/{library_name}")
@_safe
async def resource_data_library(library_name: str) -> str:
    """Items in a data library."""
    lib = STATE.data_libs[library_name]
    items = [{"path": str(p), "type_name": dtype_name} for p, dtype_name, _ep in lib.Iterate()]
    return json.dumps(items, indent=2)


@mcp.resource("metasmith://transforms/{library_name}")
@_safe
async def resource_transform_library(library_name: str) -> str:
    """Transforms in a library."""
    lib = STATE.transform_libs[library_name]
    transforms = [{"name": tr.name, "path": str(p)} for p, tr in lib.IterateTransforms()]
    return json.dumps(transforms, indent=2)


@mcp.resource("metasmith://transforms/{library_name}/{transform}")
@_safe
async def resource_transform_detail(library_name: str, transform: str) -> str:
    """Contract details of a transform."""
    result = await show_transform_contract(library_name, transform)
    return json.dumps(result, indent=2)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        prog="metasmith-mcp",
        description="Metasmith MCP Server — expose types, transforms, and workflow planning via MCP",
    )
    parser.add_argument(
        "--transforms", nargs="+", default=[], metavar="PATH",
        help="Paths to transform instance libraries",
    )
    parser.add_argument(
        "--data", nargs="+", default=[], metavar="PATH",
        help="Paths to data instance libraries (.xgdb)",
    )
    parser.add_argument(
        "--types", nargs="+", default=[], metavar="PATH",
        help="Paths to data type library YAML files",
    )
    parser.add_argument(
        "--transport", choices=["stdio", "sse"], default="stdio",
        help="MCP transport (default: stdio)",
    )
    return parser.parse_args()


def _paths_from_env(var: str) -> list[Path]:
    val = os.environ.get(var, "")
    if not val:
        return []
    return [Path(p) for p in val.split(":") if p]


def main():
    args = _parse_args()

    # CLI args + env vars
    STATE.transform_paths = [Path(p) for p in args.transforms] + _paths_from_env("METASMITH_TRANSFORM_LIBS")
    STATE.data_paths = [Path(p) for p in args.data] + _paths_from_env("METASMITH_DATA_LIBS")
    STATE.type_paths = [Path(p) for p in args.types] + _paths_from_env("METASMITH_TYPE_LIBS")

    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()
