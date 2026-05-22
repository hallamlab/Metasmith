"""Metasmith MCP Server — exposes types, transforms, data libraries, and workflow planning."""
from __future__ import annotations

import argparse
import asyncio
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
from ..models.workflow import WorkflowPlan, WorkflowTask
from ..models.remote import Source
from ..agents import Agent, TargetBuilder, TargetSpec
from ..models.build_libraries import Build
from ..models.remote import Logistics, SourceType
from ..logging import Log
from ..constants import VERSION as METASMITH_VERSION

# ---------------------------------------------------------------------------
# Server state
# ---------------------------------------------------------------------------

@dataclass
class ServerState:
    type_paths: list[Path] = field(default_factory=list)
    data_paths: list[Path] = field(default_factory=list)
    transform_paths: list[Path] = field(default_factory=list)
    agent_paths: list[Path] = field(default_factory=list)
    workspace: Path | None = None

    _type_libs: dict[str, DataTypeLibrary] = field(default_factory=dict)
    _data_libs: dict[str, DataInstanceLibrary] = field(default_factory=dict)
    _transform_libs: dict[str, TransformInstanceLibrary] = field(default_factory=dict)
    _agents: dict[str, Agent] = field(default_factory=dict)
    _tasks: dict[str, Path] = field(default_factory=dict)

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

    def _ensure_agents(self):
        if self._agents:
            return
        for p in self.agent_paths:
            self._agents[p.stem] = Agent.Load(p)

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

    @property
    def agents(self) -> dict[str, Agent]:
        self._ensure_agents()
        return self._agents

    # -- task caching -------------------------------------------------------

    def save_task(self, task: WorkflowTask) -> str:
        assert self.workspace is not None, "workspace not configured"
        key = task.GetKey()
        dest = self.workspace / key
        dest.mkdir(parents=True, exist_ok=True)
        task.SaveAs(Source.FromLocal(dest))
        self._tasks[key] = dest
        return key

    def load_task(self, task_key: str) -> WorkflowTask:
        if task_key not in self._tasks:
            # try scanning workspace
            if self.workspace and (self.workspace / task_key).exists():
                self._tasks[task_key] = self.workspace / task_key
        assert task_key in self._tasks, f"task [{task_key}] not found"
        return WorkflowTask.Load(self._tasks[task_key])


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


# ===== Server Lifecycle / Introspection ====================================

@mcp.tool()
@_safe
async def server_status() -> dict:
    """Show loaded library paths, workspace, version, and task counts."""
    return {
        "version": METASMITH_VERSION,
        "workspace": str(STATE.workspace) if STATE.workspace else None,
        "type_paths": [str(p) for p in STATE.type_paths],
        "data_paths": [str(p) for p in STATE.data_paths],
        "transform_paths": [str(p) for p in STATE.transform_paths],
        "agent_paths": [str(p) for p in STATE.agent_paths],
        "loaded": {
            "type_libs": len(STATE._type_libs),
            "data_libs": len(STATE._data_libs),
            "transform_libs": len(STATE._transform_libs),
            "agents": len(STATE._agents),
        },
        "cached_tasks": len(STATE._tasks),
    }


def _register_path(path: str, into: list[Path]) -> Path:
    p = Path(path).resolve()
    assert p.exists(), f"path [{p}] does not exist"
    if p not in into:
        into.append(p)
    return p


@mcp.tool()
@_safe
async def register_type_library(path: str) -> dict:
    """Register a type library YAML at runtime. Invalidates type cache."""
    p = _register_path(path, STATE.type_paths)
    STATE._type_libs = {}
    return {"registered": str(p), "type_paths": [str(x) for x in STATE.type_paths]}


@mcp.tool()
@_safe
async def register_data_library(path: str) -> dict:
    """Register a data instance library (.xgdb) at runtime. Invalidates data cache."""
    p = _register_path(path, STATE.data_paths)
    STATE._data_libs = {}
    return {"registered": str(p), "data_paths": [str(x) for x in STATE.data_paths]}


@mcp.tool()
@_safe
async def register_transform_library(path: str) -> dict:
    """Register a transform library at runtime. Invalidates transform cache."""
    p = _register_path(path, STATE.transform_paths)
    STATE._transform_libs = {}
    return {"registered": str(p), "transform_paths": [str(x) for x in STATE.transform_paths]}


@mcp.tool()
@_safe
async def register_agent(path: str) -> dict:
    """Register an agent YAML at runtime. Invalidates agent cache."""
    p = _register_path(path, STATE.agent_paths)
    STATE._agents = {}
    return {"registered": str(p), "agent_paths": [str(x) for x in STATE.agent_paths]}


@mcp.tool()
@_safe
async def reload_libraries(kinds: list[str] | None = None) -> dict:
    """Drop in-memory caches and re-read from disk on next access.

    Args:
        kinds: subset of ["types", "data", "transforms", "agents"]; None = all.
    """
    kinds = kinds or ["types", "data", "transforms", "agents"]
    if "types" in kinds:
        STATE._type_libs = {}
    if "data" in kinds:
        STATE._data_libs = {}
    if "transforms" in kinds:
        STATE._transform_libs = {}
    if "agents" in kinds:
        STATE._agents = {}
    return {"reloaded": kinds}


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


# ===== Type Library Editing ================================================

@mcp.tool()
@_safe
async def create_type_library(
    path: str,
    ontology: dict | None = None,
    types: dict | None = None,
) -> dict:
    """Create a new type library YAML at the given path.

    Args:
        path: target YAML file
        ontology: {name, version, doi, strict} — defaults to EDAM
        types: {type_name: {properties: {...}, extends?: [...]}}
    """
    import yaml as _yaml
    p = Path(path).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        return {"error": f"file [{p}] already exists"}
    ont = ontology or {
        "name": "EDAM",
        "version": "1.25",
        "doi": "https://doi.org/10.1093/bioinformatics/btt113",
        "strict": False,
    }
    doc = {
        "schema": "v1",
        "ontology": ont,
        "types": types or {},
    }
    with open(p, "w") as f:
        _yaml.safe_dump(doc, f)
    return {"path": str(p), "type_count": len(doc["types"])}


@mcp.tool()
@_safe
async def add_type(
    library_path: str,
    name: str,
    properties: dict,
    extends: list[str] | None = None,
    overwrite: bool = False,
) -> dict:
    """Append a type definition to an existing type library YAML."""
    import yaml as _yaml
    p = Path(library_path).resolve()
    assert p.exists(), f"library [{p}] does not exist"
    with open(p) as f:
        doc = _yaml.safe_load(f) or {}
    if "types" not in doc:
        doc["types"] = {}
    if name in doc["types"] and not overwrite:
        return {"error": f"type [{name}] already exists; pass overwrite=True"}
    entry: dict = {"properties": properties}
    if extends:
        entry["extends"] = list(extends)
    doc["types"][name] = entry
    with open(p, "w") as f:
        _yaml.safe_dump(doc, f)
    return {"path": str(p), "name": name, "type_count": len(doc["types"])}


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
    lib = _get_data_lib(library_path)
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
    lib = _get_data_lib(library_path)
    results = []
    for p, dtype_name, _ep in lib.Iterate():
        if type_filter and dtype_name != type_filter:
            continue
        results.append({"path": str(p), "type_name": dtype_name})
    return results


def _get_data_lib(library_path: str) -> DataInstanceLibrary:
    """Resolve a data library path string against the cache or load it."""
    libs = STATE.data_libs
    if library_path in libs:
        return libs[library_path]
    p = str(Path(library_path).resolve())
    if p in libs:
        return libs[p]
    # try loading
    lib = DataInstanceLibrary.Load(p)
    STATE._data_libs[p] = lib
    return lib


@mcp.tool()
@_safe
async def create_data_library(
    path: str,
    type_library_paths: list[str] | None = None,
    purge: bool = False,
) -> dict:
    """Create a new DataInstanceLibrary directory and attach type libraries.

    Args:
        path: directory to create (will be made if missing)
        type_library_paths: type YAML files to attach via AddTypeLibrary
        purge: if True, wipe the directory first
    """
    p = Path(path).resolve()
    lib = DataInstanceLibrary(p)
    if purge:
        lib.Purge()
    if type_library_paths:
        for tp in type_library_paths:
            lib.AddTypeLibrary(Path(tp).resolve())
    lib.Save()
    STATE._data_libs[str(p)] = lib
    if p not in STATE.data_paths:
        STATE.data_paths.append(p)
    return {
        "path": str(p),
        "type_namespaces": list(lib.types.keys()),
    }


@mcp.tool()
@_safe
async def attach_type_library(
    library_path: str,
    type_library_path: str,
    namespace: str | None = None,
    on_exist: str = "skip",
) -> dict:
    """Attach a type library YAML to an existing data library."""
    lib = _get_data_lib(library_path)
    lib.AddTypeLibrary(Path(type_library_path).resolve(), namespace=namespace, on_exist=on_exist)
    lib.Save()
    return {"library": library_path, "type_namespaces": list(lib.types.keys())}


@mcp.tool()
@_safe
async def add_data_item(
    library_path: str,
    host_path: str,
    dtype: str,
    parents: list[str] | None = None,
    save: bool = True,
) -> dict:
    """Register an existing file as a typed DataInstance in a library.

    Args:
        library_path: data library directory
        host_path: absolute path to the file on the host
        dtype: type name like 'sequences::gbk'
        parents: list of already-registered item paths to attach as parents
        save: if True, persist the manifest after the add
    """
    lib = _get_data_lib(library_path)
    parent_paths = [Path(p) for p in (parents or [])]
    rec_path = lib.AddItem(Path(host_path), dtype, parents=parent_paths)
    if save:
        lib.Save()
    return {"library": library_path, "path": str(rec_path), "dtype": dtype}


@mcp.tool()
@_safe
async def add_data_value(
    library_path: str,
    name: str,
    value,
    dtype: str,
    parents: list[str] | None = None,
    save: bool = True,
) -> dict:
    """Register a scalar/dict value (string or JSON dict) as a typed DataInstance."""
    lib = _get_data_lib(library_path)
    parent_paths = [Path(p) for p in (parents or [])]
    rec_path = lib.AddValue(name, value, dtype, parents=parent_paths)
    if save:
        lib.Save()
    return {"library": library_path, "path": str(rec_path), "dtype": dtype, "value": value}


@mcp.tool()
@_safe
async def set_item_parents(
    library_path: str,
    item_path: str,
    parent_paths: list[str],
    save: bool = True,
) -> dict:
    """Attach parent DataInstances to an item."""
    lib = _get_data_lib(library_path)
    parents = [lib.Get(Path(p)) for p in parent_paths]
    lib.AddParentsTo(Path(item_path), parents)
    if save:
        lib.Save()
    return {"library": library_path, "path": item_path, "parents": parent_paths}


@mcp.tool()
@_safe
async def remove_data_item(library_path: str, item_path: str, save: bool = True) -> dict:
    """Remove an item from the library manifest (filesystem is left alone)."""
    lib = _get_data_lib(library_path)
    lib.Remove(Path(item_path))
    if save:
        lib.Save()
    return {"library": library_path, "removed": item_path}


@mcp.tool()
@_safe
async def rename_data_item(library_path: str, item_path: str, new_path: str) -> dict:
    """Rename an item in the manifest and filesystem (saves library)."""
    lib = _get_data_lib(library_path)
    lib.Rename(Path(item_path), Path(new_path))
    return {"library": library_path, "old": item_path, "new": new_path}


@mcp.tool()
@_safe
async def rename_by_parent(library_path: str, parent_type: str) -> dict:
    """Rename all items based on the path stem of their matching parent type."""
    lib = _get_data_lib(library_path)
    lib.RenameByParent(parent_type)
    return {"library": library_path, "parent_type": parent_type}


@mcp.tool()
@_safe
async def prune_types(
    library_path: str,
    whitelist: list[str] | None = None,
    save: bool = True,
) -> dict:
    """Drop type definitions that aren't referenced by manifest items."""
    lib = _get_data_lib(library_path)
    wl = set(whitelist) if whitelist else None
    lib.PruneTypes(save=save, whitelist=wl)
    return {"library": library_path, "type_namespaces": list(lib.types.keys())}


@mcp.tool()
@_safe
async def consolidate_library(library_path: str) -> dict:
    """Replace absolute-path items with local symlinks; returns the move map."""
    lib = _get_data_lib(library_path)
    new_paths = await asyncio.to_thread(lib.Consolidate)
    return {
        "library": library_path,
        "moves": {str(k): str(v) for k, v in new_paths.items()},
    }


@mcp.tool()
@_safe
async def save_library(library_path: str, update_types: bool = True) -> dict:
    """Explicitly save the library manifest and type files."""
    lib = _get_data_lib(library_path)
    lib.Save(update_types=update_types)
    return {"library": library_path, "saved": True}


@mcp.tool()
@_safe
async def trace_lineage(library_path: str, from_type: str, to_type: str) -> dict:
    """Yield (from_path, to_path) pairs connected by lineage."""
    lib = _get_data_lib(library_path)
    pairs: dict[str, list[str]] = {}
    for from_inst, to_inst in lib.Trace(from_type, to_type):
        pairs.setdefault(str(from_inst.path), []).append(str(to_inst.path))
    return {
        "library": library_path,
        "from_type": from_type,
        "to_type": to_type,
        "pairs": pairs,
    }


@mcp.tool()
@_safe
async def load_remote_library(
    src_uri: str,
    dest_path: str,
    on_exist: str = "skip",
    as_image: bool = True,
) -> dict:
    """Fetch a DataInstanceLibrary image from a Source URI and load it."""
    src = Source.Parse(src_uri)
    lib = await asyncio.to_thread(
        DataInstanceLibrary.LoadFrom,
        src,
        Path(dest_path).resolve(),
        as_image,
        on_exist,
    )
    STATE._data_libs[str(lib.location)] = lib
    if lib.location not in STATE.data_paths:
        STATE.data_paths.append(lib.location)
    return {
        "library": str(lib.location),
        "src": src_uri,
        "item_count": len(lib.manifest),
    }


@mcp.tool()
@_safe
async def show_item_lineage(library_path: str, item_path: str) -> dict:
    """Show item details and its parent lineage."""
    lib = _get_data_lib(library_path)
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


# ===== Transform Authoring =================================================

_SCAFFOLD_TEMPLATE = """from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
{requirements}
{products}

def protocol(context: ExecutionContext):
    # TODO: implement protocol body
    {protocol_body}
    return ExecutionResult(
        manifest=[
            {{
                {manifest_entries}
            }},
        ],
        success=True,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by={group_by_var},{resources_line}
)
"""


def _scaffold_var(prefix: str, idx: int) -> str:
    return f"{prefix}{idx}" if idx > 0 else prefix


def _resolve_transform_lib(library_path: str) -> TransformInstanceLibrary:
    libs = STATE.transform_libs
    if library_path in libs:
        return libs[library_path]
    p = str(Path(library_path).resolve())
    if p in libs:
        return libs[p]
    lib = TransformInstanceLibrary.Load(p)
    STATE._transform_libs[p] = lib
    return lib


@mcp.tool()
@_safe
async def read_transform_source(library_path: str, transform_path: str) -> dict:
    """Return the .py source code of a transform."""
    lib = _resolve_transform_lib(library_path)
    p = Path(transform_path)
    if not p.is_absolute():
        p = lib.location / p
    if p.suffix != ".py":
        p = p.with_suffix(".py")
    assert p.exists(), f"transform [{p}] does not exist"
    return {"library": library_path, "path": str(p), "source": p.read_text()}


@mcp.tool()
@_safe
async def write_transform(library_path: str, transform_path: str, source: str, register: bool = True) -> dict:
    """Write or overwrite a transform .py file in a library.

    If register=True (default), also registers it as 'transforms::transform' via AddStub.
    """
    lib = _resolve_transform_lib(library_path)
    rel = Path(transform_path)
    if rel.is_absolute():
        rel = rel.relative_to(lib.location)
    if rel.suffix != ".py":
        rel = rel.with_suffix(".py")
    target = lib.location / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source)
    if register and rel not in lib.manifest:
        lib.AddItem(rel, "transforms::transform")
        lib.Save()
    return {"library": library_path, "path": str(target), "bytes": len(source)}


@mcp.tool()
@_safe
async def scaffold_transform(
    library_path: str,
    name: str,
    inputs: list[str],
    outputs: list[str],
    group_by: str | None = None,
    container_type: str | None = None,
    resources: dict | None = None,
) -> dict:
    """Generate a transform .py skeleton and register it.

    Args:
        library_path: transform library directory
        name: transform name (file stem)
        inputs: list of input type names like ["ncbi::assembly_accession"]
        outputs: list of output type names like ["sequences::gbk"]
        group_by: input type to group by (defaults to first input)
        container_type: optional container type like "containers::myimage.oci"
        resources: optional {cpus, memory_gb, duration_h}
    """
    lib = _resolve_transform_lib(library_path)
    assert inputs, "at least one input required"
    assert outputs, "at least one output required"
    if group_by is None:
        group_by = inputs[0]
    assert group_by in inputs, f"group_by [{group_by}] not in inputs {inputs}"

    req_lines: list[str] = []
    var_names: dict[str, str] = {}
    if container_type:
        req_lines.append(f'image = model.AddRequirement(lib.GetType("{container_type}"))')
        var_names[container_type] = "image"
    for i, t in enumerate(inputs):
        v = _scaffold_var("dep", i)
        req_lines.append(f'{v}    = model.AddRequirement(lib.GetType("{t}"))')
        var_names[t] = v
    prod_lines: list[str] = []
    out_vars: list[str] = []
    for i, t in enumerate(outputs):
        v = _scaffold_var("out", i)
        prod_lines.append(f'{v}    = model.AddProduct(lib.GetType("{t}"))')
        out_vars.append(v)

    group_var = var_names[group_by]
    if container_type:
        protocol_body = f'context.ExecWithContainer(image=image, cmd="TODO")'
    else:
        protocol_body = f'context.external_shell.Exec("TODO")'
    manifest_entries = ",\n                ".join(
        f"{v}: context.Output({v}).local" for v in out_vars
    )
    resources_line = ""
    if resources:
        parts = []
        if "cpus" in resources:
            parts.append(f"cpus={resources['cpus']}")
        if "memory_gb" in resources:
            parts.append(f"memory=Size.GB({resources['memory_gb']})")
        if "duration_h" in resources:
            parts.append(f"duration=Duration(hours={resources['duration_h']})")
        resources_line = f"\n    resources=Resources({', '.join(parts)}),"

    source = _SCAFFOLD_TEMPLATE.format(
        requirements="\n".join(req_lines),
        products="\n".join(prod_lines),
        protocol_body=protocol_body,
        manifest_entries=manifest_entries,
        group_by_var=group_var,
        resources_line=resources_line,
    )
    rel = Path(name)
    if rel.suffix != ".py":
        rel = rel.with_suffix(".py")
    target = lib.location / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source)
    if rel not in lib.manifest:
        lib.AddItem(rel, "transforms::transform")
        lib.Save()
    return {"library": library_path, "path": str(target), "source": source}


@mcp.tool()
@_safe
async def validate_transform_contract(library_path: str, transform_path: str) -> dict:
    """Load the transform and check its contract resolves (no container pulls)."""
    lib = _resolve_transform_lib(library_path)
    p = Path(transform_path)
    if p.is_absolute():
        p = p.relative_to(lib.location)
    try:
        tr = lib.GetTransform(p, reload=True)
    except Exception as exc:
        return {"ok": False, "error": str(exc), "traceback": traceback.format_exc()}
    inputs = [{"key": d.key, "properties": d.Pack()["properties"]} for d in tr.model.requires]
    outputs = [
        [{"key": d.key, "properties": d.Pack()["properties"]} for d in group]
        for group in tr.model.produces
    ]
    return {
        "ok": True,
        "name": tr.name,
        "path": str(tr._path),
        "inputs": inputs,
        "outputs": outputs,
        "group_by_key": tr.group_by.key,
    }


@mcp.tool()
@_safe
async def propagate_types(transform_library: str) -> dict:
    """Copy registered type libraries into the transform library's _metadata/types/."""
    tlib = _resolve_transform_lib(transform_library)
    copied: list[str] = []
    for type_path in STATE.type_paths:
        if not type_path.exists():
            continue
        tlib.AddTypeLibrary(type_path, on_exist="overwrite")
        copied.append(str(type_path))
    tlib.Save(update_types=True)
    return {"library": transform_library, "copied": copied}


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
        targets.Add(t)

    # Build target model (same pattern as Agent.GenerateWorkflow)
    def _get_endpoint(dtype_name: str):
        ns, _ = dtype_name.split("::")
        for trlib in tr_libs:
            if ns not in trlib.types:
                continue
            return trlib.GetType(dtype_name)
        assert False, f"no transforms had the namespace [{ns}]"

    target_model = Transform()
    _spec2dep: dict[TargetSpec, Dependency] = {}
    target_names: list[str] = []
    for spec in targets.resolve():
        e = _get_endpoint(spec.dtype_name)
        d = target_model.AddRequirement(example=e, parents={_spec2dep[p] for p in spec.parents})
        _spec2dep[spec] = d
        target_names.append(spec.dtype_name)

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

    plan = gen_result
    if not plan.steps or plan.dropped_targets:
        return {
            "success": False,
            "message": "solver could not find a complete plan",
            "step_count": len(plan.steps),
            "dropped_targets": list(plan.dropped_targets),
            "hints": [
                {
                    "kind": h.kind,
                    "target": h.target,
                    "message": h.message,
                    "chain": list(h.chain),
                    "candidate_transforms": list(h.candidate_transforms),
                    "near_misses": list(h.near_misses),
                }
                for h in plan.hints
            ],
        }
    task = WorkflowTask(
        ok=len(plan.dropped_targets) == 0, plan=plan,
        data_libraries=[data_lib] + res_libs,
        transform_libraries=tr_libs,
    )
    task_key = None
    if STATE.workspace:
        task_key = await asyncio.to_thread(STATE.save_task, task)
    return {
        "success": True,
        "task_key": task_key,
        "steps": [step.Pack() for step in plan.steps],
        "targets": [t.Pack() for t in plan.targets],
        "step_count": len(plan.steps),
    }


# ===== Workflow Plan Introspection =========================================

@mcp.tool()
@_safe
async def get_workflow_plan(task_key: str) -> dict:
    """Re-fetch a saved plan from the workspace cache."""
    task = await asyncio.to_thread(STATE.load_task, task_key)
    plan = task.plan
    return {
        "task_key": task_key,
        "ok": task.ok,
        "step_count": len(plan.steps),
        "dropped_targets": list(plan.dropped_targets),
        "steps": [s.Pack() for s in plan.steps],
        "targets": [t.Pack() for t in plan.targets],
        "given": [inst.Pack() for inst in plan.given],
    }


@mcp.tool()
@_safe
async def get_plan_hints(task_key: str) -> list[dict]:
    """Return PlanHint records for a saved plan."""
    task = await asyncio.to_thread(STATE.load_task, task_key)
    return [
        {
            "kind": h.kind,
            "target": h.target,
            "message": h.message,
            "chain": list(h.chain),
            "candidate_transforms": list(h.candidate_transforms),
            "near_misses": list(h.near_misses),
        }
        for h in task.plan.hints
    ]


@mcp.tool()
@_safe
async def render_plan_dag(
    task_key: str,
    format: str = "svg",
    blacklist_namespaces: list[str] | None = None,
) -> dict:
    """Render the plan as a DAG image; returns the rendered file path."""
    task = await asyncio.to_thread(STATE.load_task, task_key)
    assert STATE.workspace is not None, "workspace not configured"
    out_base = STATE.workspace / task_key / f"plan.dag"
    bl = set(blacklist_namespaces) if blacklist_namespaces else {"lib", "containers"}
    await asyncio.to_thread(task.plan.RenderDAG, out_base, format, blacklist_namespaces=bl)
    rendered = out_base.with_suffix(f".{format}")
    return {"task_key": task_key, "format": format, "path": str(rendered)}


@mcp.tool()
@_safe
async def list_workflow_tasks() -> list[dict]:
    """List cached workflow tasks in the workspace."""
    assert STATE.workspace is not None, "workspace not configured"
    results = []
    for entry in sorted(STATE.workspace.iterdir()):
        if not entry.is_dir():
            continue
        info: dict = {"task_key": entry.name, "path": str(entry)}
        try:
            task = WorkflowTask.Load(entry)
            info["ok"] = task.ok
            info["step_count"] = len(task.plan.steps)
        except Exception as exc:
            info["error"] = str(exc)
        results.append(info)
    return results


@mcp.tool()
@_safe
async def delete_workflow_task(task_key: str) -> dict:
    """Remove a cached task from the workspace."""
    assert STATE.workspace is not None, "workspace not configured"
    target = STATE.workspace / task_key
    assert target.exists(), f"task [{task_key}] not in workspace"
    import shutil
    shutil.rmtree(target)
    STATE._tasks.pop(task_key, None)
    return {"task_key": task_key, "deleted": True}


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


# ===== Agent Tools ========================================================

@mcp.tool()
@_safe
async def list_agents() -> list[dict]:
    """List all loaded agents."""
    return [
        {"name": name, "home": agent.home.address, "container": agent.container, "runtime": agent.runtime.name}
        for name, agent in STATE.agents.items()
    ]


@mcp.tool()
@_safe
async def load_agent(agent_path: str, name: str | None = None) -> dict:
    """Load an agent from a YAML config file.

    Args:
        agent_path: path to the agent YAML file
        name: optional display name (defaults to filename stem)
    """
    p = Path(agent_path)
    agent = await asyncio.to_thread(Agent.Load, p)
    key = name or p.stem
    STATE._agents[key] = agent
    return {"name": key, "home": agent.home.address, "container": agent.container, "runtime": agent.runtime.name}


@mcp.tool()
@_safe
async def save_agent(
    path: str,
    home_uri: str,
    container: str | None = None,
    runtime: str = "DOCKER",
    setup_commands: list[str] | None = None,
    globus_uuid: str | None = None,
) -> dict:
    """Write an agent YAML to disk and register it.

    Args:
        path: target YAML file
        home_uri: Source URI for the agent home (e.g. /local/path or ssh://host/path)
        container: container image URL (defaults to the bundled metasmith container)
        runtime: 'DOCKER' or 'APPTAINER'
        setup_commands: optional shell commands run inside AgentShell
        globus_uuid: optional Globus endpoint UUID for the agent host
    """
    from ..coms.containers import ContainerRuntime
    p = Path(path).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)
    home = Source.Parse(home_uri)
    agent = Agent(
        home=home,
        setup_commands=setup_commands or [],
        runtime=ContainerRuntime[runtime],
        globus_uuid=globus_uuid,
    )
    if container:
        agent.container = container
    agent.Save(p)
    STATE._agents[p.stem] = agent
    if p not in STATE.agent_paths:
        STATE.agent_paths.append(p)
    return {"name": p.stem, "path": str(p), "home": home.address, "runtime": agent.runtime.name}


@mcp.tool()
@_safe
async def get_agent_info(agent_name: str) -> dict:
    """Full info for a loaded agent."""
    agent = STATE.agents[agent_name]
    presets = {}
    try:
        presets = {k: str(v) for k, v in agent.GetNxfConfigPresets().items()}
    except Exception:
        pass
    return {
        "name": agent_name,
        "home": agent.home.address,
        "home_type": agent.home.type.name,
        "container": agent.container,
        "runtime": agent.runtime.name,
        "globus_uuid": agent.globus_uuid,
        "real_path": str(agent.real_path) if agent.real_path else None,
        "setup_commands": list(agent.setup_commands),
        "config_presets": presets,
    }


@mcp.tool()
@_safe
async def agent_ping(agent_name: str, timeout_s: int = 15) -> dict:
    """Run `echo ok && hostname` against the agent host."""
    agent = STATE.agents[agent_name]
    res = await asyncio.to_thread(agent._remote_oneshot, "echo ok && hostname", timeout_s)
    return {"agent": agent_name, "out": res.out, "err": res.err, "ok": any("ok" in ln for ln in res.out)}


@mcp.tool()
@_safe
async def deploy_agent(agent_name: str, assertive: bool = False) -> dict:
    """Deploy an agent to its home location (one-time setup, uses SSH).

    Args:
        agent_name: name of a loaded agent
        assertive: if True, force re-deploy even if already deployed
    """
    agent = STATE.agents[agent_name]
    await asyncio.to_thread(agent.Deploy, assertive)
    return {"status": "deployed", "agent": agent_name, "home": agent.home.address}


# ===== Lifecycle Tools ====================================================

@mcp.tool()
@_safe
async def stage_workflow(agent_name: str, task_key: str, on_exist: str = "skip") -> dict:
    """Stage a planned workflow on an agent — compiles DAG to Nextflow and transfers.

    Args:
        agent_name: name of a loaded agent
        task_key: task key returned by plan_workflow
        on_exist: behavior if already staged: skip, error, clear, update, update_workflow, update_data
    """
    agent = STATE.agents[agent_name]
    task = await asyncio.to_thread(STATE.load_task, task_key)
    await asyncio.to_thread(agent.StageWorkflow, task, on_exist)
    return {"status": "staged", "task_key": task_key, "agent": agent_name}


@mcp.tool()
@_safe
async def run_workflow(
    agent_name: str,
    task_key: str,
    config_preset: str | None = None,
    params: dict | None = None,
    resource_overrides: dict | None = None,
    stub_delay: float = 0,
) -> dict:
    """Launch a staged workflow on an agent (async — returns immediately).

    Args:
        agent_name: name of a loaded agent
        task_key: task key returned by plan_workflow
        config_preset: optional Nextflow config preset name (see list_config_presets)
        params: optional Nextflow params dict
        resource_overrides: optional {step_key: {cpus, memory_gb, duration_h}} map
        stub_delay: if >0, run in dry/stub mode with the given inter-step delay
    """
    agent = STATE.agents[agent_name]
    config_file = None
    if config_preset:
        presets = agent.GetNxfConfigPresets()
        assert config_preset in presets, f"preset [{config_preset}] not found, available: {list(presets.keys())}"
        config_file = presets[config_preset]
    # translate plain-dict resource_overrides into Resources objects
    ro = None
    if resource_overrides:
        from ..models.libraries import Resources, Size, Duration
        ro = {}
        for k, v in resource_overrides.items():
            kw = {}
            if "cpus" in v: kw["cpus"] = v["cpus"]
            if "memory_gb" in v: kw["memory"] = Size.GB(v["memory_gb"])
            if "duration_h" in v: kw["duration"] = Duration(hours=v["duration_h"])
            ro[k] = Resources(**kw)
    await asyncio.to_thread(agent.RunWorkflow, task_key, config_file, params, ro, stub_delay)
    return {"status": "running", "task_key": task_key, "agent": agent_name}


@mcp.tool()
@_safe
async def wait_for_workflow(
    agent_name: str,
    task_key: str,
    timeout_s: float = 3600.0,
    poll_s: float = 5.0,
    run: int | None = None,
    since_mtime: float | None = None,
) -> dict:
    """Block until the agent writes the 'run completed at' sentinel in agent.log."""
    agent = STATE.agents[agent_name]
    return await asyncio.to_thread(
        agent.WaitForWorkflow, task_key, timeout_s, poll_s, run, "run completed at", since_mtime,
    )


@mcp.tool()
@_safe
async def tail_workflow_log(
    agent_name: str,
    task_key: str,
    source: str = "agent",
    lines: int = 50,
    run: int | None = None,
) -> dict:
    """Return the last N lines of agent.log or main.log for the selected run."""
    agent = STATE.agents[agent_name]
    return await asyncio.to_thread(agent.TailWorkflowLog, task_key, source, lines, run)


@mcp.tool()
@_safe
async def cancel_workflow(agent_name: str, task_key: str, timeout_s: float = 30.0) -> dict:
    """Best-effort cancel of a running workflow via PID.lock removal."""
    agent = STATE.agents[agent_name]
    return await asyncio.to_thread(agent.CancelWorkflow, task_key, timeout_s)


@mcp.tool()
@_safe
async def list_workflow_runs(agent_name: str, task_key: str) -> list[dict]:
    """List all runs (logs.<ts>) for a task on an agent."""
    agent = STATE.agents[agent_name]
    return await asyncio.to_thread(agent.ListWorkflowRuns, task_key)


@mcp.tool()
@_safe
async def collect_results(
    agent_name: str,
    task_key: str,
    dest_uri: str,
    allow_globus: bool = True,
) -> dict:
    """Transfer the results directory from an agent to a destination URI."""
    agent = STATE.agents[agent_name]
    src = agent.GetResultSource(task_key, allow_globus=allow_globus, check_exists=False)
    dest = Source.Parse(dest_uri)
    mover = Logistics()
    mover.QueueTransfer(src=src, dest=dest)
    res = await asyncio.to_thread(mover.ExecuteTransfers, f"collect.{task_key}", True)
    return {
        "src": src.address,
        "dest": dest.address,
        "completed": [(s.address, d.address) for s, d in res.completed],
        "errors": list(res.errors),
    }


@mcp.tool()
@_safe
async def get_result_source(agent_name: str, task_key: str) -> dict:
    """Get the source location of workflow results.

    Args:
        agent_name: name of a loaded agent
        task_key: task key returned by plan_workflow
    """
    agent = STATE.agents[agent_name]
    source = agent.GetResultSource(task_key)
    return {"address": source.address, "type": source.type.name}


@mcp.tool()
@_safe
async def list_config_presets(agent_name: str) -> dict:
    """List available Nextflow config presets for an agent.

    Args:
        agent_name: name of a loaded agent
    """
    agent = STATE.agents[agent_name]
    presets = agent.GetNxfConfigPresets()
    return {name: str(path) for name, path in presets.items()}


# ===== Source / Logistics =================================================

@mcp.tool()
@_safe
async def parse_source(uri: str) -> dict:
    """Parse a URI into a Source — supports ssh://, http(s)://, globus://, and local paths."""
    src = Source.Parse(uri)
    return {
        "address": src.address,
        "type": src.type.name,
        "name": src.GetName(),
        "path": str(src.GetPath()),
    }


@mcp.tool()
@_safe
async def source_exists(uri: str, timeout_s: int = 10) -> dict:
    """Probe whether a Source URI is reachable. Local + SSH + HTTP supported."""
    src = Source.Parse(uri)
    exists = False
    detail: str = ""
    if src.type in (SourceType.DIRECT, SourceType.SYMLINK):
        p = Path(src.address)
        exists = p.exists()
        detail = "filesystem"
    elif src.type == SourceType.SSH:
        from ..models.remote import SshSource
        ssh = SshSource.Parse(src.address)
        import subprocess
        cmd = ["ssh", "-o", f"ConnectTimeout={timeout_s}", "-o", "BatchMode=yes", ssh.host, f"test -e {ssh.path} && echo OK"]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s + 5)
            exists = "OK" in res.stdout
            detail = f"ssh rc={res.returncode}"
        except Exception as exc:
            detail = f"ssh error: {exc}"
    elif src.type == SourceType.HTTP:
        import urllib.request
        try:
            req = urllib.request.Request(src.address, method="HEAD")
            with urllib.request.urlopen(req, timeout=timeout_s) as r:
                exists = 200 <= r.status < 400
                detail = f"http {r.status}"
        except Exception as exc:
            detail = f"http error: {exc}"
    else:
        detail = f"probe not implemented for {src.type.name}"
    return {"uri": uri, "exists": exists, "type": src.type.name, "detail": detail}


@mcp.tool()
@_safe
async def transfer_source(src_uri: str, dest_uri: str, wait: bool = True, label: str | None = None) -> dict:
    """Copy data between two Sources via Logistics."""
    src = Source.Parse(src_uri)
    dest = Source.Parse(dest_uri)
    mover = Logistics()
    mover.QueueTransfer(src=src, dest=dest)
    res = await asyncio.to_thread(mover.ExecuteTransfers, label, wait)
    return {
        "completed": [(s.address, d.address) for s, d in res.completed],
        "errors": list(res.errors),
    }


# ===== Build Tools ========================================================

@mcp.tool()
@_safe
async def build_libraries(
    type_paths: list[str] | None = None,
    transform_paths: list[str] | None = None,
) -> dict:
    """Build/compile type libraries and propagate types to transform libraries.

    Args:
        type_paths: paths to type definition directories (defaults to server's --types)
        transform_paths: paths to transform library directories (defaults to server's --transforms)
    """
    type_dirs = [Path(p) for p in type_paths] if type_paths else list(STATE.type_paths)
    transform_dirs = [Path(p) for p in transform_paths] if transform_paths else list(STATE.transform_paths)
    await asyncio.to_thread(Build, type_dirs, transform_dirs, [])
    # invalidate caches so next access reloads
    STATE._type_libs = {}
    STATE._transform_libs = {}
    return {
        "status": "built",
        "type_paths": [str(p) for p in type_dirs],
        "transform_paths": [str(p) for p in transform_dirs],
    }


# ===== MCP Resources ======================================================

@mcp.resource("metasmith://server/status")
@_safe
async def resource_server_status() -> str:
    """Server status snapshot."""
    return json.dumps(await server_status(), indent=2, default=str)


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


@mcp.resource("metasmith://agents")
@_safe
async def resource_agents() -> str:
    """All loaded agents."""
    return json.dumps(await list_agents(), indent=2, default=str)


@mcp.resource("metasmith://agents/{name}")
@_safe
async def resource_agent_detail(name: str) -> str:
    """Details for one agent."""
    if name not in STATE.agents:
        return json.dumps({"error": f"agent [{name}] not loaded"})
    a = STATE.agents[name]
    return json.dumps({
        "name": name,
        "home": a.home.address,
        "container": a.container,
        "runtime": a.runtime.name,
        "globus_uuid": a.globus_uuid,
        "real_path": str(a.real_path) if a.real_path else None,
    }, indent=2)


@mcp.resource("metasmith://tasks")
@_safe
async def resource_tasks() -> str:
    """Workspace task inventory."""
    return json.dumps(await list_workflow_tasks(), indent=2, default=str)


@mcp.resource("metasmith://tasks/{task_key}")
@_safe
async def resource_task_detail(task_key: str) -> str:
    """Plan + targets for one task."""
    return json.dumps(await get_workflow_plan(task_key), indent=2, default=str)


@mcp.resource("metasmith://tasks/{task_key}/dag")
@_safe
async def resource_task_dag(task_key: str) -> str:
    """Render and return the DAG path."""
    return json.dumps(await render_plan_dag(task_key), indent=2)


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
        "--agents", nargs="*", default=[], metavar="PATH",
        help="Paths to agent YAML config files",
    )
    parser.add_argument(
        "--workspace", default=None, metavar="PATH",
        help="Workspace directory for caching workflow tasks (default: ~/.metasmith/mcp_workspace)",
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
    STATE.agent_paths = [Path(p) for p in args.agents] + _paths_from_env("METASMITH_AGENTS")

    workspace = args.workspace or os.environ.get("METASMITH_WORKSPACE")
    if workspace:
        STATE.workspace = Path(workspace)
    else:
        STATE.workspace = Path.home() / ".metasmith" / "mcp_workspace"
    STATE.workspace.mkdir(parents=True, exist_ok=True)

    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()
