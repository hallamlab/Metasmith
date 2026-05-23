"""Transform library inspection and authoring."""
from __future__ import annotations

from pathlib import Path

from ._common import load_transform_lib, dep_info


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


def list_libraries(library_paths: list[str]) -> list[dict]:
    results = []
    for path in library_paths:
        lib = load_transform_lib(path)
        namespaces = [ns for ns in lib.types.keys() if ns != "transforms"]
        count = sum(1 for _ in lib.Iterate())
        results.append({
            "path": str(path),
            "transform_count": count,
            "type_namespaces": namespaces,
        })
    return results


def list_transforms(library_paths: list[str]) -> list[dict]:
    results = []
    for path in library_paths:
        lib = load_transform_lib(path)
        for tr_path, tr in lib.IterateTransforms():
            results.append({"name": tr.name, "path": str(tr_path), "library": str(path)})
    return results


def show_contract(library_path: str, transform_path: str) -> dict:
    lib = load_transform_lib(library_path)
    tr = lib.GetTransform(transform_path)
    inputs = [dep_info(d, lib) for d in tr.model.requires]
    outputs = [[dep_info(d, lib) for d in group] for group in tr.model.produces]
    group_by = dep_info(tr.group_by, lib)
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


def read_source(library_path: str, transform_path: str) -> dict:
    lib = load_transform_lib(library_path)
    p = Path(transform_path)
    if not p.is_absolute():
        p = lib.location / p
    if p.suffix != ".py":
        p = p.with_suffix(".py")
    assert p.exists(), f"transform [{p}] does not exist"
    return {"library": str(library_path), "path": str(p), "source": p.read_text()}


def write_transform(library_path: str, transform_path: str, source: str, register: bool = True) -> dict:
    lib = load_transform_lib(library_path)
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
    return {"library": str(library_path), "path": str(target), "bytes": len(source)}


def scaffold_transform(
    library_path: str,
    name: str,
    inputs: list[str],
    outputs: list[str],
    group_by: str | None = None,
    container_type: str | None = None,
    resources: dict | None = None,
) -> dict:
    lib = load_transform_lib(library_path)
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
        protocol_body = 'context.ExecWithContainer(image=image, cmd="TODO")'
    else:
        protocol_body = 'context.external_shell.Exec("TODO")'
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
    return {"library": str(library_path), "path": str(target), "source": source}


def validate_contract(library_path: str, transform_path: str) -> dict:
    """Reload the transform and check its contract resolves."""
    lib = load_transform_lib(library_path)
    p = Path(transform_path)
    if p.is_absolute():
        p = p.relative_to(lib.location)
    tr = lib.GetTransform(p, reload=True)
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


def propagate_types(transform_library: str, type_paths: list[str]) -> dict:
    """Copy type libraries into the transform library's _metadata/types/."""
    tlib = load_transform_lib(transform_library)
    copied: list[str] = []
    for tp in type_paths:
        type_path = Path(tp)
        if not type_path.exists():
            continue
        tlib.AddTypeLibrary(type_path, on_exist="overwrite")
        copied.append(str(type_path))
    tlib.Save(update_types=True)
    return {"library": str(transform_library), "copied": copied}
