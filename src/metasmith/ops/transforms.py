"""Transform library inspection and authoring."""
from __future__ import annotations

from pathlib import Path

from ._common import load_transform_lib, dep_info
from ..env.dispatch_scan import ScanSource
from ..logging import Log


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
    env_type: str | None = None,
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
    if env_type:
        req_lines.append(f'image = model.AddRequirement(lib.GetType("{env_type}"))')
        var_names[env_type] = "image"
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
    if env_type:
        # Both arms scaffolded: which worlds this tool supports is the author's
        # call, and deleting the arm that does not apply is a smaller ask than
        # remembering the one that does exist.
        protocol_body = (
            'context.ExecWithEnv() \\\n'
            '        .ifContainerDo(env=image, cmd="TODO") \\\n'
            '        .ifVirtualEnvDo(env=image, cmd="TODO")'
        )
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


class TransformContractError(Exception):
    """A transform's source violates the ExecWithEnv contract."""


def check_env_declarations(source: str, filename: str = "<transform>") -> dict:
    """Static half of `validate`: how this transform declares its tool runs.

    Errors are contract violations (a chain with no arm can only no-op; a
    retired or private launch entry point runs a tool outside the arms
    entirely). Warnings are shape observations that do not make the transform
    wrong.

    Syntactic only -- a protocol that dispatches through a helper function is
    invisible here, so a clean result is the absence of a detected violation,
    not a proof of portability.
    """
    scan = ScanSource(source, filename=filename)
    errors: list[str] = []
    warnings: list[str] = []

    for name, lineno in scan.forbidden:
        errors.append(
            f"line {lineno}: [{name}] is not a tool-launch entry point; "
            f"declare the run with ExecWithEnv().ifContainerDo(...)/.ifVirtualEnvDo(...)"
        )
    for chain in scan.empty_chains():
        errors.append(
            f"line {chain.lineno}: ExecWithEnv() declares no arm, so it can never run anything"
        )
    for chain in scan.chains:
        if len(set(chain.arms)) != len(chain.arms):
            errors.append(f"line {chain.lineno}: repeated arm in one chain {chain.arms}")
        if chain.duplicate_command:
            warnings.append(
                f"line {chain.lineno}: both arms carry byte-identical commands; "
                f"the split is carrying no information here"
            )
    for lineno in scan.host_shell_calls:
        warnings.append(
            f"line {lineno}: external_shell.Exec runs on the host shell, not in a "
            f"tool environment, and does not go through the arms"
        )

    return {
        "arms": scan.arms,
        "chains": [
            {"line": c.lineno, "arms": c.arms, "envs": c.envs} for c in scan.chains
        ],
        "errors": errors,
        "warnings": warnings,
        "syntactic_only": True,
    }


def validate_contract(library_path: str, transform_path: str) -> dict:
    """Reload the transform and check its contract resolves."""
    lib = load_transform_lib(library_path)
    p = Path(transform_path)
    if p.is_absolute():
        p = p.relative_to(lib.location)
    # GetTransform accepts a bare name and adds the suffix itself; the static
    # scan reads the file directly, so normalize here rather than twice.
    if p.suffix != ".py":
        p = p.with_suffix(".py")
    tr = lib.GetTransform(p, reload=True)
    inputs = [{"key": d.key, "properties": d.Pack()["properties"]} for d in tr.model.requires]
    outputs = [
        [{"key": d.key, "properties": d.Pack()["properties"]} for d in group]
        for group in tr.model.produces
    ]
    with open(lib.location/p) as f:
        env = check_env_declarations(f.read(), filename=str(p))
    if env["errors"]:
        raise TransformContractError(
            f"transform [{p}] violates the ExecWithEnv contract:\n  "
            + "\n  ".join(env["errors"])
        )
    for w in env["warnings"]:
        Log.Warn(f"{p}: {w}")
    return {
        "ok": True,
        "name": tr.name,
        "path": str(tr._path),
        "inputs": inputs,
        "outputs": outputs,
        "group_by_key": tr.group_by.key,
        "env": env,
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
