from __future__ import annotations

import json
import os
import shutil
import subprocess
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..harness.sandbox import SandboxLayout, env_for_agent


_ENV_NAME = "msm_env"
_MLIB_LAYOUT_MARKER = Path("data_types")


def _msm_bin(layout: SandboxLayout) -> Path:
    return layout.root / "envs" / _ENV_NAME / "bin" / "metasmith"


def _project_root() -> Path:
    p = Path(__file__).resolve()
    for parent in p.parents:
        if (parent / "setup.py").exists() or (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError(f"could not locate project root from {p}")


def _validate_mlib(root: Path) -> bool:
    return (root / _MLIB_LAYOUT_MARKER).exists()


def _resolve_lib_root(candidate: Path) -> Path | None:
    if _validate_mlib(candidate):
        return candidate
    nested = candidate / "main"
    if _validate_mlib(nested):
        return nested
    return None


def _metasmith_libraries_root() -> Path:
    explicit = os.environ.get("METASMITH_LIBRARIES_ROOT")
    if explicit:
        root = _resolve_lib_root(Path(explicit).expanduser().resolve())
        if root is None:
            raise RuntimeError(
                f"METASMITH_LIBRARIES_ROOT={explicit} does not contain "
                f"{_MLIB_LAYOUT_MARKER} (checked both root and root/main)"
            )
        return root
    root = _resolve_lib_root(_project_root() / "src" / "metasmith_libraries")
    if root is None:
        raise RuntimeError(
            f"no standard library at {_project_root() / 'src' / 'metasmith_libraries'}"
        )
    return root


def stage_real_libraries(layout: SandboxLayout) -> Path:
    source = _metasmith_libraries_root()
    dest = layout.root / "MetasmithLibraries"
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(source, dest, symlinks=True)
    if not _validate_mlib(dest):
        raise RuntimeError(
            f"staged MetasmithLibraries at {dest} is missing {_MLIB_LAYOUT_MARKER}"
        )
    head = subprocess.run(
        ["git", "-C", str(_project_root()), "rev-parse", "HEAD"],
        capture_output=True, text=True,
    )
    if head.returncode == 0:
        (dest / "STAGED_FROM").write_text(
            f"{_project_root()}\n{head.stdout.strip()}\n"
        )
    return dest


def run_msm(
    layout: SandboxLayout,
    args: list[str],
    *,
    check: bool = True,
    capture: bool = True,
) -> subprocess.CompletedProcess:
    bin_path = _msm_bin(layout)
    if not bin_path.exists():
        raise RuntimeError(
            f"metasmith not installed in sandbox env: {bin_path}\n"
            f"  scenario must set pre_install_metasmith = True"
        )
    env = env_for_agent(layout)
    cmd = [str(bin_path), *args]
    r = subprocess.run(cmd, env=env, capture_output=capture, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(
            f"fixture setup failed: {' '.join(cmd)}\n"
            f"  exit={r.returncode}\n"
            f"  stdout={r.stdout[-800:]}\n"
            f"  stderr={r.stderr[-800:]}"
        )
    return r


@dataclass(frozen=True)
class TypeSpec:
    name: str
    properties: dict[str, Any]
    extends: str | None = None


def build_type_lib_dir(
    layout: SandboxLayout,
    types_dir: Path,
    namespace: str,
    types: list[TypeSpec],
) -> Path:
    types_dir.mkdir(parents=True, exist_ok=True)
    yml = types_dir / f"{namespace}.yml"
    types_json = {
        t.name: ({"properties": t.properties}
                 | ({"extends": t.extends} if t.extends else {}))
        for t in types
    }
    run_msm(layout, [
        "type", "create", str(yml),
        "--types", json.dumps(types_json),
    ])
    if not yml.exists():
        raise RuntimeError(f"type create did not produce {yml}")
    return yml


_NOOP_TRANSFORM_TEMPLATE = '''\
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
{requirements}
{products}

def protocol(context: ExecutionContext):
    {output_setup}
    return ExecutionResult(
        manifest=[
            {{ {manifest_entries} }},
        ],
        success=True,
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by={group_by_var},
)
'''


@dataclass(frozen=True)
class TransformSpec:
    name: str
    inputs: list[str]
    outputs: list[str]
    group_by: str | None = None


def _render_noop_transform(spec: TransformSpec) -> str:
    if not spec.inputs:
        raise ValueError(f"transform {spec.name}: need at least one input")
    if not spec.outputs:
        raise ValueError(f"transform {spec.name}: need at least one output")
    req_lines = []
    var_names: dict[str, str] = {}
    for i, t in enumerate(spec.inputs):
        v = f"dep_{i}"
        req_lines.append(f'{v} = model.AddRequirement(lib.GetType("{t}"))')
        var_names[t] = v
    prod_lines = []
    out_vars: list[str] = []
    for i, t in enumerate(spec.outputs):
        v = f"out_{i}"
        prod_lines.append(f'{v} = model.AddProduct(lib.GetType("{t}"))')
        out_vars.append(v)
    group_by = spec.group_by or spec.inputs[0]
    if group_by not in var_names:
        raise ValueError(
            f"transform {spec.name}: group_by {group_by!r} not in inputs"
        )
    out_setup_lines = [
        f"{v}_path = context.Output({v})" for v in out_vars
    ]
    out_setup_lines.extend(
        f'context.external_shell.Exec(f"touch {{{v}_path.external}}")'
        for v in out_vars
    )
    out_setup = "\n    ".join(out_setup_lines)
    manifest_entries = ", ".join(
        f"{v}: {v}_path.local" for v in out_vars
    )
    return _NOOP_TRANSFORM_TEMPLATE.format(
        requirements="\n".join(req_lines),
        products="\n".join(prod_lines),
        output_setup=out_setup,
        manifest_entries=manifest_entries,
        group_by_var=var_names[group_by],
    )


def build_transform_lib(
    layout: SandboxLayout,
    lib_dir: Path,
    types_dir: Path,
    transforms: list[TransformSpec],
    *,
    compile_now: bool = True,
) -> Path:
    lib_dir.mkdir(parents=True, exist_ok=True)
    for tr in transforms:
        (lib_dir / f"{tr.name}.py").write_text(_render_noop_transform(tr))
    if compile_now:
        run_msm(layout, [
            "build",
            "-t", str(types_dir),
            "-r", str(lib_dir),
        ])
    return lib_dir


@dataclass(frozen=True)
class DataItemSpec:
    name: str
    dtype: str
    host_path: Path | None = None
    value: str | None = None
    json_value: bool = False
    parents: list[str] | None = None


def build_data_lib(
    layout: SandboxLayout,
    lib_path: Path,
    type_lib_yaml: Path,
    items: list[DataItemSpec],
) -> Path:
    lib_path.parent.mkdir(parents=True, exist_ok=True)
    run_msm(layout, [
        "data", "create", str(lib_path),
        "--type-lib", str(type_lib_yaml),
    ])
    for it in items:
        if (it.host_path is None) == (it.value is None):
            raise ValueError(
                f"DataItemSpec {it.name!r}: exactly one of "
                f"host_path/value must be set"
            )
        if it.host_path is not None:
            argv = [
                "data", "add-item", str(lib_path),
                "--path", str(it.host_path),
                "--dtype", it.dtype,
            ]
        else:
            argv = [
                "data", "add-value", str(lib_path),
                "--name", it.name,
                "--value", it.value,
                "--dtype", it.dtype,
            ]
            if it.json_value:
                argv += ["--json-value"]
        for p in (it.parents or []):
            argv += ["--parent", p]
        run_msm(layout, argv)
    return lib_path


def write_text_file(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text))
    return path


def write_raw_transform_lib(
    layout: SandboxLayout,
    lib_dir: Path,
    types_dir: Path,
    transform_sources: dict[str, str],
) -> Path:
    lib_dir.mkdir(parents=True, exist_ok=True)
    for stem, src in transform_sources.items():
        (lib_dir / f"{stem}.py").write_text(src)
    run_msm(layout, [
        "build",
        "-t", str(types_dir),
        "-r", str(lib_dir),
    ])
    return lib_dir
