"""Shared helpers for new scenario `setup_fixtures` hooks.

Each helper invokes the sandbox-local metasmith binary (installed at
``<sandbox>/envs/msm_env/bin/metasmith`` by ``install_metasmith_into_sandbox``)
via subprocess with ``env_for_agent``. Using metasmith's own CLI to build
fixtures guarantees they are valid metasmith artifacts (no hand-rolled YAML
that can drift from the schema).

Transforms specifically are written as full ``.py`` source files (not via
``transform scaffold``) because scaffold requires an already-initialized
library, which is the operation we are trying to bootstrap. The
``metasmith build`` command compiles the lib's ``_metadata/`` once
everything is in place.
"""
from __future__ import annotations

import json
import subprocess
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..harness.sandbox import SandboxLayout, env_for_agent


_ENV_NAME = "msm_env"


def _msm_bin(layout: SandboxLayout) -> Path:
    return layout.root / "envs" / _ENV_NAME / "bin" / "metasmith"


def run_msm(
    layout: SandboxLayout,
    args: list[str],
    *,
    check: bool = True,
    capture: bool = True,
) -> subprocess.CompletedProcess:
    """Run ``metasmith <args>`` inside the sandbox env."""
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


# ---------------------------------------------------------------------------
# Type library
# ---------------------------------------------------------------------------


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
    """Create ``types_dir/<namespace>.yml`` with the given types.

    Returns the YAML file path. The directory form is what ``metasmith
    build -t`` expects (LoadTypeLibraries scans for ``*.yml``).
    """
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


# ---------------------------------------------------------------------------
# Transform library — hand-written .py files compiled via `metasmith build`
# ---------------------------------------------------------------------------


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
    name: str                    # filename stem
    inputs: list[str]            # type names like "myns::foo"
    outputs: list[str]
    group_by: str | None = None  # which input to group by (default: first input)


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
    """Materialize a transform library and (optionally) compile its metadata.

    Each TransformSpec becomes ``<lib_dir>/<name>.py`` with a no-op protocol.
    With ``compile_now=True`` (default), ``metasmith build -t <types_dir>
    -r <lib_dir>`` runs to produce ``_metadata/index.yml`` and the embedded
    type copies — the lib is then loadable by ``plan``, ``transform list``,
    ``transform show``, etc.

    Pass ``compile_now=False`` for the recovery scenarios that deliberately
    leave the lib in a broken state.
    """
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


# ---------------------------------------------------------------------------
# Data instance library
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DataItemSpec:
    """A single item in a data instance library.

    Exactly one of ``host_path`` (file-backed) or ``value`` (scalar/dict)
    must be set. ``parents`` lists previous-item ``name``s.
    """
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
    """Create a data instance library and populate it."""
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
    """Drop a small textual fixture file (e.g. a sample input)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text))
    return path


def write_raw_transform_lib(
    layout: SandboxLayout,
    lib_dir: Path,
    types_dir: Path,
    transform_sources: dict[str, str],
) -> Path:
    """Materialize a transform library from already-written .py sources.

    For cases where the no-op template doesn't fit (e.g. parent-aware
    contracts that ``TransformSpec`` doesn't expose). Each entry in
    ``transform_sources`` is ``{stem: source_text}`` — the source is
    written to ``<lib_dir>/<stem>.py`` verbatim. ``metasmith build``
    is then run to compile the lib's ``_metadata``.
    """
    lib_dir.mkdir(parents=True, exist_ok=True)
    for stem, src in transform_sources.items():
        (lib_dir / f"{stem}.py").write_text(src)
    run_msm(layout, [
        "build",
        "-t", str(types_dir),
        "-r", str(lib_dir),
    ])
    return lib_dir
