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
import os
import shutil
import subprocess
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..harness.sandbox import SandboxLayout, env_for_agent


_ENV_NAME = "msm_env"
_METASMITH_LIBRARIES_REPO = "https://github.com/hallamlab/MetasmithLibraries.git"
_MLIB_LAYOUT_MARKER = Path("resources") / "containers" / "_metadata" / "index.yml"


def _msm_bin(layout: SandboxLayout) -> Path:
    return layout.root / "envs" / _ENV_NAME / "bin" / "metasmith"


# ---------------------------------------------------------------------------
# MetasmithLibraries discovery + staging
# ---------------------------------------------------------------------------


def _project_root() -> Path:
    """Walk up from this file to the metasmith dev tree root."""
    p = Path(__file__).resolve()
    for parent in p.parents:
        if (parent / "setup.py").exists() or (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError(f"could not locate project root from {p}")


def _validate_mlib(root: Path) -> bool:
    """Return True if ``root`` is the canonical MetasmithLibraries lib root.

    The canonical layout has ``data_types/``, ``resources/``, ``transforms/``
    directly at the top level (matching the ``main`` branch of the
    upstream repo).
    """
    return (root / _MLIB_LAYOUT_MARKER).exists()


def _resolve_lib_root(candidate: Path) -> Path | None:
    """Given a path that might be a MetasmithLibraries checkout or the
    parent of one, return the canonical lib root or None.

    Handles multi-worktree layouts where ``<candidate>/main`` is the
    actual working tree of the ``main`` branch.
    """
    if _validate_mlib(candidate):
        return candidate
    nested = candidate / "main"
    if _validate_mlib(nested):
        return nested
    return None


def _metasmith_libraries_cache_dir() -> Path:
    return _project_root() / "tests" / "e2e_agentic" / ".cache" / "MetasmithLibraries"


def _metasmith_libraries_root() -> Path:
    """Resolve the source MetasmithLibraries checkout.

    Priority:
    1. ``METASMITH_LIBRARIES_ROOT`` env var.
    2. Sibling dir ``<project_root>/../metasmith-libraries`` (dev layout).
    3. Auto-bootstrap clone into
       ``tests/e2e_agentic/.cache/MetasmithLibraries`` (gitignored).

    No ``git pull`` on subsequent runs — the cache clone is a one-shot
    bootstrap. To update, delete the cache dir (or set the env var to a
    fresh checkout).
    """
    explicit = os.environ.get("METASMITH_LIBRARIES_ROOT")
    if explicit:
        root = _resolve_lib_root(Path(explicit).expanduser().resolve())
        if root is None:
            raise RuntimeError(
                f"METASMITH_LIBRARIES_ROOT={explicit} does not contain "
                f"{_MLIB_LAYOUT_MARKER} (checked both root and root/main)"
            )
        return root
    # Walk up from the project root checking each level for a sibling
    # checkout. Multi-worktree layouts (e.g. `projects/metasmith/dev/`)
    # place the real sibling project two parents up rather than one.
    pr = _project_root()
    for ancestor in (pr.parent, pr.parent.parent):
        root = _resolve_lib_root(ancestor / "metasmith-libraries")
        if root is not None:
            return root
    cache_root = _resolve_lib_root(_metasmith_libraries_cache_dir())
    if cache_root is not None:
        return cache_root
    cache = _metasmith_libraries_cache_dir()
    cache.parent.mkdir(parents=True, exist_ok=True)
    if cache.exists():
        shutil.rmtree(cache)
    r = subprocess.run(
        ["git", "clone", "--depth=1", _METASMITH_LIBRARIES_REPO, str(cache)],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(
            f"auto-clone of MetasmithLibraries failed (exit {r.returncode}):\n"
            f"  stdout: {r.stdout[-500:]}\n  stderr: {r.stderr[-500:]}\n"
            f"  set METASMITH_LIBRARIES_ROOT to point at an existing checkout, "
            f"or clone manually into {cache.parent.parent}"
        )
    root = _resolve_lib_root(cache)
    if root is None:
        raise RuntimeError(
            f"auto-cloned MetasmithLibraries at {cache} is missing {_MLIB_LAYOUT_MARKER}"
        )
    return root


def stage_real_libraries(layout: SandboxLayout) -> Path:
    """Clone MetasmithLibraries into ``<sandbox>/MetasmithLibraries``.

    Uses the resolved source checkout (env var, sibling dir, or auto-clone
    cache) as the local origin for a ``git clone --depth=1`` into the
    sandbox. The sandbox carries its own .git dir, so test runs preserve
    provenance (HEAD SHA visible via ``git -C <sandbox>/MetasmithLibraries
    rev-parse HEAD``).

    The tutorials expect ``MLIB = <sandbox>/MetasmithLibraries`` —
    matching the canonical layout the docs reference at
    ``docs/source/setup/tutorials.rst``.
    """
    source = _metasmith_libraries_root()
    dest = layout.root / "MetasmithLibraries"
    if dest.exists():
        shutil.rmtree(dest)
    r = subprocess.run(
        ["git", "clone", "--depth=1", str(source), str(dest)],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(
            f"sandbox clone of MetasmithLibraries from {source} failed "
            f"(exit {r.returncode}):\n  stdout: {r.stdout[-500:]}\n"
            f"  stderr: {r.stderr[-500:]}"
        )
    if not _validate_mlib(dest):
        raise RuntimeError(
            f"staged MetasmithLibraries at {dest} is missing {_MLIB_LAYOUT_MARKER}"
        )
    return dest


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
