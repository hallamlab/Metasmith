"""End-to-end proof that the mamba executor runs a real tool with no relay
and no container.

Unlike the unit tests (which assert the *shape* of the emitted command), this
drives `ExecutionContext.ExecWithContainer` against a real local shell and a
real conda env (`mamba run -n <env>`), then checks the tool actually ran by
inspecting its output file. This is the load-bearing claim of the whole
refactor: a transform's tool executes inside a conda env, in-process on the
host, with the relay entirely out of the picture.

Requires a working `mamba` and the named env on PATH; skipped otherwise.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from metasmith.coms.terminals import LiveShell
from metasmith.env import Runtime
from metasmith.models.libraries import ContextData, ContextPath, ExecutionContext
from metasmith.models.solver import Dependency, Endpoint


ENV_NAME = "msm"  # the dev conda env this repo runs under


def _mamba_env_available(name: str) -> bool:
    mamba = shutil.which("mamba") or shutil.which("micromamba") or shutil.which("conda")
    if not mamba:
        return False
    try:
        out = subprocess.run([mamba, "env", "list"], capture_output=True, text=True, timeout=30)
    except Exception:
        return False
    return any(line.split() and line.split()[0] == name for line in out.stdout.splitlines())


pytestmark = pytest.mark.skipif(
    not _mamba_env_available(ENV_NAME),
    reason=f"mamba env [{ENV_NAME}] not available",
)


def _dep(name: str) -> Dependency:
    return Dependency(properties={name}, parents=set())


def test_mamba_runs_real_tool_no_relay(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "_metasmith").mkdir()

    # The conda-env dependency: a file whose content is the env name.
    env_file = tmp_path / "tool.condaenv"
    env_file.write_text(f"{ENV_NAME}\n")
    env_dep = _dep("env")
    env_cp = ContextPath(local=env_file, external=env_file, container=env_file)
    env_cd = ContextData(input_group=[env_cp], endpoint=Endpoint(properties={"env"}), type_name="env")

    src = tmp_path / "in.txt"
    src.write_text("hello from a conda-env tool\n")
    dst = tmp_path / "out.txt"

    ctx = ExecutionContext(
        _inputs=[{env_dep: env_cd}],
        _get_output_paths=lambda *a: None,
        external_shell=LiveShell(),
        external_cwd=tmp_path,
        external_agent_home=tmp_path / "msm_home",
        _environment=Runtime.MAMBA,
    )

    # Sanity: the model is identity (no container boundary).
    model = ctx.GetContainerModel(env_dep)
    assert model.runtime == Runtime.MAMBA
    assert model.needs_relay is False
    assert model.binds == []

    with ctx.external_shell as shell:
        ctx.external_shell = shell  # ensure the live shell is the running one
        # `cp` is a real tool resolved through `mamba run -n <env>`.
        ctx.ExecWithContainer(env_dep, f"cp {src} {dst}")

    # The tool actually ran inside the conda env: the output exists and matches.
    assert dst.exists(), "mamba-run tool did not produce its output"
    assert dst.read_text() == src.read_text()

    # No relay artifacts were created on this path.
    assert not (tmp_path / "_metasmith" / "relay").exists()
