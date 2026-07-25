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
    assert model.container.binds == []

    with ctx.external_shell as shell:
        ctx.external_shell = shell  # ensure the live shell is the running one
        # `cp` is a real tool resolved through `mamba run -n <env>`.
        ctx.ExecWithContainer(env_dep, f"cp {src} {dst}")

    # The tool actually ran inside the conda env: the output exists and matches.
    assert dst.exists(), "mamba-run tool did not produce its output"
    assert dst.read_text() == src.read_text()

    # No relay artifacts were created on this path.
    assert not (tmp_path / "_metasmith" / "relay").exists()


def test_mamba_inherits_the_hosts_gpu(tmp_path, monkeypatch):
    """mamba/native run on the host, so a GPU is inherited, not passed through.

    The container runtimes need a flag (`--nv`, `--gpus all`) to cross the
    boundary; mamba has no boundary, so the correct behaviour is to add nothing
    and still see whatever the host has. This asserts both halves against the
    real machine: detection agrees with `nvidia-smi`, and the emitted command
    carries no GPU flag either way.

    Skipped where there is no GPU -- the no-flag half is still covered by the
    unit tests; this is the real-hardware half.
    """
    import shutil as _shutil
    import subprocess as _sp

    if not _shutil.which("nvidia-smi"):
        pytest.skip("no nvidia-smi on this host")
    listing = _sp.run(["nvidia-smi", "-L"], capture_output=True, text=True)
    if listing.returncode != 0 or "GPU " not in listing.stdout:
        pytest.skip("nvidia-smi present but reports no device")
    expected = len([l for l in listing.stdout.splitlines() if l.startswith("GPU ")])

    monkeypatch.chdir(tmp_path)
    (tmp_path / "_metasmith").mkdir()

    env_file = tmp_path / "tool.condaenv"
    env_file.write_text(f"{ENV_NAME}\n")
    env_dep = _dep("env")
    env_cp = ContextPath(local=env_file, external=env_file, container=env_file)
    env_cd = ContextData(input_group=[env_cp], endpoint=Endpoint(properties={"env"}), type_name="env")

    ctx = ExecutionContext(
        _inputs=[{env_dep: env_cd}],
        _get_output_paths=lambda *a: None,
        external_shell=LiveShell(),
        external_cwd=tmp_path,
        external_agent_home=tmp_path / "msm_home",
        _environment=Runtime.MAMBA,
        # what the step declared at stage time
        params={"gpus": {"gpus": "required", "gpu_memory_gb": 4.0}},
    )

    with ctx.external_shell as shell:
        ctx.external_shell = shell
        found = ctx.DetectGpus()
        # the framework's own detection agrees with the host
        assert len(found) == expected, f"detected {found}, nvidia-smi reports {expected}"
        assert all(f.value_gb > 0 for f in found)

        # a declaring step gets NO wrapper flags under mamba -- the device is
        # simply inherited
        model = ctx.GetContainerModel(env_dep)
        assert model.MakeGpuArgs() == []
        assert model.extra_args == []
        cmd = model.MakeRunCommand()
        assert "--nv" not in cmd and "--gpus" not in cmd
        assert cmd == f"mamba run -n {ENV_NAME}"

        # and the tool, run through the conda env, really sees the device
        out = tmp_path / "gpu.txt"
        ctx.ExecWithContainer(env_dep, f"nvidia-smi -L > {out}")
    assert out.exists()
    assert "GPU 0" in out.read_text(), out.read_text()
