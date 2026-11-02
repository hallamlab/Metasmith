import os
import shutil
import subprocess
from pathlib import Path

import pytest

from metasmith.coms.terminals import LiveShell
from metasmith.env import Runtime
from metasmith.models.libraries import ContextData, ContextPath, ExecutionContext
from metasmith.models.solver import Dependency, Endpoint


ENV_NAME = "msm"


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

    model = ctx.GetContainerModel(env_dep)
    assert model.runtime == Runtime.MAMBA
    assert model.needs_relay is False
    assert model.container.binds == []

    with ctx.external_shell as shell:
        ctx.external_shell = shell
        ctx.ExecWithEnv(env_dep, f"cp {src} {dst}")

    assert dst.exists(), "mamba-run tool did not produce its output"
    assert dst.read_text() == src.read_text()

    assert not (tmp_path / "_metasmith" / "relay").exists()


def test_mamba_inherits_the_hosts_gpu(tmp_path, monkeypatch):
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
        params={"gpus": {"gpus": "required", "gpu_memory_gb": 4.0}},
    )

    with ctx.external_shell as shell:
        ctx.external_shell = shell
        found = ctx.DetectGpus()
        assert len(found) == expected, f"detected {found}, nvidia-smi reports {expected}"
        assert all(f.value_gb > 0 for f in found)

        model = ctx.GetContainerModel(env_dep)
        assert model.MakeGpuArgs() == []
        assert model.extra_args == []
        cmd = model.MakeRunCommand()
        assert "--nv" not in cmd and "--gpus" not in cmd
        assert cmd == f"mamba run -n {ENV_NAME}"

        out = tmp_path / "gpu.txt"
        ctx.ExecWithEnv(env_dep, f"nvidia-smi -L > {out}")
    assert out.exists()
    assert "GPU 0" in out.read_text(), out.read_text()
