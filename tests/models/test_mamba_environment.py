"""The mamba executor + native mode (T4): run tools with no container and
no relay.

A mamba/native Environment crosses no container boundary, so:
  - needs_relay is False (no daemon, no bounce)
  - binds collapse to nothing (shared host filesystem)
  - the run-command is `mamba run -n <env>` (or empty for native)
  - the virtual-env arm issues exactly one shell call — no image cache probe —
    on a local shell, and the bounce script cd's to the real host cwd (not
    the container /ws), i.e. paths are identity.
"""

import os
from pathlib import Path

import pytest

from metasmith.coms.terminals import ShellResult
from metasmith.env import ContainerDef, Environment, Runtime
from metasmith.models.libraries import ContextData, ContextPath, ExecutionContext
from metasmith.models.solver import Dependency, Endpoint
import metasmith.models.libraries as libraries_mod


FIXED_ID = "MAMBAID00000"


class RecordingShell:
    def __init__(self):
        self.calls: list[str] = []

    def Exec(self, cmd: str, timeout=None, history: bool = False) -> ShellResult:
        self.calls.append(cmd)
        Path(f"exitcode.{FIXED_ID}").write_text("0\n")
        return ShellResult(out=[], err=[])

    def ExecAsync(self, cmd: str) -> str:
        return "key"

    def AwaitDone(self, timeout=None, _key=None):
        pass


def _dep(name: str) -> Dependency:
    return Dependency(properties={name}, parents=set())


# --------------------------------------------------------------------------
# Environment-level behavior
# --------------------------------------------------------------------------

class TestMambaEnvironment:
    def test_no_relay(self):
        assert Environment(image="checkm", runtime=Runtime.MAMBA).needs_relay is False

    def test_run_command_is_mamba_run(self):
        env = Environment(image="checkm-1.2.0", runtime=Runtime.MAMBA)
        assert env.MakeRunCommand(local=False) == "mamba run -n checkm-1.2.0"
        # local=True is meaningless without a cache; still no container.
        assert env.MakeRunCommand(local=True) == "mamba run -n checkm-1.2.0"

    def test_no_binds_no_cache_no_pull(self):
        env = Environment(image="checkm", runtime=Runtime.MAMBA, container=ContainerDef(binds=[(Path("/a"), Path("/b"))]))
        assert env.MakeBindsParam() == ""
        assert env.GetLocalPath() is None
        assert env.GetSandboxPath() is None
        assert env.MakePullCommand() == ""
        assert env.ProvisionSteps(agent_home=Path("/h")) == []

    def test_wrapper_prefix(self):
        assert Environment(image="checkm", runtime=Runtime.MAMBA).MakeWrapperPrefix() == "mamba run -n checkm"

    def test_bootstrap_has_no_relay(self):
        env = Environment(image="checkm", runtime=Runtime.MAMBA)
        bs = env.RenderBootstrap(agent_home=Path("/h"), run_command="", run_binds="", dev_src="x", dev_target="t", bind_file="b")
        assert "msm_relay" not in bs
        assert "bouncing to external" not in bs
        assert "run_container" not in bs
        assert "metasmith api execute_transform" in bs


class TestNativeMode:
    def test_native_overrides_runtime_to_no_relay(self):
        # native composes with a runtime but never crosses a boundary.
        env = Environment(image="x", runtime=Runtime.APPTAINER, native=True)
        assert env.needs_relay is False
        assert env.MakeRunCommand() == ""
        assert env.MakeWrapperPrefix() == ""
        assert env.MakeBindsParam() == ""

    def test_native_is_not_a_runtime_member(self):
        assert not hasattr(Runtime, "NATIVE")


# --------------------------------------------------------------------------
# ExecWithEnv's virtual-env arm on the mamba path — no relay, no probe, identity cwd
# --------------------------------------------------------------------------

def test_mamba_exec_no_relay_identity_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "_metasmith").mkdir()
    monkeypatch.setattr(libraries_mod, "GenerateId", lambda *a, **k: FIXED_ID)

    # The image dep for a conda env carries the env name as text.
    env_file = tmp_path / "tool.condaenv"
    env_file.write_text("checkm-1.2.0\n")
    image_dep = _dep("env")
    cp = ContextPath(local=env_file, external=env_file, container=env_file)
    cd = ContextData(input_group=[cp], endpoint=Endpoint(properties={"env"}), type_name="env")

    real_cwd = tmp_path  # identity: external cwd == host cwd
    ctx = ExecutionContext(
        _inputs=[{image_dep: cd}],
        _get_output_paths=lambda *a: None,
        external_shell=RecordingShell(),
        external_cwd=real_cwd,
        external_agent_home=tmp_path / "msm_home",
        _environment=Runtime.MAMBA,
    )
    shell: RecordingShell = ctx.external_shell  # type: ignore[assignment]

    # The model built for a mamba dep is identity: no binds, workdir == cwd.
    model = ctx.GetContainerModel(image_dep)
    assert model.runtime == Runtime.MAMBA
    assert model.container.binds == []
    assert Path(model.container.workdir) == real_cwd

    ctx.ExecWithEnv().ifVirtualEnvDo(image_dep, "checkm version")

    # Exactly one shell call — no image cache probe (mamba has no local image).
    assert len(shell.calls) == 1
    run_cmd = shell.calls[0]
    assert run_cmd.startswith("mamba run -n checkm-1.2.0 bash ")
    assert "docker" not in run_cmd and "apptainer" not in run_cmd
    assert "msm_relay" not in run_cmd

    # The bounce script cd's to the real host cwd, not the container /ws.
    bounce = next((tmp_path / "_metasmith").glob(".bounce.*"))
    body = bounce.read_text()
    assert f"cd {real_cwd}" in body
    assert "cd /ws" not in body
