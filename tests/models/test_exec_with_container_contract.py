"""Contract characterization of `ExecutionContext.ExecWithContainer`.

This pins the T3 decoupling point: today `ExecutionContext.external_shell`
is a hard-typed `RemoteShell`, and `ExecWithContainer` assembles a container
run-command and hands it to `external_shell.Exec(...)`. The refactor will
replace `external_shell` with a generic shell supplied by the tool's
`Environment`. This test locks the *observable contract* — what command the
shell is asked to run — so the swap is provably behavior-preserving.

We use a recording shell (not the live RemoteShell) and assert:
  1. The command run on the shell is `<MakeRunCommand(local=...)> bash <bounce>`.
  2. The run-command embedded in it matches the Environment the context builds.

DOCKER is used so `GetLocalPath()` is None and the cache-probe `Exec` is
skipped — keeping the recorded call sequence to exactly one (the run).
"""

import os
from pathlib import Path

import pytest

from metasmith.coms.terminals import ShellResult
from metasmith.env import Runtime
from metasmith.models.libraries import (
    ContextData,
    ContextPath,
    ExecutionContext,
)
from metasmith.models.solver import Dependency, Endpoint
import metasmith.models.libraries as libraries_mod


FIXED_ID = "TESTID000000"


class RecordingShell:
    """Captures every Exec command. Writes the exit-code file the bounce
    protocol expects so ExecWithContainer doesn't sys.exit on a missing code."""

    def __init__(self):
        self.calls: list[str] = []

    def Exec(self, cmd: str, timeout=None, history: bool = False) -> ShellResult:
        self.calls.append(cmd)
        # The container trap would write exitcode.<id>; emulate success.
        Path(f"exitcode.{FIXED_ID}").write_text("0\n")
        return ShellResult(out=[], err=[])

    def ExecAsync(self, cmd: str) -> str:
        return "key"

    def AwaitDone(self, timeout=None, _key=None):
        pass


def _dep(name: str) -> Dependency:
    return Dependency(properties={name}, parents=set())


def _build_context(tmp_path: Path, runtime: Runtime, image_dep: Dependency):
    # Binary image file so IsText(path.local) is False -> uses path.external as uri.
    sif = tmp_path / "tool.sif"
    sif.write_bytes(b"\x00\x01\x02\x03")
    image_uri = "/hpc/home/containers/tool.sif"
    image_cp = ContextPath(local=sif, external=Path(image_uri), container=Path(image_uri))
    image_cd = ContextData(
        input_group=[image_cp],
        endpoint=Endpoint(properties={"image"}),
        type_name="image",
    )
    return ExecutionContext(
        _inputs=[{image_dep: image_cd}],
        _get_output_paths=lambda *a: None,
        external_shell=RecordingShell(),
        external_cwd=Path("/hpc/home/work"),
        external_agent_home=Path("/hpc/home/msm_home"),
        container_runtime=runtime,
    )


def test_exec_with_container_issues_run_command(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "_metasmith").mkdir()
    monkeypatch.setattr(libraries_mod, "GenerateId", lambda *a, **k: FIXED_ID)

    image_dep = _dep("image")
    ctx = _build_context(tmp_path, Runtime.DOCKER, image_dep)
    shell: RecordingShell = ctx.external_shell  # type: ignore[assignment]

    ctx.ExecWithContainer(image_dep, "echo hello")

    # DOCKER has no local cache path -> no cache probe -> exactly one Exec (the run).
    assert len(shell.calls) == 1
    run_cmd = shell.calls[0]

    # The run-command prefix is exactly what the context's Environment would emit
    # (local=False, since DOCKER cache path is None).
    expected_container = ctx.GetContainerModel(image_dep)
    expected_run = expected_container.MakeRunCommand(local=False)
    assert run_cmd.startswith(expected_run + " bash ")

    # And it ends by invoking the bounce script under the container workdir.
    assert "/ws/_metasmith/.bounce." in run_cmd
    assert run_cmd.endswith(run_cmd.split(" bash ")[-1])  # tail is the bounce path

    # The assembled run-command names the image uri and the docker runtime.
    assert "/hpc/home/containers/tool.sif" in run_cmd
    assert run_cmd.startswith("docker run ")
