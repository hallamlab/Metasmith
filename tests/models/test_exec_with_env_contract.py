"""Contract characterization of `ExecutionContext.ExecWithEnv`.

A container is a filesystem layout with an entrypoint; a conda env is a package
set on PATH. `ExecWithEnv()` makes a transform declare each world it supports
and lets metasmith pick, replacing the single `ExecWithContainer` that silently
dropped its binds under a runtime with no mount namespace.

Two contracts are pinned here:

  1. The shell contract, inherited from `ExecWithContainer`: the command run on
     the shell is `<MakeRunCommand(local=...)> bash <bounce>`, and the embedded
     run-command matches the Environment the context builds. DOCKER is used so
     `GetLocalPath()` is None and the cache-probe `Exec` is skipped, keeping the
     recorded call sequence to exactly one (the run).

  2. The dispatch contract: exactly the arm matching the agent's runtime runs,
     the other is recorded and skipped, and a chain with no matching arm is
     reported so the framework can fail the step rather than let it no-op.
"""

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
    protocol expects so the arm doesn't sys.exit on a missing code."""

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
        _environment=runtime,
    )


def _mamba_context(tmp_path: Path, env_dep: Dependency, env_name: str = "toolenv"):
    """A text resource resolves through ResolveEnvImage; a mapping with a
    `conda:` key is what the migrated env declarations look like."""
    decl = tmp_path / "tool.env"
    decl.write_text(f"container: docker://example/tool:1\nconda: {env_name}\n")
    cp = ContextPath(local=decl, external=decl, container=decl)
    cd = ContextData(input_group=[cp], endpoint=Endpoint(properties={"env"}), type_name="env")
    return ExecutionContext(
        _inputs=[{env_dep: cd}],
        _get_output_paths=lambda *a: None,
        external_shell=RecordingShell(),
        external_cwd=tmp_path,
        external_agent_home=tmp_path / "msm_home",
        _environment=Runtime.MAMBA,
    )


@pytest.fixture
def _bounce_ready(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "_metasmith").mkdir()
    monkeypatch.setattr(libraries_mod, "GenerateId", lambda *a, **k: FIXED_ID)


def _bounce_scripts(tmp_path: Path) -> list[Path]:
    return sorted((tmp_path / "_metasmith").glob(".bounce.*"))


def _bounce_text(tmp_path: Path) -> str:
    scripts = _bounce_scripts(tmp_path)
    assert len(scripts) == 1, scripts
    return scripts[0].read_text()


# ---------------------------------------------------------------- shell contract


def test_container_arm_issues_run_command(tmp_path, _bounce_ready):
    image_dep = _dep("image")
    ctx = _build_context(tmp_path, Runtime.DOCKER, image_dep)
    shell: RecordingShell = ctx.external_shell  # type: ignore[assignment]

    ctx.ExecWithEnv().ifContainerDo(env=image_dep, cmd="echo hello")

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


# ------------------------------------------------------------- dispatch contract


def test_container_runtime_runs_only_the_container_arm(tmp_path, _bounce_ready):
    image_dep = _dep("image")
    ctx = _build_context(tmp_path, Runtime.DOCKER, image_dep)
    shell: RecordingShell = ctx.external_shell  # type: ignore[assignment]

    chain = ctx.ExecWithEnv() \
        .ifContainerDo(env=image_dep, cmd="echo container") \
        .ifVirtualEnvDo(env=image_dep, cmd="echo venv")

    assert chain.matched == "ifContainerDo"
    assert chain.declared == ["ifContainerDo", "ifVirtualEnvDo"]
    assert len(shell.calls) == 1
    # The skipped arm's command was never even written out.
    assert "echo venv" not in _bounce_text(tmp_path)
    assert ctx.UnmatchedEnvDispatches() == []


def test_mamba_runtime_runs_only_the_virtual_env_arm(tmp_path, _bounce_ready):
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    shell: RecordingShell = ctx.external_shell  # type: ignore[assignment]

    chain = ctx.ExecWithEnv() \
        .ifContainerDo(env=env_dep, cmd="echo container", binds=[("/db", "/db")]) \
        .ifVirtualEnvDo(env=env_dep, cmd="echo venv")

    assert chain.matched == "ifVirtualEnvDo"
    # mamba has no image to cache-probe, so the venv arm is the single call.
    assert len(shell.calls) == 1
    assert shell.calls[0].startswith("mamba run -n toolenv bash ")
    assert "echo venv" in _bounce_text(tmp_path)
    assert ctx.UnmatchedEnvDispatches() == []


def test_arm_order_does_not_change_which_arm_runs(tmp_path, _bounce_ready):
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    chain = ctx.ExecWithEnv() \
        .ifVirtualEnvDo(env=env_dep, cmd="echo venv") \
        .ifContainerDo(env=env_dep, cmd="echo container")
    assert chain.matched == "ifVirtualEnvDo"
    assert chain.declared == ["ifVirtualEnvDo", "ifContainerDo"]


def test_container_only_chain_on_mamba_is_reported_unmatched(tmp_path, _bounce_ready):
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    shell: RecordingShell = ctx.external_shell  # type: ignore[assignment]

    chain = ctx.ExecWithEnv().ifContainerDo(env=env_dep, cmd="echo container")

    # Nothing ran, and the framework can see that it didn't.
    assert chain.matched is None
    assert shell.calls == []
    unmatched = ctx.UnmatchedEnvDispatches()
    assert unmatched == [chain]
    assert unmatched[0].declared == ["ifContainerDo"]


def test_venv_only_chain_on_docker_is_reported_unmatched(tmp_path, _bounce_ready):
    image_dep = _dep("image")
    ctx = _build_context(tmp_path, Runtime.DOCKER, image_dep)
    chain = ctx.ExecWithEnv().ifVirtualEnvDo(env=image_dep, cmd="echo venv")
    assert chain.matched is None
    assert ctx.UnmatchedEnvDispatches() == [chain]


def test_empty_chain_is_reported_unmatched(tmp_path, _bounce_ready):
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    chain = ctx.ExecWithEnv()
    assert ctx.UnmatchedEnvDispatches() == [chain]
    assert chain.declared == []


def test_virtual_env_arm_rejects_binds(tmp_path, _bounce_ready):
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    with pytest.raises(TypeError):
        ctx.ExecWithEnv().ifVirtualEnvDo(  # type: ignore[call-arg]
            env=env_dep, cmd="echo venv", binds=[("/db", "/db")],
        )


# ---------------------------------------------------------------------- exports


def test_exports_are_prepended_to_the_bounce_script(tmp_path, _bounce_ready):
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    ctx.ExecWithEnv().ifVirtualEnvDo(
        env=env_dep, cmd="gtdbtk classify_wf",
        exports={"GTDBTK_DATA_PATH": Path("/ref/gtdb")},
    )
    body = _bounce_text(tmp_path)
    assert "export GTDBTK_DATA_PATH=/ref/gtdb" in body
    # ...before the command, or the tool never sees it.
    assert body.index("export GTDBTK_DATA_PATH") < body.index("gtdbtk classify_wf")


def test_exports_with_spaces_are_quoted(tmp_path, _bounce_ready):
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    ctx.ExecWithEnv().ifVirtualEnvDo(
        env=env_dep, cmd="run", exports={"REF": "/a path/with spaces"},
    )
    assert "export REF='/a path/with spaces'" in _bounce_text(tmp_path)


@pytest.mark.parametrize("name", ["PATH", "HOME", "LD_LIBRARY_PATH", "TMPDIR", "PWD"])
def test_reserved_exports_are_refused(tmp_path, _bounce_ready, name):
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    with pytest.raises(AssertionError, match="reserved"):
        ctx.ExecWithEnv().ifVirtualEnvDo(env=env_dep, cmd="run", exports={name: "/x"})


def test_export_names_must_be_shell_identifiers(tmp_path, _bounce_ready):
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    with pytest.raises(AssertionError, match="valid shell identifier"):
        ctx.ExecWithEnv().ifVirtualEnvDo(env=env_dep, cmd="run", exports={"a-b": "1"})
