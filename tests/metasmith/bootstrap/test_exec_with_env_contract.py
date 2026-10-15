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
import metasmith.models.libraries.execution as libraries_mod


FIXED_ID = "TESTID000000"


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


def _build_context(tmp_path: Path, runtime: Runtime, image_dep: Dependency):
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


# --------------------------------------------------------- one call, either runtime
#
# The body says what to run and in what environment. Everything below is the engine
# deciding how, from the same call: which key of the env resource to read, whether
# there is a mount namespace to bind into, and what launches the shell.


def test_container_runtime_issues_a_run_command(tmp_path, _bounce_ready):
    image_dep = _dep("image")
    ctx = _build_context(tmp_path, Runtime.DOCKER, image_dep)
    shell: RecordingShell = ctx.external_shell  # type: ignore[assignment]

    ctx.ExecWithEnv(env=image_dep, cmd="echo hello")

    assert len(shell.calls) == 1
    run_cmd = shell.calls[0]

    expected_container = ctx.GetContainerModel(image_dep)
    expected_run = expected_container.MakeRunCommand(local=False)
    assert run_cmd.startswith(expected_run + " bash ")

    assert "/ws/_metasmith/.bounce." in run_cmd
    assert run_cmd.endswith(run_cmd.split(" bash ")[-1])

    assert "/hpc/home/containers/tool.sif" in run_cmd
    assert run_cmd.startswith("docker run ")


def test_the_same_call_runs_under_mamba_without_a_container(tmp_path, _bounce_ready):
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    shell: RecordingShell = ctx.external_shell  # type: ignore[assignment]

    ctx.ExecWithEnv(env=env_dep, cmd="echo hello")

    assert len(shell.calls) == 1
    assert shell.calls[0].startswith("mamba run -n toolenv bash ")
    assert "echo hello" in _bounce_text(tmp_path)


def test_the_runtime_picks_which_key_of_the_env_resource_is_read(tmp_path, _bounce_ready):
    # `tool.env` carries both; MAMBA reads `conda:` and a container runtime reads
    # `container:`. The body names neither.
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    assert ctx.GetContainerModel(env_dep).image == "toolenv"

    ctx._environment = Runtime.DOCKER
    ctx.__post_init__()
    assert ctx.GetContainerModel(env_dep).image == "docker://example/tool:1"


def test_binds_are_refused_where_there_is_no_boundary_to_cross(tmp_path, _bounce_ready):
    # The honest refusal: a mount is meaningless without a mount namespace, and
    # silently dropping it would run the command against paths that are not there.
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    with pytest.raises(AssertionError, match="binds are meaningless"):
        ctx.ExecWithEnv(env=env_dep, cmd="echo hello", binds=[("/db", "/db")])


def test_exports_are_prepended_to_the_bounce_script(tmp_path, _bounce_ready):
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    ctx.ExecWithEnv(
        env=env_dep, cmd="gtdbtk classify_wf",
        exports={"GTDBTK_DATA_PATH": Path("/ref/gtdb")},
    )
    body = _bounce_text(tmp_path)
    assert "export GTDBTK_DATA_PATH=/ref/gtdb" in body
    assert body.index("export GTDBTK_DATA_PATH") < body.index("gtdbtk classify_wf")


def test_exports_with_spaces_are_quoted(tmp_path, _bounce_ready):
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    ctx.ExecWithEnv(env=env_dep, cmd="run", exports={"REF": "/a path/with spaces"})
    assert "export REF='/a path/with spaces'" in _bounce_text(tmp_path)


@pytest.mark.parametrize("name", ["PATH", "HOME", "LD_LIBRARY_PATH", "TMPDIR", "PWD"])
def test_reserved_exports_are_refused(tmp_path, _bounce_ready, name):
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    with pytest.raises(AssertionError, match="reserved"):
        ctx.ExecWithEnv(env=env_dep, cmd="run", exports={name: "/x"})


def test_export_names_must_be_shell_identifiers(tmp_path, _bounce_ready):
    env_dep = _dep("env")
    ctx = _mamba_context(tmp_path, env_dep)
    with pytest.raises(AssertionError, match="valid shell identifier"):
        ctx.ExecWithEnv(env=env_dep, cmd="run", exports={"a-b": "1"})
