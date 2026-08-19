from pathlib import Path

from metasmith.env import ContainerDef, Environment, Runtime


def _env(runtime, *, binds=None, **kw) -> Environment:
    return Environment(
        image="docker://example/tool:1.0", runtime=runtime,
        container=ContainerDef(workdir=Path("/ws"), binds=list(binds or [])),
        **kw,
    )


def _docker(**kw) -> Environment:
    return _env(Runtime.DOCKER, **kw)


def _apptainer(**kw) -> Environment:
    return _env(Runtime.APPTAINER, **kw)


class TestExtraArgsRendering:
    def test_default_is_empty_and_omitted(self):
        cmd = _docker().MakeRunCommand()
        assert cmd.endswith("example/tool:1.0")
        assert _docker().extra_args == []

    def test_docker_extra_args_appear_before_image(self):
        cmd = _docker(extra_args=["--gpus", "all", "--shm-size=8g", "-e", "FOO=bar"]).MakeRunCommand()
        assert "--gpus all" in cmd
        assert "--shm-size=8g" in cmd
        assert "-e FOO=bar" in cmd
        assert cmd.index("--gpus all") < cmd.index("example/tool:1.0")
        assert cmd.index("-e FOO=bar") < cmd.index("example/tool:1.0")

    def test_apptainer_extra_args_appear_before_image(self):
        cmd = _apptainer(extra_args=["--nv", "--env", "FOO=bar"]).MakeRunCommand()
        assert "--nv" in cmd
        assert "--env FOO=bar" in cmd
        assert cmd.index("--nv") < cmd.index("docker://example/tool:1.0")

    def test_extra_args_render_after_binds(self):
        cmd = _docker(
            binds=[(Path("/host/data"), Path("/data"))],
            extra_args=["--network=none"],
        ).MakeRunCommand()
        assert 'source="/host/data"' in cmd
        assert cmd.index('source="/host/data"') < cmd.index("--network=none")
        assert cmd.index("--network=host") < cmd.index("--network=none")
