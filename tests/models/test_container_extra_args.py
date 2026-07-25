"""Tests for the per-call `args=` plumbing on container runs.

Covers `Environment.extra_args` rendering in `MakeRunCommand` for both Docker
and Apptainer runtimes, and confirms the default is empty (so transforms
that don't pass `args=` see no change in the emitted command).
"""

from pathlib import Path

from metasmith.env import Environment, Runtime


def _docker(**kw) -> Environment:
    return Environment(
        image="docker://example/tool:1.0",
        workdir=Path("/ws"),
        runtime=Runtime.DOCKER,
        **kw,
    )


def _apptainer(**kw) -> Environment:
    return Environment(
        image="docker://example/tool:1.0",
        workdir=Path("/ws"),
        runtime=Runtime.APPTAINER,
        **kw,
    )


class TestExtraArgsRendering:
    def test_default_is_empty_and_omitted(self):
        """No `args=` -> command identical to the prior shape (no extras emitted)."""
        cmd = _docker().MakeRunCommand()
        # Image still appears, no stray tokens after the default flags.
        assert cmd.endswith("example/tool:1.0")
        # Sanity: the field defaults to a real empty list, not None.
        assert _docker().extra_args == []

    def test_docker_extra_args_appear_before_image(self):
        cmd = _docker(extra_args=["--gpus", "all", "--shm-size=8g", "-e", "FOO=bar"]).MakeRunCommand()
        assert "--gpus all" in cmd
        assert "--shm-size=8g" in cmd
        assert "-e FOO=bar" in cmd
        # Image positional must come AFTER the extra args.
        assert cmd.index("--gpus all") < cmd.index("example/tool:1.0")
        assert cmd.index("-e FOO=bar") < cmd.index("example/tool:1.0")

    def test_apptainer_extra_args_appear_before_image(self):
        cmd = _apptainer(extra_args=["--nv", "--env", "FOO=bar"]).MakeRunCommand()
        assert "--nv" in cmd
        assert "--env FOO=bar" in cmd
        assert cmd.index("--nv") < cmd.index("docker://example/tool:1.0")

    def test_extra_args_render_after_binds(self):
        """Caller's args follow binds, so a later flag wins over an earlier default."""
        cmd = _docker(
            binds=[(Path("/host/data"), Path("/data"))],
            extra_args=["--network=none"],
        ).MakeRunCommand()
        # Bind rendered.
        assert 'source="/host/data"' in cmd
        # Bind precedes the extra arg.
        assert cmd.index('source="/host/data"') < cmd.index("--network=none")
        # And the caller's --network=none comes after the framework's --network=host default.
        assert cmd.index("--network=host") < cmd.index("--network=none")
