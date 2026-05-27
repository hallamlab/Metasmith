"""Tests for the SIF→sandbox unpack helpers on Container.

When the host's apptainer ships no setuid `starter-suid` (e.g. the
conda-forge build), apptainer falls back to squashfuse_ll for SIF mounts,
which deadlocks under msm_relay's fork chain on WSL2 (Bug E.2). The
runtime workaround is to unpack the SIF to a sandbox directory once at
deploy time; `MakeRunCommand(local=True)` then picks the sandbox over the
SIF via a shell-level conditional.

These tests pin the helper shapes and the conditional substitution. They
don't spawn apptainer — only inspect the emitted shell text.
"""

from pathlib import Path

from metasmith.coms.containers import Container, ContainerRuntime


def _apptainer(**kw) -> Container:
    return Container(
        image="docker://quay.io/example/tool:1.0",
        workdir=Path("/ws"),
        runtime=ContainerRuntime.APPTAINER,
        **kw,
    )


def _docker(**kw) -> Container:
    return Container(
        image="docker://quay.io/example/tool:1.0",
        workdir=Path("/ws"),
        runtime=ContainerRuntime.DOCKER,
        **kw,
    )


class TestCachePaths:
    def test_sandbox_is_sibling_of_sif(self):
        c = _apptainer(container_cache=Path("/cache"))
        sif = c.GetLocalPath()
        sandbox = c.GetSandboxPath()
        assert sif is not None and sandbox is not None
        assert sif.parent == sandbox.parent == Path("/cache")
        # Same stem (sanitized image name), different suffix.
        assert sif.stem == sandbox.stem
        assert sif.suffix == ".sif"
        assert sandbox.suffix == ".sandbox"

    def test_docker_has_no_sandbox(self):
        # Sandbox is an apptainer-only concept; docker runtime returns None
        # the same way GetLocalPath does today.
        assert _docker().GetSandboxPath() is None


class TestProbe:
    def test_probe_checks_starter_suid_setuid(self):
        probe = _apptainer().MakeNeedsSandboxProbe()
        # The probe must look at `starter-suid` specifically; that's the
        # missing piece in conda-forge apptainer that drives the wedge.
        assert "starter-suid" in probe
        # The setuid bit test `[ -u ... ]` is the right check; presence
        # alone is not enough (a non-setuid copy still wouldn't work).
        assert "[ -u" in probe
        # Sentinel string the deploy step matches against.
        assert "needs-sandbox" in probe


class TestBuildCommand:
    def test_build_uses_force_sandbox_against_sif(self):
        c = _apptainer(container_cache=Path("/cache"))
        cmd = c.MakeBuildSandboxCommand()
        sif = c.GetLocalPath()
        sandbox = c.GetSandboxPath()
        assert cmd.startswith("apptainer build")
        assert "--sandbox" in cmd
        # --force lets the step rebuild over a partial sandbox without
        # the caller having to nuke it first.
        assert "--force" in cmd
        assert str(sandbox) in cmd
        assert str(sif) in cmd

    def test_build_empty_for_docker(self):
        # No sandbox concept for docker; helper returns empty string so
        # callers can interpolate without branching.
        assert _docker().MakeBuildSandboxCommand() == ""


class TestRunCommandSwitch:
    def test_apptainer_local_emits_sandbox_or_sif_ternary(self):
        c = _apptainer(container_cache=Path("/cache"))
        cmd = c.MakeRunCommand(local=True)
        sif = c.GetLocalPath()
        sandbox = c.GetSandboxPath()
        # Both paths must appear inside a shell conditional that picks
        # sandbox-dir when present, else SIF.
        assert "[ -d" in cmd and str(sandbox) in cmd
        assert str(sif) in cmd
        assert "if" in cmd and "then" in cmd and "else" in cmd and "fi" in cmd
        # The whole expression must be a single shell token (wrapped in
        # double quotes) so it lands as one argument to apptainer.
        assert '"$(if' in cmd

    def test_apptainer_local_false_uses_remote_image(self):
        # local=False keeps the OCI URL as the image arg (apptainer pulls
        # streaming) — unchanged behavior.
        cmd = _apptainer().MakeRunCommand(local=False)
        assert "docker://quay.io/example/tool:1.0" in cmd
        assert "if [ -d" not in cmd

    def test_docker_unaffected(self):
        # The substitution is gated on the APPTAINER branch; docker's
        # command should not contain any shell conditional.
        cmd = _docker().MakeRunCommand()
        assert "if [ -d" not in cmd
        assert ".sandbox" not in cmd
