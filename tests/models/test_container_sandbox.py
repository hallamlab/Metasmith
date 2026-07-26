"""Tests for the SIF↔sandbox decision helpers on Environment.

The host's apptainer routes the rootfs through one of three mechanisms:
1. Kernel squashfs mount (setuid starter-suid present — HPC like Sockeye)
2. squashfuse_ll (apptainer without setuid for SIF — wedges under
   msm_relay's fork chain on WSL2, Bug E.2)
3. fuse-overlayfs (apptainer <1.4 without setuid for sandbox — races
   SIGBUS under SLURM array contention on fir, Bug E.4)

`MakeSandboxDecisionProbe` is a static two-axis check emitting either
`use-sif` (kernel mount safe, or sandbox would be worse) or `use-sandbox`
(apptainer >=1.4 with no setuid: SIF would FUSE-wedge, sandbox is
kernel-overlayfs). Deploy consults the verdict at deploy time;
`MakeRunCommand(local=True)` reads the sandbox dir's presence on disk at
run time. These tests pin the emitted shell text's semantic properties
and the run-time ternary — they don't spawn apptainer.
"""

from pathlib import Path

from metasmith.env import ContainerDef, Environment, Runtime


def _env(image, runtime, *, container_cache=Path('./'), binds=None, **kw) -> Environment:
    return Environment(
        image=image, runtime=runtime,
        container=ContainerDef(cache=container_cache, workdir=Path('/ws'), binds=list(binds or [])),
        **kw,
    )


def _apptainer(**kw) -> Environment:
    return _env("docker://quay.io/example/tool:1.0", Runtime.APPTAINER, **kw)


def _docker(**kw) -> Environment:
    return _env("docker://quay.io/example/tool:1.0", Runtime.DOCKER, **kw)


class TestCachePaths:
    def test_sandbox_is_sibling_of_sif(self):
        c = _apptainer(container_cache=Path("/cache"))
        sif = c.GetLocalPath()
        sandbox = c.GetSandboxPath()
        assert sif is not None and sandbox is not None
        # Both derive from the one store root, so they are always siblings
        # — the run-time SIF/sandbox ternary depends on this.
        assert sif.parent == sandbox.parent
        # Same stem (sanitized image name), different suffix.
        assert sif.stem == sandbox.stem
        assert sif.suffix == ".sif"
        assert sandbox.suffix == ".sandbox"

    def test_store_root_honors_apptainer_cachedir(self):
        # Single point of control: the store root is a shell expression that
        # prefers APPTAINER_CACHEDIR (expanded on the execution host) and
        # falls back to the caller's container_cache. pathlib round-trips the
        # ${...:-.../...} segment cleanly for .parent / .name.
        c = _apptainer(container_cache=Path("/cache"))
        expected_root = Path("${APPTAINER_CACHEDIR:-/cache}")
        assert c.GetLocalPath().parent == expected_root
        assert c.GetSandboxPath().parent == expected_root
        assert c.GetLocalPath().name == "docker..quay.io_example_tool..1.0.sif"

    def test_docker_has_no_sandbox(self):
        # Sandbox is an apptainer-only concept; docker runtime returns None
        # the same way GetLocalPath does today.
        assert _docker().GetSandboxPath() is None


class TestSandboxDecisionProbe:
    def test_probe_checks_starter_suid_setuid(self):
        probe = _apptainer().MakeSandboxDecisionProbe()
        # The setuid `starter-suid` check is the first axis: kernel-mount
        # path is safe for both SIF and sandbox, so verdict is use-sif.
        assert "starter-suid" in probe
        assert "[ -u" in probe

    def test_probe_checks_apptainer_version(self):
        probe = _apptainer().MakeSandboxDecisionProbe()
        # The second axis is apptainer major.minor — versions <1.4 route
        # the sandbox through fuse-overlayfs (Bug E.4 SIGBUS on fir);
        # >=1.4 uses kernel overlayfs.
        assert "apptainer --version" in probe
        # Numeric major/minor comparison must be present so the gate is
        # accurate across point releases.
        assert "-ge 2" in probe or "-ge 4" in probe
        assert "-ge 4" in probe  # the load-bearing one

    def test_probe_emits_only_two_verdicts(self):
        probe = _apptainer().MakeSandboxDecisionProbe()
        # Verdicts are the contract consumed by agents.py:Deploy.
        # Both literals must appear (probe can take either branch).
        assert '"use-sif"' in probe
        assert '"use-sandbox"' in probe

    def test_probe_empty_for_docker(self):
        # Docker has no sandbox/SIF distinction; helper returns empty so
        # callers can interpolate without branching.
        assert _docker().MakeSandboxDecisionProbe() == ""


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
        # sandbox-dir when present, else SIF. The directory's presence is
        # the run-time signal; deploy controls the presence.
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
