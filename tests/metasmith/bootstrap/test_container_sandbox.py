"""Tests for the SIF/sandbox helpers on Environment.

An unprivileged user cannot mount squashfs in the kernel (it has no
FS_USERNS_MOUNT), so without the setuid starter apptainer serves a SIF's
rootfs through a userspace FUSE reader it spawns itself. That works;
metasmith no longer inspects the host to find out. The unpacked sandbox
directory is reached only when no SIF can be produced at all -- some hosts
ship an mksquashfs that segfaults on large images, which nothing static can
predict -- or when `rootfs=sandbox` declares it.

`Rootfs` is that declaration: `auto` (try, then fall back), `sif` and
`sandbox`. It decides both what `MakeMaterialiseCommand` builds and which
artifact `MakeRunCommand(local=True)` hands apptainer, which is what makes a
forced mode observable rather than advisory. These tests pin the emitted
shell text's semantic properties -- they don't spawn apptainer.
"""

from pathlib import Path

from metasmith.env import ContainerDef, Environment, Rootfs, Runtime


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

    def test_build_from_image_never_names_a_sif(self):
        """The sandbox arm goes registry -> sandbox, skipping mksquashfs.

        `apptainer build --sandbox <dir> <sif>` needs the SIF to exist, and
        producing it runs mksquashfs -- which aborts on large images on some
        hosts. When the sandbox is the artifact the SIF is a throwaway
        intermediate, so the build reads the OCI layers directly and the SIF
        is never made.
        """
        c = _apptainer(container_cache=Path("/cache"))
        cmd = c.MakeBuildSandboxCommand(from_image=True)
        assert cmd == (
            f"apptainer build --force --sandbox {c.GetSandboxPath()} "
            f"docker://quay.io/example/tool:1.0"
        )
        assert ".sif" not in cmd

    def test_build_empty_for_docker(self):
        # No sandbox concept for docker; helper returns empty string so
        # callers can interpolate without branching.
        assert _docker().MakeBuildSandboxCommand() == ""


class TestRunCommandSwitch:
    def test_auto_emits_sandbox_or_sif_ternary(self):
        c = _apptainer(container_cache=Path("/cache"))
        cmd = c.MakeRunCommand(local=True)
        sif = c.GetLocalPath()
        sandbox = c.GetSandboxPath()
        # Nobody declared an artifact, so read what materialising left on
        # disk: sandbox-dir when present, else SIF. A sandbox is only ever
        # there because no SIF could be built, so it wins.
        assert "[ -d" in cmd and str(sandbox) in cmd
        assert str(sif) in cmd
        assert "if" in cmd and "then" in cmd and "else" in cmd and "fi" in cmd
        # The whole expression must be a single shell token (wrapped in
        # double quotes) so it lands as one argument to apptainer.
        assert '"$(if' in cmd

    def test_forced_modes_name_their_artifact_outright(self):
        """No ternary, which is the point.

        Left conditional, a `rootfs=sif` run in a shared image store would
        quietly resolve to a `.sandbox` some earlier experiment left behind
        -- exactly how the old host-level override managed to be inert.
        """
        sif_env = _apptainer(container_cache=Path("/cache"), rootfs=Rootfs.SIF)
        cmd = sif_env.MakeRunCommand(local=True)
        assert cmd.endswith(f'"{sif_env.GetLocalPath()}"')
        assert ".sandbox" not in cmd and "if [ -d" not in cmd

        box_env = _apptainer(container_cache=Path("/cache"), rootfs=Rootfs.SANDBOX)
        cmd = box_env.MakeRunCommand(local=True)
        assert cmd.endswith(f'"{box_env.GetSandboxPath()}"')
        assert ".sif" not in cmd and "if [ -d" not in cmd

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
