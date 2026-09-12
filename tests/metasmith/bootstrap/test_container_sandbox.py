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
        assert sif.parent == sandbox.parent
        assert sif.stem == sandbox.stem
        assert sif.suffix == ".sif"
        assert sandbox.suffix == ".sandbox"

    def test_store_root_honors_apptainer_cachedir(self):
        c = _apptainer(container_cache=Path("/cache"))
        expected_root = Path("${APPTAINER_CACHEDIR:-/cache}")
        assert c.GetLocalPath().parent == expected_root
        assert c.GetSandboxPath().parent == expected_root
        assert c.GetLocalPath().name == "docker..quay.io_example_tool..1.0.sif"

    def test_docker_has_no_sandbox(self):
        assert _docker().GetSandboxPath() is None


class TestBuildCommand:
    def test_build_uses_force_sandbox_against_sif(self):
        c = _apptainer(container_cache=Path("/cache"))
        cmd = c.MakeBuildSandboxCommand()
        sif = c.GetLocalPath()
        sandbox = c.GetSandboxPath()
        assert cmd.startswith("apptainer build")
        assert "--sandbox" in cmd
        assert "--force" in cmd
        assert str(sandbox) in cmd
        assert str(sif) in cmd

    def test_build_from_image_never_names_a_sif(self):
        c = _apptainer(container_cache=Path("/cache"))
        cmd = c.MakeBuildSandboxCommand(from_image=True)
        assert cmd == (
            f"apptainer build --force --sandbox {c.GetSandboxPath()} "
            f"docker://quay.io/example/tool:1.0"
        )
        assert ".sif" not in cmd

    def test_build_empty_for_docker(self):
        assert _docker().MakeBuildSandboxCommand() == ""


class TestRunCommandSwitch:
    def test_auto_emits_sandbox_or_sif_ternary(self):
        c = _apptainer(container_cache=Path("/cache"))
        cmd = c.MakeRunCommand(local=True)
        sif = c.GetLocalPath()
        sandbox = c.GetSandboxPath()
        assert "[ -d" in cmd and str(sandbox) in cmd
        assert str(sif) in cmd
        assert "if" in cmd and "then" in cmd and "else" in cmd and "fi" in cmd
        assert '"$(if' in cmd

    def test_forced_modes_name_their_artifact_outright(self):
        sif_env = _apptainer(container_cache=Path("/cache"), rootfs=Rootfs.SIF)
        cmd = sif_env.MakeRunCommand(local=True)
        assert cmd.endswith(f'"{sif_env.GetLocalPath()}"')
        assert ".sandbox" not in cmd and "if [ -d" not in cmd

        box_env = _apptainer(container_cache=Path("/cache"), rootfs=Rootfs.SANDBOX)
        cmd = box_env.MakeRunCommand(local=True)
        assert cmd.endswith(f'"{box_env.GetSandboxPath()}"')
        assert ".sif" not in cmd and "if [ -d" not in cmd

    def test_apptainer_local_false_uses_remote_image(self):
        cmd = _apptainer().MakeRunCommand(local=False)
        assert "docker://quay.io/example/tool:1.0" in cmd
        assert "if [ -d" not in cmd

    def test_docker_unaffected(self):
        cmd = _docker().MakeRunCommand()
        assert "if [ -d" not in cmd
        assert ".sandbox" not in cmd
