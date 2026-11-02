from pathlib import Path

import pytest

from metasmith.env import ContainerDef, Environment, Runtime


IMAGE = "docker://quay.io/example/tool:1.0"
CACHE = Path("/cache")
CACHED = "docker..quay.io_example_tool..1.0"
STORE = "${APPTAINER_CACHEDIR:-/cache}"
SIF = f"{STORE}/{CACHED}.sif"
SANDBOX = f"{STORE}/{CACHED}.sandbox"

BINDS = [(Path("/host/data"), Path("/data")), (Path("/host/db"), Path("/db"))]


def _container(runtime: Runtime, *, workdir=Path("/ws"), binds=None) -> Environment:
    return Environment(
        image=IMAGE,
        runtime=runtime,
        container=ContainerDef(
            cache=CACHE,
            workdir=workdir,
            binds=list(BINDS) if binds is None else binds,
        ),
    )


class TestPullGolden:
    def test_docker(self):
        cmd = _container(Runtime.DOCKER).MakePullCommand()
        assert cmd == "docker pull --platform=linux/amd64 quay.io/example/tool:1.0"

    def test_apptainer(self):
        cmd = _container(Runtime.APPTAINER).MakePullCommand()
        assert cmd == (
            f"apptainer pull {SIF} docker://quay.io/example/tool:1.0"
        )


class TestBindsGolden:
    def test_docker(self):
        binds = _container(Runtime.DOCKER).MakeBindsParam()
        assert binds == (
            '--mount type=bind,source="/host/data",target="/data" '
            '--mount type=bind,source="/host/db",target="/db"'
        )

    def test_apptainer(self):
        binds = _container(Runtime.APPTAINER).MakeBindsParam()
        assert binds == "--bind /host/data:/data,/host/db:/db"

    @pytest.mark.parametrize("runtime", [Runtime.DOCKER, Runtime.APPTAINER])
    def test_empty_binds_is_empty_string(self, runtime):
        assert _container(runtime, binds=[]).MakeBindsParam() == ""


class TestRunCommandGolden:
    def test_docker(self):
        cmd = _container(Runtime.DOCKER).MakeRunCommand(local=False)
        assert cmd == (
            'docker run --platform=linux/amd64 --rm -u $(id -u):$(id -g) '
            '--network=host -e TMPDIR=${TMPDIR-"/tmp"} '
            '--label msm.run=${METASMITH_RUN:-} --entrypoint="" '
            '--workdir="/ws" '
            '--mount type=bind,source="/host/data",target="/data" '
            '--mount type=bind,source="/host/db",target="/db" '
            'quay.io/example/tool:1.0'
        )

    def test_docker_no_workdir_no_binds(self):
        cmd = _container(Runtime.DOCKER, workdir=None, binds=[]).MakeRunCommand(local=False)
        assert cmd == (
            'docker run --platform=linux/amd64 --rm -u $(id -u):$(id -g) '
            '--network=host -e TMPDIR=${TMPDIR-"/tmp"} '
            '--label msm.run=${METASMITH_RUN:-} --entrypoint="" '
            'quay.io/example/tool:1.0'
        )

    def test_apptainer_remote(self):
        cmd = _container(Runtime.APPTAINER).MakeRunCommand(local=False)
        assert cmd == (
            'apptainer exec --no-home --cleanenv --env TMPDIR=${TMPDIR-"/tmp"} '
            '--env OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1} --env OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1} '
            '--env METASMITH_RUN=${METASMITH_RUN:-} '
            '--pwd "/ws" '
            '--bind /host/data:/data,/host/db:/db '
            'docker://quay.io/example/tool:1.0'
        )

    def test_apptainer_no_workdir_no_binds(self):
        cmd = _container(Runtime.APPTAINER, workdir=None, binds=[]).MakeRunCommand(local=False)
        assert cmd == (
            'apptainer exec --no-home --cleanenv --env TMPDIR=${TMPDIR-"/tmp"} '
            '--env OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1} --env OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1} '
            '--env METASMITH_RUN=${METASMITH_RUN:-} '
            'docker://quay.io/example/tool:1.0'
        )

    def test_apptainer_local_sandbox_sif_ternary(self):
        cmd = _container(Runtime.APPTAINER).MakeRunCommand(local=True)
        assert cmd == (
            'apptainer exec --no-home --cleanenv --env TMPDIR=${TMPDIR-"/tmp"} '
            '--env OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1} --env OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1} '
            '--env METASMITH_RUN=${METASMITH_RUN:-} '
            '--pwd "/ws" '
            '--bind /host/data:/data,/host/db:/db '
            f'"$(if [ -d "{SANDBOX}" ]; then echo "{SANDBOX}"; else echo "{SIF}"; fi)"'
        )


MAMBA_ENV = "toolenv"


def _mamba(*, native=False, extra_args=None) -> Environment:
    return Environment(
        image=MAMBA_ENV,
        runtime=Runtime.MAMBA,
        native=native,
        extra_args=list(extra_args or []),
        container=ContainerDef(cache=CACHE, workdir=Path("/ws"), binds=list(BINDS)),
    )


class TestMambaGolden:
    def test_run_command_is_env_activation_only(self):
        assert _mamba().MakeRunCommand() == f"mamba run -n {MAMBA_ENV}"

    def test_extra_args_ride_along(self):
        assert _mamba(extra_args=["--no-capture-output"]).MakeRunCommand() == (
            f"mamba run -n {MAMBA_ENV} --no-capture-output"
        )

    def test_binds_collapse_to_nothing(self):
        assert _mamba().MakeBindsParam() == ""

    def test_nothing_to_pull_and_no_image_store(self):
        env = _mamba()
        assert env.MakePullCommand() == ""
        assert env.GetLocalPath() is None
        assert env.GetSandboxPath() is None
        assert env.ProvisionSteps(agent_home=Path("/home")) == []

    def test_wrapper_prefix_activates_the_env(self):
        assert _mamba().MakeWrapperPrefix() == f"mamba run -n {MAMBA_ENV}"


class TestNativeGolden:
    @pytest.mark.parametrize("runtime", [Runtime.DOCKER, Runtime.APPTAINER, Runtime.MAMBA])
    def test_native_emits_no_wrapper_whatever_the_runtime(self, runtime):
        env = Environment(image=IMAGE, runtime=runtime, native=True,
                          container=ContainerDef(cache=CACHE, workdir=Path("/ws"), binds=list(BINDS)))
        assert env.MakeRunCommand() == ""
        assert env.MakeWrapperPrefix() == ""
        assert env.MakeBindsParam() == ""
        assert env.needs_relay is False

    def test_native_still_forwards_caller_args(self):
        env = Environment(image=IMAGE, runtime=Runtime.DOCKER, native=True,
                          extra_args=["--flag", "v"])
        assert env.MakeRunCommand() == "--flag v"


class TestGpuArgsGolden:
    def test_docker(self):
        assert _container(Runtime.DOCKER).MakeGpuArgs() == ["--gpus", "all"]

    def test_apptainer(self):
        assert _container(Runtime.APPTAINER).MakeGpuArgs() == ["--nv"]

    def test_mamba_and_native_inherit_the_host(self):
        assert _mamba().MakeGpuArgs() == []
        assert _mamba(native=True).MakeGpuArgs() == []


AGENT_HOME = Path("/arc/home/u/msm_home")


class TestProvisionGolden:
    def test_docker_refreshes_via_pull_with_local_fallback(self):
        steps = _container(Runtime.DOCKER).ProvisionSteps(agent_home=AGENT_HOME)
        assert len(steps) == 1
        cmd = steps[0][0]
        assert cmd == (
            'docker pull --platform=linux/amd64 quay.io/example/tool:1.0 || '
            'docker image inspect "quay.io/example/tool:1.0" >/dev/null 2>&1 || '
            '{ echo "ERROR: could not pull [quay.io/example/tool:1.0] and no local copy is cached" >&2; false; }'
        )

    def test_docker_native_has_nothing_to_provision(self):
        env = Environment(
            image=IMAGE, runtime=Runtime.DOCKER, native=True,
            container=ContainerDef(cache=CACHE, workdir=Path("/ws"), binds=list(BINDS)),
        )
        assert env.ProvisionSteps(agent_home=AGENT_HOME) == []

    def test_apptainer_materialises_one_artifact_without_asking_the_host(self):
        steps = _container(Runtime.APPTAINER).ProvisionSteps(agent_home=AGENT_HOME)
        assert len(steps) == 1
        cmd = steps[0][0]

        assert cmd.startswith(
            f'mkdir -p "{STORE}"; {{ [ -e {SIF} ] && [ -e {SIF}.verified ]; }}'
        )
        assert f'apptainer build --force --sandbox {SANDBOX} {IMAGE}' in cmd, (
            "sandbox rung is not building from the registry"
        )
        assert f'apptainer pull {SIF} {IMAGE}' in cmd
        assert cmd.index("apptainer pull") < cmd.index("mksquashfs")
        assert cmd.index("mksquashfs") < cmd.index("--sandbox")

    def test_assertive_forces_a_rebuild(self):
        steps = _container(Runtime.APPTAINER).ProvisionSteps(agent_home=AGENT_HOME, assertive=True)
        assert steps[0][0].startswith(
            f'mkdir -p "{STORE}"; rm -rf {SANDBOX} {SIF} {SANDBOX}.verified {SIF}.verified; '
        )

    def test_docker_assertive_is_a_no_op(self):
        default_cmd = _container(Runtime.DOCKER).ProvisionSteps(agent_home=AGENT_HOME)[0][0]
        assertive_cmd = _container(Runtime.DOCKER).ProvisionSteps(agent_home=AGENT_HOME, assertive=True)[0][0]
        assert default_cmd == assertive_cmd
