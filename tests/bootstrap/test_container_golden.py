"""Golden (exact-string) characterization of the container command surface.

These pin the *byte-for-byte* shell emitted by `Environment.MakePullCommand`,
`MakeBindsParam`, and `MakeRunCommand` for Docker and Apptainer — the surfaces
the `Container` -> `Environment` carve moved into the sealed `env` package.
They exist because that carve silently dropped `_store_root()` (and with it
`APPTAINER_CACHEDIR` support) with no test catching it. Per-runtime coverage
of the whole emitted string is what makes the next reshape fail loudly instead
of shipping.

Intentionally brittle by design: the golden strings encode shell that contains
`$(id -u)`, `${TMPDIR-"/tmp"}`, and the `"$(if [ -d … ])"` sandbox/sif ternary.
They are asserted literally and NOT normalized. Any of these going red is the
review surface — either the change altered behavior (investigate) or it is
intentional (update the golden + note why).
"""

from pathlib import Path

import pytest

from metasmith.env import ContainerDef, Environment, Runtime


IMAGE = "docker://quay.io/example/tool:1.0"
CACHE = Path("/cache")
# `_cached_name()` sanitization of IMAGE: "://"->".." , ":"->".." , "/"->"_"
CACHED = "docker..quay.io_example_tool..1.0"
# The image store root is a shell expression expanded on the execution host:
# APPTAINER_CACHEDIR when the cluster sets one, else the caller's cache dir.
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


# --------------------------------------------------------------------------
# MakePullCommand
# --------------------------------------------------------------------------

class TestPullGolden:
    def test_docker(self):
        cmd = _container(Runtime.DOCKER).MakePullCommand()
        assert cmd == "docker pull --platform=linux/amd64 quay.io/example/tool:1.0"

    def test_apptainer(self):
        cmd = _container(Runtime.APPTAINER).MakePullCommand()
        assert cmd == (
            f"apptainer pull {SIF} docker://quay.io/example/tool:1.0"
        )


# --------------------------------------------------------------------------
# MakeBindsParam
# --------------------------------------------------------------------------

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


# --------------------------------------------------------------------------
# MakeRunCommand
# --------------------------------------------------------------------------

class TestRunCommandGolden:
    def test_docker(self):
        cmd = _container(Runtime.DOCKER).MakeRunCommand(local=False)
        assert cmd == (
            'docker run --platform=linux/amd64 --rm -u $(id -u):$(id -g) '
            '--network=host -e TMPDIR=${TMPDIR-"/tmp"} --entrypoint="" '
            '--workdir="/ws" '
            '--mount type=bind,source="/host/data",target="/data" '
            '--mount type=bind,source="/host/db",target="/db" '
            'quay.io/example/tool:1.0'
        )

    def test_docker_no_workdir_no_binds(self):
        cmd = _container(Runtime.DOCKER, workdir=None, binds=[]).MakeRunCommand(local=False)
        assert cmd == (
            'docker run --platform=linux/amd64 --rm -u $(id -u):$(id -g) '
            '--network=host -e TMPDIR=${TMPDIR-"/tmp"} --entrypoint="" '
            'quay.io/example/tool:1.0'
        )

    def test_apptainer_remote(self):
        cmd = _container(Runtime.APPTAINER).MakeRunCommand(local=False)
        assert cmd == (
            'apptainer exec --no-home --cleanenv --env TMPDIR=${TMPDIR-"/tmp"} '
            '--env OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1} --env OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1} '
            '--pwd "/ws" '
            '--bind /host/data:/data,/host/db:/db '
            'docker://quay.io/example/tool:1.0'
        )

    def test_apptainer_no_workdir_no_binds(self):
        cmd = _container(Runtime.APPTAINER, workdir=None, binds=[]).MakeRunCommand(local=False)
        assert cmd == (
            'apptainer exec --no-home --cleanenv --env TMPDIR=${TMPDIR-"/tmp"} '
            '--env OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1} --env OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1} '
            'docker://quay.io/example/tool:1.0'
        )

    def test_apptainer_local_sandbox_sif_ternary(self):
        # local=True swaps the image arg for a shell conditional that prefers
        # the unpacked sandbox dir (deploy built one) else the SIF. This whole
        # expression must be one double-quoted token so it lands as a single
        # argument to `apptainer exec`.
        cmd = _container(Runtime.APPTAINER).MakeRunCommand(local=True)
        assert cmd == (
            'apptainer exec --no-home --cleanenv --env TMPDIR=${TMPDIR-"/tmp"} '
            '--env OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1} --env OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1} '
            '--pwd "/ws" '
            '--bind /host/data:/data,/host/db:/db '
            f'"$(if [ -d "{SANDBOX}" ]; then echo "{SANDBOX}"; else echo "{SIF}"; fi)"'
        )


# --------------------------------------------------------------------------
# the runtimes that are NOT containers
#
# These are the arm the original carve was least covered on, and the reason a
# dropped behaviour could ship unnoticed. mamba runs the tool on the host
# filesystem under an activated env; native means we are already inside the
# target environment and emit no wrapper at all. Both must collapse the
# container-shaped inputs (cache, workdir, binds) to nothing rather than
# rendering them in some third dialect.
# --------------------------------------------------------------------------

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
        # there is no boundary to bind across
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


# --------------------------------------------------------------------------
# GPU args
# --------------------------------------------------------------------------

class TestGpuArgsGolden:
    def test_docker(self):
        assert _container(Runtime.DOCKER).MakeGpuArgs() == ["--gpus", "all"]

    def test_apptainer(self):
        assert _container(Runtime.APPTAINER).MakeGpuArgs() == ["--nv"]

    def test_mamba_and_native_inherit_the_host(self):
        assert _mamba().MakeGpuArgs() == []
        assert _mamba(native=True).MakeGpuArgs() == []


# --------------------------------------------------------------------------
# ProvisionSteps — the deploy-time shell
# --------------------------------------------------------------------------

AGENT_HOME = Path("/arc/home/u/msm_home")


class TestProvisionGolden:
    def test_docker_has_nothing_to_provision(self):
        # no local image store for docker; the daemon owns its own cache
        assert _container(Runtime.DOCKER).ProvisionSteps(agent_home=AGENT_HOME) == []

    def test_apptainer_materialises_one_artifact_without_asking_the_host(self):
        """One artifact, no probe -- and the sandbox rung never packs a squashfs.

        The sandbox is built straight from the registry so mksquashfs is never
        invoked on that rung, which is what makes it usable as the last-resort
        fallback on a host whose mksquashfs segfaults (micb0: exit 139 on a
        plain `apptainer pull` of the metasmith image).
        """
        steps = _container(Runtime.APPTAINER).ProvisionSteps(agent_home=AGENT_HOME)
        assert len(steps) == 1
        cmd = steps[0][0]

        assert cmd.startswith(f'mkdir -p "{STORE}"; if [ ! -e {SIF} ] && [ ! -d {SANDBOX} ]; then')
        assert f'apptainer build --force --sandbox {SANDBOX} {IMAGE}' in cmd, (
            "sandbox rung is not building from the registry"
        )
        assert f'apptainer pull {SIF} {IMAGE}' in cmd
        # cheapest first: the pull, then the mksquashfs workaround, then the
        # unpack. Nothing inspects the host ahead of any of it.
        assert cmd.index("apptainer pull") < cmd.index("mksquashfs")
        assert cmd.index("mksquashfs") < cmd.index("--sandbox")

    def test_assertive_forces_a_rebuild(self):
        steps = _container(Runtime.APPTAINER).ProvisionSteps(agent_home=AGENT_HOME, assertive=True)
        assert steps[0][0].startswith(f'mkdir -p "{STORE}"; rm -rf {SANDBOX} {SIF}; if [ ! -e {SIF} ]')
