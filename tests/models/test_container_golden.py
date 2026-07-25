"""Golden (exact-string) characterization of the container command surface.

These pin the *byte-for-byte* shell emitted by `Container.MakePullCommand`,
`MakeBindsParam`, and `MakeRunCommand` for both Docker and Apptainer — the
exact surfaces that the upcoming `Container` -> `Environment` carve (the `env`
module refactor) will move. Today these methods have only semantic/substring
coverage (`test_container_extra_args.py`, `test_container_sandbox.py`,
`test_container_binds.py`); none assert the whole string.

Intentionally brittle by design: the golden strings encode shell that contains
`$(id -u)`, `${TMPDIR-"/tmp"}`, and the `"$(if [ -d … ])"` sandbox/sif ternary.
They are asserted literally and NOT normalized. After the refactor, any of
these going red is the review surface — either the carve changed behavior
(investigate) or the change is intentional (update the golden + note why).
"""

from pathlib import Path

import pytest

from metasmith.coms.containers import Container, ContainerRuntime


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


def _container(runtime: ContainerRuntime, *, workdir=Path("/ws"), binds=None) -> Container:
    return Container(
        image=IMAGE,
        workdir=workdir,
        runtime=runtime,
        container_cache=CACHE,
        binds=list(BINDS) if binds is None else binds,
    )


# --------------------------------------------------------------------------
# MakePullCommand
# --------------------------------------------------------------------------

class TestPullGolden:
    def test_docker(self):
        cmd = _container(ContainerRuntime.DOCKER).MakePullCommand()
        assert cmd == "docker pull --platform=linux/amd64 quay.io/example/tool:1.0"

    def test_apptainer(self):
        cmd = _container(ContainerRuntime.APPTAINER).MakePullCommand()
        assert cmd == (
            f"apptainer pull {SIF} docker://quay.io/example/tool:1.0"
        )


# --------------------------------------------------------------------------
# MakeBindsParam
# --------------------------------------------------------------------------

class TestBindsGolden:
    def test_docker(self):
        binds = _container(ContainerRuntime.DOCKER).MakeBindsParam()
        assert binds == (
            '--mount type=bind,source="/host/data",target="/data" '
            '--mount type=bind,source="/host/db",target="/db"'
        )

    def test_apptainer(self):
        binds = _container(ContainerRuntime.APPTAINER).MakeBindsParam()
        assert binds == "--bind /host/data:/data,/host/db:/db"

    @pytest.mark.parametrize("runtime", [ContainerRuntime.DOCKER, ContainerRuntime.APPTAINER])
    def test_empty_binds_is_empty_string(self, runtime):
        assert _container(runtime, binds=[]).MakeBindsParam() == ""


# --------------------------------------------------------------------------
# MakeRunCommand
# --------------------------------------------------------------------------

class TestRunCommandGolden:
    def test_docker(self):
        cmd = _container(ContainerRuntime.DOCKER).MakeRunCommand(local=False)
        assert cmd == (
            'docker run --platform=linux/amd64 --rm -u $(id -u):$(id -g) '
            '--network=host -e TMPDIR=${TMPDIR-"/tmp"} --entrypoint="" '
            '--workdir="/ws" '
            '--mount type=bind,source="/host/data",target="/data" '
            '--mount type=bind,source="/host/db",target="/db" '
            'quay.io/example/tool:1.0'
        )

    def test_docker_no_workdir_no_binds(self):
        cmd = _container(ContainerRuntime.DOCKER, workdir=None, binds=[]).MakeRunCommand(local=False)
        assert cmd == (
            'docker run --platform=linux/amd64 --rm -u $(id -u):$(id -g) '
            '--network=host -e TMPDIR=${TMPDIR-"/tmp"} --entrypoint="" '
            'quay.io/example/tool:1.0'
        )

    def test_apptainer_remote(self):
        cmd = _container(ContainerRuntime.APPTAINER).MakeRunCommand(local=False)
        assert cmd == (
            'apptainer exec --no-home --cleanenv --env TMPDIR=${TMPDIR-"/tmp"} '
            '--env OPENBLAS_NUM_THREADS=1 --env OMP_NUM_THREADS=1 '
            '--pwd "/ws" '
            '--bind /host/data:/data,/host/db:/db '
            'docker://quay.io/example/tool:1.0'
        )

    def test_apptainer_no_workdir_no_binds(self):
        cmd = _container(ContainerRuntime.APPTAINER, workdir=None, binds=[]).MakeRunCommand(local=False)
        assert cmd == (
            'apptainer exec --no-home --cleanenv --env TMPDIR=${TMPDIR-"/tmp"} '
            '--env OPENBLAS_NUM_THREADS=1 --env OMP_NUM_THREADS=1 '
            'docker://quay.io/example/tool:1.0'
        )

    def test_apptainer_local_sandbox_sif_ternary(self):
        # local=True swaps the image arg for a shell conditional that prefers
        # the unpacked sandbox dir (deploy built one) else the SIF. This whole
        # expression must be one double-quoted token so it lands as a single
        # argument to `apptainer exec`.
        cmd = _container(ContainerRuntime.APPTAINER).MakeRunCommand(local=True)
        assert cmd == (
            'apptainer exec --no-home --cleanenv --env TMPDIR=${TMPDIR-"/tmp"} '
            '--env OPENBLAS_NUM_THREADS=1 --env OMP_NUM_THREADS=1 '
            '--pwd "/ws" '
            '--bind /host/data:/data,/host/db:/db '
            f'"$(if [ -d "{SANDBOX}" ]; then echo "{SANDBOX}"; else echo "{SIF}"; fi)"'
        )
