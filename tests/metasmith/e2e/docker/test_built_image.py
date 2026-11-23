"""A tool env can be an image a transform built: a licensed download in, a runnable image out.

`BuildImage` turns a recipe plus a package into the image file this runtime runs
from, and `ExecWithEnv` runs a command in it when that file is the env input --
the path a licensed tool takes, exercised here with a fake package and a debian
recipe so it runs in seconds.
"""

from __future__ import annotations

import shutil
import subprocess
import tarfile
import time as _real_time

import pytest

from metasmith.coms.terminals import LiveShell
from metasmith.env import Runtime
from metasmith.models.libraries import ContextData, ContextPath, ExecutionContext
from metasmith.models.solver import Dependency, Endpoint
from metasmith.serialization import IsText
import metasmith.models.libraries.execution as execution_mod


# The engine launches every tool shell with bash, which alpine lacks.
BASE = "debian:bookworm-slim"

INSTALL = """\
set -eu
mkdir -p /opt/fake
tar -xf "$1" -C /opt/fake
chmod 755 /opt/fake/hello.sh
ln -s /opt/fake/hello.sh /usr/local/bin/fake-tool
"""

DOCKERFILE = f"""\
FROM {BASE}
RUN --mount=type=bind,target=/build sh /build/install.sh /build/package
"""

DEF = """\
Bootstrap: docker
From: BASE

%files
    {{ recipe }}/install.sh /opt/msm-build/install.sh
    {{ package }} /opt/msm-build/package

%post
    sh /opt/msm-build/install.sh /opt/msm-build/package
    rm -rf /opt/msm-build
""".replace("BASE", BASE)


class _NoSleepTime:
    def __getattr__(self, name):
        return getattr(_real_time, name)

    def sleep(self, *_a, **_kw):
        pass


def _cp(p):
    return ContextPath(local=p, external=p, container=p)


@pytest.fixture(params=[Runtime.DOCKER, Runtime.APPTAINER], ids=lambda r: r.value)
def runtime(request):
    rt = request.param
    if rt == Runtime.DOCKER:
        request.getfixturevalue("docker_available")
        if subprocess.run(["docker", "image", "inspect", BASE], capture_output=True).returncode != 0:
            if subprocess.run(["docker", "pull", BASE], capture_output=True, timeout=600).returncode != 0:
                pytest.skip(f"cannot obtain {BASE}")
    elif shutil.which("apptainer") is None:
        pytest.skip("apptainer not installed")
    return rt


@pytest.fixture
def case(tmp_path, monkeypatch, runtime):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(execution_mod, "time", _NoSleepTime())
    (tmp_path / "_metasmith").mkdir()
    home = tmp_path / "agent_home"
    home.mkdir()

    script = tmp_path / "hello.sh"
    script.write_text('#!/bin/sh\necho "licensed-ok $1"\n')
    package = tmp_path / "fake_package.tgz"
    with tarfile.open(package, "w:gz") as t:
        t.add(script, arcname="hello.sh")
    recipe = tmp_path / "recipe"
    recipe.mkdir()
    (recipe / "install.sh").write_text(INSTALL)
    (recipe / "Dockerfile").write_text(DOCKERFILE)
    (recipe / "apptainer.def").write_text(DEF)

    image_dep = Dependency(properties={"fake.env"}, parents=set())
    image = tmp_path / "fake.env"
    data = ContextData(input_group=[_cp(image)], endpoint=Endpoint(properties={"fake.env"}), type_name="fake.env")

    shell = LiveShell().__enter__()
    ctx = ExecutionContext(
        _inputs=[{image_dep: data}],
        _get_output_paths=lambda *a: None,
        external_shell=shell,
        external_cwd=tmp_path,
        external_agent_home=home,
        _environment=runtime,
    )
    yield ctx, image_dep, _cp(recipe), _cp(package), _cp(image)
    shell.__exit__(None, None, None)
    if runtime == Runtime.DOCKER:
        subprocess.run(
            "docker images -q 'msm-built/fake.env' | xargs -r docker rmi -f",
            shell=True, capture_output=True,
        )


def test_a_built_image_runs_as_the_tool_env(case, tmp_path):
    ctx, image_dep, recipe, package, image = case
    assert ctx.BuildImage(recipe=recipe, out=image, files={"package": package}) == 0
    assert image.local.exists() and not IsText(image.local)
    assert not (tmp_path / "_image_build").exists(), "the docker build context outlived the build"

    ctx.ExecWithEnv(env=image_dep, cmd="fake-tool hi > out.txt")
    assert (tmp_path / "out.txt").read_text().strip() == "licensed-ok hi"


def test_a_failed_build_reports_failure(case, tmp_path):
    ctx, _, recipe, package, image = case
    (recipe.local / "install.sh").write_text("exit 3\n")
    assert ctx.BuildImage(recipe=recipe, out=image, files={"package": package}) != 0
    assert not image.local.exists()
