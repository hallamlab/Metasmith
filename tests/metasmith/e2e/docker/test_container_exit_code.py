"""The exit code a container step reports is the command's own.

Pins I1 and I2 of the annotation-trio investigation. `_ExecInEnv` wraps every
container command in a generated bounce script whose EXIT trap records the
status. A transform is free to change directory, so the trap must not depend on
where the command left the shell, and a status that cannot be recovered must be
reported as such rather than as a plain failure.
"""

from __future__ import annotations

import subprocess
import time as _real_time
from pathlib import Path

import pytest

from metasmith.coms.terminals import LiveShell
from metasmith.env import Runtime
from metasmith.logging import Log
from metasmith.models.libraries import ContextData, ContextPath, ExecutionContext
from metasmith.models.solver import Dependency, Endpoint
import metasmith.models.libraries.execution as execution_mod


# The tag the standard library pins. It has bash, so the trap semantics match
# what a real transform gets -- a busybox shell's differ.
IMAGE = "quay.io/hallamlab/python_for_data_science:1.4.0"

# Exists in the image and is owned by root, so the task uid cannot write there.
UNWRITABLE = "/usr"

# `exec` replaces the bounce shell, so its EXIT trap goes with it and no
# marker is ever written -- the same shape as a task the OOM killer takes,
# reproduced without having to exhaust the box.
LOST_MARKER_STATUS = 43


@pytest.fixture(scope="session")
def probe_image(docker_available):
    present = subprocess.run(
        ["docker", "image", "inspect", IMAGE], capture_output=True
    )
    if present.returncode != 0:
        pulled = subprocess.run(
            ["docker", "pull", "--platform=linux/amd64", IMAGE],
            capture_output=True, timeout=900,
        )
        if pulled.returncode != 0:
            pytest.skip(f"cannot obtain {IMAGE}: {pulled.stderr.decode()[-400:]}")
    return IMAGE


class _NoSleepTime:
    # `_ExecInEnv` sleeps five seconds before every non-zero exit. Five cases of
    # that is most of this test's wall clock and none of its meaning. Narrow to
    # the module under test so the shell's own polling still sleeps.
    def __getattr__(self, name):
        return getattr(_real_time, name)

    def sleep(self, *_a, **_kw):
        pass


@pytest.fixture
def container_case(tmp_path, monkeypatch, probe_image):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "_metasmith").mkdir()
    agent_home = tmp_path / "agent_home"
    agent_home.mkdir()
    monkeypatch.setattr(execution_mod, "time", _NoSleepTime())

    env_decl = tmp_path / "tool.env"
    env_decl.write_text(f"container: docker://{probe_image}\nconda: unused\n")
    image_dep = Dependency(properties={"image"}, parents=set())
    cp = ContextPath(local=env_decl, external=env_decl, container=env_decl)
    cd = ContextData(
        input_group=[cp], endpoint=Endpoint(properties={"image"}), type_name="image",
    )

    log_file = tmp_path / "probe.log"
    log_file.touch()

    def _run(cmd: str) -> dict:
        raised: SystemExit | None = None
        Log.AddLogFile(log_file)
        try:
            with LiveShell() as shell:
                ctx = ExecutionContext(
                    _inputs=[{image_dep: cd}],
                    _get_output_paths=lambda *a: None,
                    external_shell=shell,
                    external_cwd=tmp_path,
                    external_agent_home=agent_home,
                    _environment=Runtime.DOCKER,
                )
                try:
                    ctx.ExecWithEnv(env=image_dep, cmd=cmd)
                except SystemExit as e:
                    raised = e
        finally:
            Log.RemoveLogFile(log_file)
        code = 0 if raised is None else int(raised.code or 0)
        return {"code": code, "log": log_file.read_text()}

    return _run


def test_success_after_a_bare_cd_reports_zero(container_case, tmp_path):
    # I1: the reporter's failure. The work succeeds, the shell is left in a
    # directory the task uid cannot write, and the marker write is what fails.
    res = container_case(
        f"cd {UNWRITABLE}\n"
        f"echo done > /ws/product.txt\n"
    )
    assert res["code"] == 0, res["log"][-2000:]
    assert (tmp_path / "product.txt").read_text().strip() == "done"


def test_success_in_a_subshell_reports_zero(container_case, tmp_path):
    # The workaround the reporter added to downloadInterProScanDB. It must keep
    # working once the engine no longer needs it.
    res = container_case(
        f"(cd {UNWRITABLE} && echo done > /ws/product.txt)\n"
    )
    assert res["code"] == 0, res["log"][-2000:]
    assert (tmp_path / "product.txt").read_text().strip() == "done"


def test_failure_after_a_bare_cd_reports_the_commands_code(container_case):
    # A genuine failure must survive the relocated marker with its own code,
    # not be flattened to 1 like the false failure above.
    res = container_case(
        f"cd {UNWRITABLE}\n"
        f"exit 42\n"
    )
    assert res["code"] == 42, res["log"][-2000:]


def test_failure_in_the_workdir_reports_the_commands_code(container_case):
    res = container_case("exit 42\n")
    assert res["code"] == 42, res["log"][-2000:]


def test_an_unreadable_marker_is_reported_distinctly(container_case):
    # I2: the bounce shell is replaced, so its EXIT trap never runs and there is
    # no marker to read. That is not the same event as a command that returned
    # non-zero, and the report must say so and carry the container's own status.
    res = container_case(f"exec bash -c 'exit {LOST_MARKER_STATUS}'\n")
    assert res["code"] == LOST_MARKER_STATUS, res["log"][-2000:]
    log = res["log"]
    assert "exitcode." in log, "the report does not name the marker it could not read"
    assert str(LOST_MARKER_STATUS) in log, "the report does not carry the container's own status"
