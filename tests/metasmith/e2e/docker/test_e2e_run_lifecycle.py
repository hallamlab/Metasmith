"""Stopping a real run, end to end.

The one test that would have caught the reported bug: cancel a live run and then
ask the agent what is still running. The relay and container lanes are pinned in
tests/metasmith/lifecycle/; what this adds is the whole chain at once.
"""
from __future__ import annotations

import time

import pytest

from metasmith.gui.store import Project
from metasmith.ops import runtime as op_runtime

from .test_e2e_gui_run import _ready_to_launch, _real_relay, gui  # noqa: F401

pytestmark = pytest.mark.docker

# Long enough that a one-step stub run is still sleeping when the cancel lands.
STUB_DELAY_S = 180.0
# How long the driver may take to finish its post-run pass after nextflow stops.
DRAIN_TIMEOUT_S = 180.0


def _eventually(predicate, timeout: float, interval: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _launch_stub_run(gui, tmp_path, docker_image) -> tuple[str, str]:
    wf_name, agent_name = _ready_to_launch(gui, tmp_path, docker_image)
    project: Project = gui.application.config["MSM_PROJECT"]
    agent_path = str(project.agent_path(agent_name))
    # Direct ops rather than POST /api/runs: only this path takes a stub delay,
    # and without one a one-step run finishes before there is anything to cancel.
    staged = op_runtime.stage(agent_path, str(project.workflow_path(wf_name)))
    op_runtime.run(agent_path, staged["task_key"], stub_delay=STUB_DELAY_S)
    return agent_path, staged["task_key"]


@pytest.mark.skipif(
    not _real_relay(),
    reason="needs a real msm_relay; this suite's image builds a stub "
           "(./dev.sh -brc && ./dev.sh -br builds the real one)",
)
class TestStoppingARealRun:
    def test_a_run_outlives_the_shell_that_launched_it(self, gui, tmp_path, docker_image):
        # Never kill on disconnect: RunWorkflow returns as soon as start.sh has
        # detached, and the shell it used is gone by the time we look.
        agent_path, key = _launch_stub_run(gui, tmp_path, docker_image)
        try:
            report = op_runtime.ps(agent_path, key)
            assert report["token"], f"start.sh recorded no run token: {report}"
            assert not report["empty"], (
                "the run did not survive the shell that launched it"
            )
        finally:
            op_runtime.reap(agent_path, key)

    def test_cancel_leaves_nothing_running(self, gui, tmp_path, docker_image):
        agent_path, key = _launch_stub_run(gui, tmp_path, docker_image)
        try:
            out = op_runtime.cancel(agent_path, key)
            assert out["survived"] == [], out
            assert out["status"] == "cancelled", out
            assert op_runtime.ps(agent_path, key, scope="workload")["empty"], (
                "cancel returned clean but the agent still has work running"
            )
            # The driver deliberately outlives the cancel to snapshot logs and
            # promote the cache, so the run scope clears a moment later.
            assert _eventually(
                lambda: op_runtime.ps(agent_path, key)["empty"], DRAIN_TIMEOUT_S,
            ), op_runtime.ps(agent_path, key)
        finally:
            op_runtime.reap(agent_path, key)

    def test_reap_clears_a_run_nobody_cancelled(self, gui, tmp_path, docker_image):
        # The recovery surface: a run whose controller is long gone.
        agent_path, key = _launch_stub_run(gui, tmp_path, docker_image)
        assert not op_runtime.ps(agent_path, key)["empty"]

        out = op_runtime.reap(agent_path, key)

        assert out["survived"] == [], out
        assert op_runtime.ps(agent_path, key)["empty"]
