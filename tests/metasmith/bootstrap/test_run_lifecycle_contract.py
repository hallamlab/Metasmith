"""What a run's generated scripts must say for it to be stoppable.

Every line pinned here is text in a script nothing else reads, so nothing else
would notice it going missing. The behaviours behind them are exercised in
tests/metasmith/lifecycle/ -- this file is the cheap sentry that fails first.
"""
from pathlib import Path

import pytest

from metasmith.agents.runner import (
    NXF_SHUTDOWN_GRACE_S, RenderLauncher, RenderNextflowScript,
)
from metasmith.constants import AgentPaths
from metasmith.env.environment import ContainerDef, Environment, Runtime

TASK_KEY = "abcd1234"


@pytest.fixture
def launcher() -> str:
    return RenderLauncher(TASK_KEY, ["# agent setup"], "--bind /a:/b")


@pytest.fixture
def driver() -> str:
    return RenderNextflowScript(
        workspace=Path("/ws/runs")/TASK_KEY,
        log_dir=Path(f"./{AgentPaths.INTERNALS}/logs.T"),
        host="agenthost", results_folder="results",
        nxf_report=Path("r.html"), nxf_dag=Path("d.dot"), stub_param="",
    )


class TestLauncherRootsTheRun:
    def test_the_driver_is_backgrounded_under_job_control(self, launcher):
        # `set -m` is what gives the backgrounded driver its own process group;
        # without it $! leads nothing and a reap by group finds nothing.
        lines = launcher.splitlines()
        run = next(i for i, l in enumerate(lines) if "api run_workflow" in l)
        assert "set -m" in lines[:run]
        assert lines[run].rstrip().endswith("&")
        assert lines[run + 1].strip() == "RUN_PGID=$!"

    def test_the_run_token_is_exported_before_the_driver_starts(self, launcher):
        lines = launcher.splitlines()
        export = next(
            i for i, l in enumerate(lines)
            if l.startswith(f"export {AgentPaths.RUN_TOKEN_ENV}=")
        )
        assert f'"{TASK_KEY}.$TIMESTAMP"' in lines[export]
        assert export < next(i for i, l in enumerate(lines) if "api run_workflow" in l)

    def test_the_group_and_token_are_written_down(self, launcher):
        assert f"> ./{AgentPaths.RUN_TOKEN_FILE}" in launcher
        assert f'echo "$RUN_PGID" > ./{AgentPaths.RUN_PGID_FILE}' in launcher

    def test_the_driver_does_not_inherit_a_terminal(self, launcher):
        # A backgrounded job in its own group stops on SIGTTIN if it reads the
        # terminal it was launched from.
        run = next(l for l in launcher.splitlines() if "api run_workflow" in l)
        assert "</dev/null" in run


class TestDriverStopsNextflowByGroup:
    def test_nextflow_gets_its_own_group(self, driver):
        # Job control is enabled for exactly one command -- the nextflow launch
        # -- so $PID is a pgid and nothing else in the script gets its own group.
        lines = [l.strip() for l in driver.splitlines()]
        on, off, pid = lines.index("set -m"), lines.index("set +m"), lines.index("PID=$!")
        assert lines[on + 1].startswith("nextflow")
        assert off == pid + 1

    def test_the_supervisor_terms_the_group_waits_then_kills(self, driver):
        assert "kill -TERM -$PID" in driver
        assert f"for _ in $(seq {NXF_SHUTDOWN_GRACE_S})" in driver
        assert "kill -KILL -$PID" in driver
        assert driver.index("kill -TERM -$PID") < driver.index("kill -KILL -$PID")

    def test_the_lock_file_is_the_cancel_handle(self, driver):
        assert f"PIDF=./{AgentPaths.PID_LOCK_FILE}" in driver
        assert "echo $PID >$PIDF" in driver


def _run_command(runtime: Runtime) -> str:
    return Environment(
        image="quay.io/example/tool:1.0", runtime=runtime,
        container=ContainerDef(binds=[(Path("/host"), Path("/data"))]),
    ).MakeRunCommand()


class TestToolContainersCarryTheRunToken:
    def test_docker_labels_the_container(self):
        # `--rm` leaves no other handle once the client is gone.
        assert (
            f"--label {AgentPaths.RUN_LABEL}=${{{AgentPaths.RUN_TOKEN_ENV}:-}}"
            in _run_command(Runtime.DOCKER)
        )

    def test_apptainer_puts_the_token_back_after_cleanenv(self):
        cmd = _run_command(Runtime.APPTAINER)
        assert "--cleanenv" in cmd
        assert f"--env {AgentPaths.RUN_TOKEN_ENV}=" in cmd

    @pytest.mark.parametrize("runtime,foreign", [
        (Runtime.DOCKER, "--env "),
        (Runtime.APPTAINER, "--label "),
    ])
    def test_neither_carries_the_other_dialect(self, runtime, foreign):
        assert foreign not in _run_command(runtime)
