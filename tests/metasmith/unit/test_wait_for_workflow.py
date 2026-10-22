"""`WaitForWorkflow` must not call a live run a failed one.

`PID.lock` is removed the moment nextflow exits, but the driver runs on for as
long as promotion, results compilation and log gathering take -- and the driver
is what writes the sentinel the wait is looking for. Reading the lock's absence
as failure reports `errored` for every run with real work to do at the end.
"""

from __future__ import annotations

import os
import signal
import subprocess
from pathlib import Path

import pytest

from metasmith.agents.run_control import _RunControl
from metasmith.constants import AgentPaths
from metasmith.models.remote import Source

SENTINEL = "run completed at"


class _LocalAgent(_RunControl):
    def __init__(self, home: Path):
        self.home = Source.FromLocal(home)

    def _is_ssh(self) -> bool:
        return False


@pytest.fixture
def rig(tmp_path):
    key = "TESTKEY01"
    home = tmp_path / "agent_home"
    workspace = AgentPaths.to_task(key, root=home).parent.parent
    logs = workspace / AgentPaths.INTERNALS / "logs.20260101-000000"
    logs.mkdir(parents=True)
    (logs / "agent.log").write_text("running\n")
    (workspace / AgentPaths.INTERNALS / "logs.latest").symlink_to(logs)
    return _LocalAgent(home), key, workspace


def _wait(agent, key, **kw):
    return agent.WaitForWorkflow(
        key, timeout_s=1.0, poll_s=0.1, grace_s=0.0, **kw
    )["status"]


@pytest.fixture
def live_process_group():
    procs = []

    def start() -> int:
        p = subprocess.Popen(
            ["sleep", "60"], start_new_session=True,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        procs.append(p)
        return os.getpgid(p.pid)

    yield start
    for p in procs:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        p.wait(timeout=10)


def test_a_live_driver_after_nextflow_exits_is_not_an_error(
    rig, live_process_group
):
    agent, key, workspace = rig
    # No PID.lock: nextflow is gone. The driver is not.
    (workspace / AgentPaths.RUN_PGID_FILE).write_text(
        f"{live_process_group()}\n"
    )
    assert _wait(agent, key) == "timeout", (
        "a run whose driver is still compiling results was reported failed"
    )


def test_a_dead_driver_with_no_sentinel_is_an_error(rig):
    agent, key, workspace = rig
    # A pgid nothing is left in: `kill -0` on it fails, as after a crash.
    (workspace / AgentPaths.RUN_PGID_FILE).write_text("2147483646\n")
    assert _wait(agent, key) == "errored"


def test_a_workspace_with_no_pgid_falls_back_to_the_old_answer(rig):
    agent, key, _workspace = rig
    assert _wait(agent, key) == "errored", (
        "a workspace that cannot answer must not be waited on forever"
    )


def test_the_sentinel_still_wins_over_a_live_driver(rig, live_process_group):
    agent, key, workspace = rig
    (workspace / AgentPaths.RUN_PGID_FILE).write_text(
        f"{live_process_group()}\n"
    )
    run_dir = Path(os.readlink(workspace / AgentPaths.INTERNALS / "logs.latest"))
    (run_dir / "agent.log").write_text(f"done\n{SENTINEL} [now]\n")
    assert _wait(agent, key) == "completed"


def test_a_log_with_no_match_is_not_read_as_a_shifted_line(rig):
    """`grep -c` prints its 0 and exits 1, so a `|| echo 0` fallback doubles it.

    Positional parsing then read the second 0 as the PID field and never saw a
    dead run at all.
    """
    agent, key, workspace = rig
    (workspace / AgentPaths.RUN_PGID_FILE).write_text("2147483646\n")
    assert SENTINEL not in (
        workspace / AgentPaths.INTERNALS / "logs.latest" / "agent.log"
    ).read_text()
    assert _wait(agent, key) == "errored"


@pytest.fixture
def fake_slurm(tmp_path, monkeypatch):
    bin_dir = tmp_path / "fake_slurm_bin"
    bin_dir.mkdir()
    state = bin_dir / "state"

    def set_state(s: str):
        state.write_text(s)

    for tool in ("squeue", "sacct"):
        script = bin_dir / tool
        script.write_text(f"#!/bin/bash\ncat {state}\n")
        script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    return set_state


def _relaunch_under_slurm(workspace: Path):
    from metasmith.agents.run_host import SLURM_LAUNCH_CLEAR, SlurmLaunchClear
    res = subprocess.run(
        ["bash", "-c", SlurmLaunchClear(workspace)], capture_output=True, text=True,
    )
    assert SLURM_LAUNCH_CLEAR in res.stdout, res.stderr
    (workspace / AgentPaths.RUN_SLURM_JOB_FILE).write_text("4242\n")


def test_a_queued_slurm_driver_is_not_read_through_the_last_runs_log(rig, fake_slurm):
    agent, key, workspace = rig
    latest = workspace / AgentPaths.INTERNALS / "logs.latest"
    (latest / "agent.log").write_text(f"done\n{SENTINEL} [then]\n")
    (workspace / AgentPaths.RUN_PGID_FILE).write_text("2147483646\n")
    _relaunch_under_slurm(workspace)
    fake_slurm("PENDING")
    assert _wait(agent, key) == "missing", (
        "a queued driver was reported done from the previous run's sentinel"
    )
    new_run = workspace / AgentPaths.INTERNALS / "logs.20260102-000000"
    new_run.mkdir()
    (new_run / "agent.log").write_text(f"done\n{SENTINEL} [now]\n")
    latest.symlink_to(new_run)
    assert _wait(agent, key) == "completed"


def test_a_slurm_driver_gone_before_its_log_is_an_error(rig, fake_slurm):
    agent, key, workspace = rig
    _relaunch_under_slurm(workspace)
    fake_slurm("FAILED")
    res = agent.WaitForWorkflow(key, timeout_s=1.0, poll_s=0.1, grace_s=0.0)
    assert res["status"] == "errored"
    assert "4242" in res["tail"][0]


def test_a_live_run_keeps_its_log_link_when_a_slurm_launch_is_refused(
    rig, live_process_group
):
    from metasmith.agents.run_host import SLURM_LAUNCH_CLEAR, SlurmLaunchClear
    _agent, _key, workspace = rig
    (workspace / AgentPaths.RUN_PGID_FILE).write_text(f"{live_process_group()}\n")
    res = subprocess.run(
        ["bash", "-c", SlurmLaunchClear(workspace)], capture_output=True, text=True,
    )
    assert SLURM_LAUNCH_CLEAR not in res.stdout
    assert (workspace / AgentPaths.INTERNALS / "logs.latest").is_symlink()
