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
