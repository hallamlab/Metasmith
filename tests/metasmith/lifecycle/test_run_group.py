"""The run group: what start.sh roots, and what a reap can still find.

start.sh is rendered by the real generator; only the `msm` wrapper it launches
is a stub, so the process tree this exercises is the shape a real run has.
"""
import os
import shutil
import signal
import subprocess
from pathlib import Path

import pytest

from metasmith.agents.run_control import RenderScan, _RunControl, _SCAN_SCRIPT
from metasmith.agents.runner import RenderLauncher
from metasmith.constants import AgentPaths

from _procs import alive, group_alive, pids_matching, wait_until


def _sleeps(*seconds: str) -> list[int]:
    # Exact needles: "sleep 95" would match every test in this module at once.
    out: list[int] = []
    for s in seconds:
        out += pids_matching(f"sleep {s}")
    return out

TASK_KEY = "lifecyc1"


def _workspace(tmp_path: Path, stub_body: str) -> Path:
    # start.sh cd's to its own directory and launches `../../msm`, so the layout
    # around it is part of the contract being exercised.
    workspace = tmp_path/AgentPaths.STAGED/TASK_KEY
    (workspace/AgentPaths.INTERNALS).mkdir(parents=True)
    stub = tmp_path/"msm"
    stub.write_text("#!/bin/bash\n" + stub_body)
    stub.chmod(0o755)
    launcher = workspace/AgentPaths.LAUNCHER_FILE
    launcher.write_text(RenderLauncher(TASK_KEY, [], ""))
    launcher.chmod(0o755)
    return workspace


def _start(workspace: Path) -> None:
    subprocess.run(
        ["bash", str(workspace/AgentPaths.LAUNCHER_FILE)], cwd=workspace,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30, check=True,
    )


def _scan(workspace: Path) -> dict:
    cmd = RenderScan(_SCAN_SCRIPT, workspace=workspace, relay=Path("/nonexistent/msm_relay"))
    res = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, timeout=60)
    return _RunControl._parse_scan(res.stdout.splitlines())


@pytest.fixture
def reaper():
    # start.sh detaches, so nothing here owns the tree it makes; teardown kills
    # by the group start.sh recorded, whatever the test asserted.
    groups: list[int] = []
    yield groups
    for pgid in groups:
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(pgid, sig)
            except (ProcessLookupError, PermissionError):
                pass


def test_start_sh_records_its_group_and_token(tmp_path, reaper):
    workspace = _workspace(tmp_path, "sleep 950 &\nsleep 951 &\nwait\n")
    _start(workspace)

    pgid_file = workspace/AgentPaths.RUN_PGID_FILE
    token_file = workspace/AgentPaths.RUN_TOKEN_FILE
    assert pgid_file.is_file() and token_file.is_file()
    pgid = int(pgid_file.read_text().strip())
    reaper.append(pgid)
    token = token_file.read_text().strip()
    assert token.startswith(f"{TASK_KEY}.")

    assert wait_until(lambda: len(_sleeps("950", "951")) == 2)
    report = _scan(workspace)
    assert report["token"] == token
    assert str(report["run_pgid"]) == str(pgid)
    assert {p["pid"] for p in report["processes"]} >= {
        str(p) for p in _sleeps("950", "951")
    }, "start.sh's descendants are not in the group it recorded"


def test_a_group_kill_clears_the_whole_run(tmp_path, reaper):
    workspace = _workspace(tmp_path, "sleep 952 &\nsleep 953 &\nwait\n")
    _start(workspace)
    pgid = int((workspace/AgentPaths.RUN_PGID_FILE).read_text().strip())
    reaper.append(pgid)
    assert wait_until(lambda: len(_sleeps("952", "953")) == 2)
    tree = _sleeps("952", "953")

    os.killpg(pgid, signal.SIGTERM)

    assert wait_until(lambda: not any(alive(p) for p in tree))
    assert wait_until(lambda: not group_alive(pgid))
    assert _scan(workspace)["processes"] == []


def test_the_token_scan_finds_what_left_the_group(tmp_path, reaper):
    # The evidence for skipping cgroups: a deliberate setsid escapes the process
    # group, but METASMITH_RUN is inherited across it and cannot be shed.
    if shutil.which("setsid") is None:
        pytest.skip("setsid(1) not available")
    workspace = _workspace(tmp_path, "setsid sleep 954 &\nsleep 955\n")
    _start(workspace)
    pgid = int((workspace/AgentPaths.RUN_PGID_FILE).read_text().strip())
    reaper.append(pgid)
    assert wait_until(lambda: pids_matching("sleep 954"))
    escaped = pids_matching("sleep 954")
    try:
        assert all(os.getpgid(p) != pgid for p in escaped), "setsid did not escape"

        report = _scan(workspace)
        found = {p["pid"] for p in report["token_processes"]}
        assert found >= {str(p) for p in escaped}, (
            "the METASMITH_RUN scan missed a process that setsid'd out of the "
            "run group -- the process-group-only design would leak it, and the "
            "cgroup arm this plan skipped would be needed after all"
        )
        assert {p["pid"] for p in report["processes"]}.isdisjoint(
            str(p) for p in escaped
        ), "the group scan claimed a process that had already left the group"
    finally:
        for p in escaped:
            try:
                os.kill(p, signal.SIGKILL)
            except ProcessLookupError:
                pass
