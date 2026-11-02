# A launch on an agent home that cannot create files is refused before anything is sent.
#
# Lustre reports a file quota late, so a poller reading 997K of 1000K never saw the
# stop. Creating a file and a directory is the probe that cannot lag; the quota
# numbers, where `lfs` gives them, only decide whether to warn.

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

import metasmith.agents.workflow_ops as _agents
from metasmith.agents import Agent, HomeHeadroomError
from metasmith.agents.headroom import FILES_FLOOR, check_headroom
from metasmith.coms.terminals import ShellResult
from metasmith.constants import AgentPaths
from metasmith.models.remote import Source


TASK_KEY = "headroom01"


class _Shell:
    def __init__(self, env: dict, canned_probe: list[str] | None = None):
        self.env = env
        self.canned_probe = canned_probe
        self.calls: list[str] = []

    def Exec(self, cmd, **_) -> ShellResult:
        self.calls.append(cmd)
        if self.launched_by(cmd):
            return ShellResult([], [], 0)
        if self.canned_probe is not None and ".msm_headroom" in cmd:
            return ShellResult(self.canned_probe, [], 0)
        res = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, env=self.env, timeout=60)
        return ShellResult(res.stdout.splitlines(), res.stderr.splitlines(), res.returncode)

    @staticmethod
    def launched_by(cmd: str) -> bool:
        return AgentPaths.LAUNCHER_FILE in cmd and "-e" not in cmd

    @property
    def launched(self) -> bool:
        return any(self.launched_by(c) for c in self.calls)


class _Mover:
    sent: list[str] = []

    def __init__(self, *a, **kw): pass
    def QueueTransfer(self, src, dest): _Mover.sent.append(Path(str(dest.GetPath())).name)
    def ExecuteTransfers(self, wait_for_complete=False): return []


@pytest.fixture
def workspace(tmp_path):
    workspace = tmp_path/"msm_home"/AgentPaths.STAGED/TASK_KEY
    (workspace/AgentPaths.INTERNALS/AgentPaths.TASK).mkdir(parents=True)
    launcher = workspace/AgentPaths.LAUNCHER_FILE
    launcher.write_text("#!/bin/sh\nexit 0\n")
    launcher.chmod(0o755)
    yield workspace
    workspace.chmod(0o755)


@pytest.fixture
def launch(tmp_path, monkeypatch, workspace):
    _Mover.sent = []
    monkeypatch.setattr(_agents, "Logistics", _Mover)
    monkeypatch.delenv("METASMITH_DRIVER_SLURM", raising=False)

    def _launch(path_prefix: Path | None = None, canned_probe: list[str] | None = None) -> _Shell:
        env = dict(os.environ)
        if path_prefix is not None:
            env["PATH"] = f"{path_prefix}:{env['PATH']}"
        shell = _Shell(env, canned_probe)

        class _AgentShell:
            def __init__(self, _agent): pass
            def __enter__(self): return shell
            def __exit__(self, *a): return False

        monkeypatch.setattr(_agents, "AgentShell", _AgentShell)
        agent = Agent(home=Source.FromLocal(tmp_path/"msm_home"))
        agent.RunWorkflow(TASK_KEY, config_file=agent.GetNxfConfigPresets()["local"], is_local_preset=True)
        return shell

    return _launch


def _fake_lfs(tmp_path: Path, files_used: str, files_limit: int) -> Path:
    # fir charges /scratch to the directory's Lustre project id; the user's own
    # quota there reports no limit at all.
    bin_dir = tmp_path/"fakebin"
    bin_dir.mkdir()
    (bin_dir/"lfs").write_text(
        "#!/bin/bash\n"
        'case "$1" in\n'
        '  project) echo "83115734 P $3" ;;\n'
        '  quota) [ "$3" = "-p" ]'
        f' && echo "  $5 4742546500 20000000000 20000000000 - {files_used} {files_limit} {files_limit} -"'
        ' || echo "  $5 1 0 0 - 1297213 0 0 -" ;;\n'
        "esac\n"
    )
    (bin_dir/"lfs").chmod(0o755)
    return bin_dir


@pytest.mark.skipif(os.geteuid() == 0, reason="root creates files in a read-only directory")
def test_a_home_that_refuses_creation_refuses_the_launch(launch, workspace):
    workspace.chmod(0o555)
    with pytest.raises(HomeHeadroomError) as e:
        launch()
    message = str(e.value)
    assert str(workspace) in message, message
    assert "Permission denied (EACCES)" in message, message
    assert "higher file limit" in message, message
    assert _Mover.sent == [], "the run was sent out despite the refusal"


def test_a_quota_refusal_names_the_errno_and_the_way_out(launch, workspace):
    probe = [f"CREATE|fail|{workspace}/.msm_headroom.1|Disk quota exceeded"]
    with pytest.raises(HomeHeadroomError) as e:
        launch(canned_probe=probe)
    message = str(e.value)
    assert "Disk quota exceeded (EDQUOT)" in message, message
    assert "free files" in message and "higher file limit" in message, message


def test_low_file_headroom_warns_with_the_numbers_and_still_launches(launch, tmp_path, workspace):
    lfs = _fake_lfs(tmp_path, "997102*", 1_000_000)
    shell = launch(path_prefix=lfs)
    assert shell.launched
    warnings = check_headroom(shell.Exec(next(c for c in shell.calls if ".msm_headroom" in c)).out, 0, TASK_KEY)
    assert len(warnings) == 1, warnings
    assert "2,898" in warnings[0] and "1,000,000" in warnings[0], warnings[0]
    assert "higher file limit" in warnings[0], warnings[0]


def test_roomy_quota_and_no_lfs_both_launch_quietly(launch, tmp_path):
    lfs = _fake_lfs(tmp_path, "100", 1_000_000)
    shell = launch(path_prefix=lfs)
    assert shell.launched
    probe = next(c for c in shell.calls if ".msm_headroom" in c)
    assert check_headroom(shell.Exec(probe).out, 0, TASK_KEY) == []
    assert check_headroom(["CREATE|ok", "LFS|u someone|/scratch|lfs: Permission denied"], 0, TASK_KEY) == []


def test_the_warning_scales_with_the_plan():
    probe = ["CREATE|ok", f"LFS|p 1|/scratch|  /scratch 1 0 0 - {1_000_000 - FILES_FLOOR - 1} 0 1000000 -"]
    assert check_headroom(probe, 1, TASK_KEY) == []
    assert len(check_headroom(probe, 100, TASK_KEY)) == 1


def test_a_probe_that_returns_nothing_does_not_refuse():
    assert check_headroom([], 3, TASK_KEY) == []


def test_a_project_quota_df_also_reports_warns_once():
    # What fir's /scratch answered, with the file count raised to the limit's edge.
    probe = [
        "CREATE|ok", "DFK|20000000000|4742546500", "DFI|1000000|997102",
        "LFS|u someone|/scratch|  /scratch 18051620240 0 0 - 1297213 0 0 -",
        "LFS|p 83115734|/scratch|  /scratch 4742546500 20000000000 20000000000 - 997102 1000000 1000000 -",
    ]
    warnings = check_headroom(probe, 12, TASK_KEY)
    assert len(warnings) == 1 and "lfs quota -p 83115734" in warnings[0], warnings
