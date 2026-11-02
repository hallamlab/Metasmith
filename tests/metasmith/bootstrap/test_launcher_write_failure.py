# A launcher that cannot create its log dir says so and exits non-zero.
#
# On fir a /scratch file quota at 997K of 1000K made start.sh's `mkdir -p $LOG_DIR`
# fail. start.sh went on regardless: it printed a run token, the client reported
# the run triggered, `logs.latest` named a log dir that never existed, and nothing
# ran. The quota is simulated with a `mkdir` on PATH that fails the way Lustre does.

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

import metasmith.agents.workflow_ops as _agents
from metasmith.agents import Agent
from metasmith.agents.runner import LAUNCH_WRITE_EXIT, RenderLauncher
from metasmith.coms.terminals import ShellResult
from metasmith.constants import AgentPaths
from metasmith.models.remote import Source


TASK_KEY = "quotatask"


def _quota_mkdir(tmp_path: Path) -> Path:
    shim = tmp_path/"shim"
    shim.mkdir()
    real = subprocess.run(["bash", "-c", "command -v mkdir"], capture_output=True, text=True).stdout.strip()
    (shim/"mkdir").write_text(
        "#!/bin/bash\n"
        'case "${@: -1}" in\n'
        "  *logs.*) echo \"mkdir: cannot create directory '${@: -1}': Disk quota exceeded\" >&2; exit 1 ;;\n"
        "esac\n"
        f'exec {real} "$@"\n'
    )
    (shim/"mkdir").chmod(0o755)
    return shim


def _home(tmp_path: Path) -> Path:
    home = tmp_path/"msm_home"
    workspace = home/AgentPaths.STAGED/TASK_KEY
    (workspace/AgentPaths.INTERNALS/AgentPaths.TASK).mkdir(parents=True)
    stub = home/"msm"
    stub.write_text(f"#!/bin/bash\ntouch {home}/DRIVER_RAN\n")
    stub.chmod(0o755)
    return workspace


def _write_launcher(workspace: Path, name: str, text: str) -> Path:
    path = workspace/name
    path.write_text(text)
    path.chmod(0o755)
    return path


def _run(launcher: Path, shim: Path) -> subprocess.CompletedProcess:
    env = os.environ | {"PATH": f"{shim}:{os.environ['PATH']}"}
    return subprocess.run(
        ["bash", str(launcher), "0"], cwd="/", env=env,
        capture_output=True, text=True, timeout=60,
    )


@pytest.mark.parametrize("background", [True, False], ids=["login-node", "slurm-job"])
def test_a_log_dir_the_quota_refuses_fails_the_launch(tmp_path, background):
    workspace = _home(tmp_path)
    launcher = _write_launcher(workspace, AgentPaths.LAUNCHER_FILE, RenderLauncher(
        TASK_KEY, [], "", background=background, workdir=str(workspace),
    ))
    res = _run(launcher, _quota_mkdir(tmp_path))

    assert res.returncode == LAUNCH_WRITE_EXIT, res
    assert "run token is" not in res.stdout, "a run token was printed for a run that never began"
    assert f"{workspace}/{AgentPaths.INTERNALS}/logs." in res.stderr, res.stderr
    assert "Disk quota exceeded (EDQUOT)" in res.stderr, res.stderr
    assert "higher file limit" in res.stderr, res.stderr
    assert not (workspace/AgentPaths.INTERNALS/"logs.latest").is_symlink()
    assert not (workspace/AgentPaths.RUN_TOKEN_FILE).exists()
    assert not (tmp_path/"msm_home"/"DRIVER_RAN").exists()


def test_a_launcher_that_cannot_write_reports_the_launch_failed(tmp_path, monkeypatch):
    workspace = _home(tmp_path)
    launcher = _write_launcher(workspace, AgentPaths.LAUNCHER_FILE, RenderLauncher(TASK_KEY, [], ""))
    shim = _quota_mkdir(tmp_path)
    env = os.environ | {"PATH": f"{shim}:{os.environ['PATH']}"}

    class _Shell:
        def Exec(self, cmd, **_):
            res = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, env=env, timeout=60)
            return ShellResult(res.stdout.splitlines(), res.stderr.splitlines(), res.returncode)

    class _AgentShell:
        def __init__(self, _agent): pass
        def __enter__(self): return _Shell()
        def __exit__(self, *a): return False

    class _Mover:
        def __init__(self, *a, **kw): pass
        def QueueTransfer(self, src, dest): pass
        def ExecuteTransfers(self, wait_for_complete=False): return []

    monkeypatch.setattr(_agents, "AgentShell", _AgentShell)
    monkeypatch.setattr(_agents, "Logistics", _Mover)
    monkeypatch.delenv("METASMITH_DRIVER_SLURM", raising=False)
    agent = Agent(home=Source.FromLocal(tmp_path/"msm_home"))

    with pytest.raises(RuntimeError) as e:
        agent.RunWorkflow(TASK_KEY, config_file=agent.GetNxfConfigPresets()["local"], is_local_preset=True)
    message = str(e.value)
    assert "launch of [quotatask] failed" in message, message
    assert "Disk quota exceeded (EDQUOT)" in message, message
    assert "higher file limit" in message, message
