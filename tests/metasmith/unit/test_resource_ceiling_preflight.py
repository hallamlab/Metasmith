"""A workflow that cannot fit on the machine says so before it runs anything.

Pins I8 of the annotation-trio investigation. The trio's interproscan and
diamond steps ask for more cpus and more memory than a local executor's block
allows, and nextflow discovers that only after the run has been staged and
launched: it aborts the whole run with "Process requirement exceeds available
memory" and takes the healthy siblings with it. The retry ladder doubles the
request, so no retry recovers.

A local, mamba or native executor runs tasks on the driver's own host, so an
over-request there can never be scheduled -- that is the case that earns a
refusal. A scheduler queues the same request against a bigger node and must not
be refused. Sits beside the GPU launch preflight, which answers the same shape
of question the same way.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import metasmith.agents.workflow_ops as _agents
from metasmith.agents import Agent
from metasmith.constants import AgentPaths
from metasmith.coms.terminals import ShellResult
from metasmith.models.libraries import Duration, Resources, Size
from metasmith.models.remote import Source


TASK_KEY = "testtask01"
BIG_STEP = "p01__interproscan"
SMALL_STEP = "p02__prodigal"

# What the trio actually declares for its heaviest step.
BIG = Resources(cpus=8, memory=Size.GB(64), duration=Duration(hours=12))
# Fits under the ceiling below, but its second attempt would not.
SMALL = Resources(cpus=1, memory=Size.GB(4), duration=Duration(hours=1))

CEILING_CPUS = 4
CEILING_GB = 8
# A ceiling every declared step fits under, so nothing is refused and what is
# left to look at is the retry ladder.
ROOMY_CPUS = 8
ROOMY_GB = 64


def _resources_file(workspace: Path) -> None:
    lines = ["process {"]
    for name, res in [(BIG_STEP, BIG), (SMALL_STEP, SMALL)]:
        lines.append(f"    withName: '{name}' " + "{")
        lines += [f"        {l}" for l in res.AsNextflowFormat(is_config=True)]
        lines.append("    }")
    lines.append("}")
    (workspace / AgentPaths.NXF_RES).write_text("\n".join(lines) + "\n")


def _ceiling_params(cpus: int, gb: int) -> dict:
    # `params.executor` is the documented override, and declaring it is how an
    # operator tells metasmith what this host will actually schedule. Passed
    # rather than written into the preset: a preset's own `executor {}` block is
    # evaluated before the params file merges, so editing it never binds.
    return {"executor": {"cpus": cpus, "memory": f"{gb} GB"}}


class RecordingMover:
    sent: dict[str, str] = {}

    def __init__(self, *a, **kw):
        self._queued: list[tuple[Path, Path]] = []

    def QueueTransfer(self, src, dest):
        self._queued.append((Path(str(src.GetPath())), Path(str(dest.GetPath()))))

    def ExecuteTransfers(self, wait_for_complete=False):
        for src, dest in self._queued:
            if src.exists() and src.is_file():
                RecordingMover.sent[dest.name] = src.read_text()
        return []


class LocalFakeShell:
    """Runs every probe for real against the local agent home, except the launch.

    Reading the workspace for real is what keeps this test out of the way of how
    the ceiling check decides to find a step's request.
    """

    def __init__(self):
        self.calls: list[str] = []

    def Exec(self, cmd, timeout=None, history=False, quiet=False, **_) -> ShellResult:
        self.calls.append(cmd)
        if self._is_launch(cmd):
            return ShellResult(out=[], err=[], exit_code=0)
        try:
            res = subprocess.run(
                ["bash", "-c", cmd], capture_output=True, text=True, timeout=30,
            )
        except Exception:
            return ShellResult(out=[], err=[], exit_code=1)
        return ShellResult(
            out=res.stdout.splitlines(), err=res.stderr.splitlines(),
            exit_code=res.returncode,
        )

    @staticmethod
    def _is_launch(cmd: str) -> bool:
        # The probe that checks the launcher exists names it too; only the bare
        # invocation is the launch.
        return AgentPaths.LAUNCHER_FILE in cmd and "-e" not in cmd

    @property
    def launched(self) -> bool:
        return any(self._is_launch(c) for c in self.calls)


@pytest.fixture
def agent(tmp_path, monkeypatch):
    home = tmp_path / "msm_home"
    workspace = home / AgentPaths.STAGED / TASK_KEY
    (workspace / AgentPaths.INTERNALS / AgentPaths.TASK).mkdir(parents=True)
    (home / "relay").mkdir(parents=True, exist_ok=True)
    (home / "lib").mkdir(parents=True, exist_ok=True)
    launcher = workspace / AgentPaths.LAUNCHER_FILE
    launcher.write_text("#!/bin/sh\nexit 0\n")
    launcher.chmod(0o755)
    _resources_file(workspace)
    RecordingMover.sent = {}
    monkeypatch.setattr(_agents, "Logistics", RecordingMover)
    return Agent(home=Source.FromLocal(home))


@pytest.fixture
def launch(monkeypatch, agent):
    def _launch(preset: str, config: Path | None = None, **kw):
        shell = LocalFakeShell()

        class _AgentShell:
            def __init__(self, _agent): pass
            def __enter__(self): return shell
            def __exit__(self, *a): return False

        monkeypatch.setattr(_agents, "AgentShell", _AgentShell)
        cfg = config or agent.GetNxfConfigPresets()[preset]
        agent.RunWorkflow(
            TASK_KEY, config_file=cfg,
            is_local_preset=(preset == "local"), **kw,
        )
        return shell

    return _launch


@pytest.fixture
def local_ceiling():
    return _ceiling_params(CEILING_CPUS, CEILING_GB)


@pytest.fixture
def roomy_ceiling():
    return _ceiling_params(ROOMY_CPUS, ROOMY_GB)


def test_a_request_over_the_local_ceiling_is_refused_before_launching(
    launch, local_ceiling,
):
    with pytest.raises(Exception) as e:
        shell = launch("local", params=local_ceiling)
        assert not shell.launched, (
            "the launch went ahead; nextflow will abort the run an hour in "
            "instead of this being said now"
        )
    assert RecordingMover.sent == {}, "the run was staged out despite the refusal"
    message = str(e.value)
    assert "interproscan" in message, message
    assert "64" in message, f"the refusal does not name the request: {message}"
    assert str(CEILING_GB) in message, f"the refusal does not name the ceiling: {message}"


def test_the_refusal_names_the_override_that_lifts_it(launch, local_ceiling):
    # A box that is merely small is a hard stop otherwise, so the message has to
    # carry the way out.
    with pytest.raises(Exception) as e:
        launch("local", params=local_ceiling)
    message = str(e.value)
    assert "executor" in message or "--" in message, (
        f"the refusal offers no way past it: {message}"
    )


def test_a_scheduler_queues_the_same_request(launch):
    # Green today and it must stay green: slurm will find a node that fits.
    shell = launch("slurm")
    assert shell.launched


def test_the_retry_ladder_stays_under_the_ceiling(launch, roomy_ceiling):
    # Every step fits here, so nothing is refused -- but the ladder doubles the
    # request on each attempt, and four attempts of a 64 GB step ask for 512 GB
    # of a 64 GB machine. Every one of those is unschedulable for the same
    # reason as the first, so no retry ever recovers.
    launch("local", params=roomy_ceiling)
    config = RecordingMover.sent.get(AgentPaths.NXF_CONFIG, "")
    assert config, "nothing was staged out to inspect"
    assert "withName" in config, (
        "the launched config carries no per-step block, so nothing bounds the "
        "retry ladder by the ceiling"
    )
    per_step = config[config.index("withName"):]
    assert str(ROOMY_GB) in per_step, (
        f"no per-step block names the {ROOMY_GB} GB ceiling, so attempt 2 of a "
        f"step that fits asks for twice the machine: {per_step[:400]}"
    )
