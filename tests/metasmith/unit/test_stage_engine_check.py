"""Staging's pre-send probes: the engine-version check, and the inputs the agent host lacks."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from metasmith.agents.workflow_ops import (
    EngineMismatch, _assert_engine_runs_library, _send_local_inputs,
)
from metasmith.models.remote import Source
from metasmith.coms.terminals import ShellResult
from metasmith.env.dispatch_scan import ScanSource

ONE_CALL = "context.ExecWithEnv(env=tool, cmd='true')\n"


class _Shell:
    def __init__(self, *out: str):
        self.out = list(out)

    def Exec(self, cmd, **_):
        return ShellResult(out=self.out, err=[], exit_code=0)


class _SilentShell:
    def Exec(self, cmd, what=None, **_):
        raise TimeoutError(f"[{what}] produced no output for 60s")


def _task(source: str):
    step = SimpleNamespace(transform=SimpleNamespace(_env_scan=ScanSource(source)))
    return SimpleNamespace(_key="k", plan=SimpleNamespace(steps=[step], given=[]))


def test_an_engine_before_the_one_call_api_is_refused_by_version():
    with pytest.raises(EngineMismatch) as e:
        _assert_engine_runs_library(_Shell("including dev binds", "metasmith 0.22.1"), _task(ONE_CALL))
    assert "0.22.1" in str(e.value) and "0.23.0" in str(e.value)


def test_an_engine_on_the_one_call_api_stages():
    _assert_engine_runs_library(_Shell("metasmith 0.25.0"), _task(ONE_CALL))


def test_a_library_that_runs_no_tool_needs_no_check():
    _assert_engine_runs_library(_Shell("metasmith 0.22.1"), _task("x = 1\n"))


def test_an_unreadable_version_is_not_a_refusal():
    _assert_engine_runs_library(_Shell("apptainer: command not found"), _task(ONE_CALL))


def test_a_silent_version_probe_is_not_a_refusal():
    _assert_engine_runs_library(_SilentShell(), _task(ONE_CALL))


class _AgentHost:
    # Answers the presence probe as a host that lacks `missing`, and runs every
    # other command here, which is where a local home's copies land.
    def __init__(self, *missing: Path):
        self.missing = [str(p) for p in missing]

    def Exec(self, cmd, what=None, **_):
        if what == "checking the inputs on the agent host":
            out = [f"MISSING::{p}" for p in self.missing if p in cmd]
        else:
            out = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True).stdout.splitlines()
        return ShellResult(out=out, err=[], exit_code=0)


def _given(*paths: Path):
    given = [SimpleNamespace(path=p, ResolvePath=lambda p=p: p) for p in paths]
    return SimpleNamespace(_key="k", plan=SimpleNamespace(steps=[], given=given))


def _reads(tmp_path: Path, name="r1.fq") -> Path:
    f = tmp_path / "local" / name
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text("@r\nACGT\n+\nIIII\n")
    return f


def test_an_input_the_agent_lacks_is_copied_beside_its_own_path(tmp_path):
    reads = _reads(tmp_path)
    home = tmp_path / "home"
    out = _send_local_inputs(_AgentHost(reads), _given(reads), Source.FromLocal(home), home / "inputs")
    copy = home / "inputs" / reads.relative_to("/")
    assert out == {reads: copy}
    assert copy.read_text() == reads.read_text()


def test_an_input_the_agent_has_is_left_where_it_is(tmp_path):
    reads = _reads(tmp_path)
    home = tmp_path / "home"
    assert _send_local_inputs(_AgentHost(), _given(reads), Source.FromLocal(home), home / "inputs") == {}
    assert not (home / "inputs").exists()


def test_an_input_on_neither_host_is_refused(tmp_path):
    gone = tmp_path / "gone.fq"
    with pytest.raises(FileNotFoundError, match="not on this machine either") as e:
        _send_local_inputs(_AgentHost(gone), _given(gone), Source.FromLocal(tmp_path / "home"), tmp_path / "home/inputs")
    assert str(gone) in str(e.value)


def test_an_agent_that_cannot_be_copied_to_says_what_to_do(tmp_path):
    reads = _reads(tmp_path)
    with pytest.raises(FileNotFoundError, match="put each input on the agent host yourself"):
        _send_local_inputs(_AgentHost(reads), _given(reads), None, tmp_path / "home/inputs")


def test_a_failed_copy_fails_staging(tmp_path):
    reads = _reads(tmp_path)
    home = tmp_path / "home"
    home.mkdir()
    os.chmod(home, 0o500)
    try:
        with pytest.raises(RuntimeError, match="copying 1 input") as e:
            _send_local_inputs(_AgentHost(reads), _given(reads), Source.FromLocal(home), home / "inputs")
    finally:
        os.chmod(home, 0o700)
    assert str(reads) in str(e.value)


def test_a_silent_presence_probe_is_not_a_refusal(tmp_path):
    reads = _reads(tmp_path)
    assert _send_local_inputs(_SilentShell(), _given(reads), None, tmp_path / "home/inputs") == {}
