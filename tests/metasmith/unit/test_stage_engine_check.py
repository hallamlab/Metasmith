"""Staging's pre-send probes: the engine-version check and the external-bind check."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from metasmith.agents.workflow_ops import (
    EngineMismatch, _assert_engine_runs_library, _assert_external_binds_exist,
)
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


def test_a_missing_bind_is_refused():
    with pytest.raises(FileNotFoundError, match="/data/gone"):
        _assert_external_binds_exist(_Shell("MISSING::/data/gone"), _task(""), ["/data/gone"])


def test_a_silent_bind_probe_is_not_a_refusal():
    _assert_external_binds_exist(_SilentShell(), _task(""), ["/data/gone"])
