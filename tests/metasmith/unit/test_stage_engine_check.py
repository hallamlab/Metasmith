"""Staging refuses a library the agent's engine cannot dispatch, before anything is sent."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from metasmith.agents.workflow_ops import EngineMismatch, _assert_engine_runs_library
from metasmith.coms.terminals import ShellResult
from metasmith.env.dispatch_scan import ScanSource

ONE_CALL = "context.ExecWithEnv(env=tool, cmd='true')\n"


class _Shell:
    def __init__(self, *out: str):
        self.out = list(out)

    def Exec(self, cmd, **_):
        return ShellResult(out=self.out, err=[], exit_code=0)


def _task(source: str):
    step = SimpleNamespace(transform=SimpleNamespace(_env_scan=ScanSource(source)))
    return SimpleNamespace(plan=SimpleNamespace(steps=[step]))


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
