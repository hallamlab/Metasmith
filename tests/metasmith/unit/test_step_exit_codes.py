"""A failed step ends with the exit code that killed it.

The retry ladder escalates by exit code, so a step whose tool was OOM-killed must end with
137 rather than the generic 1 a failed protocol used to report, and a pipe must fail when
any of its stages does.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.models.libraries.execution import ExecutionContext


@pytest.fixture
def context(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return ExecutionContext(
        _inputs=[], _get_output_paths=lambda *a: None, external_shell=None,
        external_cwd=tmp_path, external_agent_home=tmp_path,
    )


def test_local_shell_returns_the_commands_exit_code(context):
    assert context.LocalShell("exit 3") == 3
    assert context.LocalShell("true") == 0


def test_local_shell_fails_a_pipe_whose_first_stage_fails(context):
    assert context.LocalShell("false | cat") == 1


def test_a_step_with_no_failed_command_ends_with_one(context):
    assert context.FailureExitCode() == 1


def test_an_oom_kill_outranks_a_later_generic_failure(context):
    context.LocalShell("exit 137")
    context.LocalShell("exit 2")
    assert context.FailureExitCode() == 137


def test_otherwise_the_last_failure_is_the_answer(context):
    context.LocalShell("exit 2")
    context.LocalShell("exit 5")
    assert context.FailureExitCode() == 5


def test_the_api_exits_with_the_steps_code(monkeypatch):
    import metasmith.coms.api as api
    from metasmith.models.libraries import ExecutionResult

    monkeypatch.setattr(api, "StageAndRunTransform", lambda *a, **k: ExecutionResult(False, exit_code=137))
    with pytest.raises(SystemExit) as e:
        api.Api().execute_transform({"workspace": "/ws", "step_index": "1", "host": "h"})
    assert e.value.code == 137
