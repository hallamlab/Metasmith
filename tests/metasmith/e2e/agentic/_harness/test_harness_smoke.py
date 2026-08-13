"""Unit tests for the ralph loop + CONTROL.json + budget plumbing.

Runs in the default pytest collection — no e2e_agentic marker, no real
model calls, no containers, no opencode/claude binaries required.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pytest

from tests.metasmith.e2e.agentic.drivers.base import IterResult
from tests.metasmith.e2e.agentic.harness.budget import TokenBudget
from tests.metasmith.e2e.agentic.harness.control import (
    Control,
    clear_control,
    control_path,
    read_control,
    write_control,
)
from tests.metasmith.e2e.agentic.harness.loop import (
    LoopBudgets,
    LoopOutcome,
    LoopResult,
    ralph_loop,
)


# ---------------------------------------------------------------------------
# Stub driver: scripted per-iteration behavior
# ---------------------------------------------------------------------------


@dataclass
class ScriptedStep:
    tokens_in: int = 100
    tokens_out: int = 50
    final_text: str = ""
    # callback receives the sandbox path so the step can simulate the agent
    # writing CONTROL.json this iteration
    side_effect: Callable[[Path], None] | None = None


@dataclass
class StubDriver:
    name: str = "stub"
    model: str = "stub-model"
    steps: list[ScriptedStep] = field(default_factory=list)
    invocations: list[Path] = field(default_factory=list)
    started: int = 0
    stopped: int = 0
    _i: int = 0

    def start_session(self, env: dict[str, str] | None = None) -> None:
        self.started += 1
        self.start_env = env

    def stop_session(self) -> None:
        self.stopped += 1

    def invoke(self, *, prompt, sandbox, env, max_tokens_per_iter, log_dir,
               max_usd_per_iter=None) -> IterResult:
        if self._i >= len(self.steps):
            raise AssertionError("StubDriver invoked more times than scripted")
        step = self.steps[self._i]
        self._i += 1
        self.invocations.append(sandbox)
        if step.side_effect is not None:
            step.side_effect(sandbox)
        return IterResult(
            exit_code=0,
            tokens_in=step.tokens_in,
            tokens_out=step.tokens_out,
            final_text=step.final_text,
            transcript_path=None,
            duration_s=0.0,
        )


# ---------------------------------------------------------------------------
# control.py
# ---------------------------------------------------------------------------


def test_control_round_trip_done(tmp_path: Path) -> None:
    write_control(tmp_path, "done", task_key="abc123", notes="ok")
    c = read_control(tmp_path)
    assert isinstance(c, Control)
    assert c.action == "done"
    assert c.task_key == "abc123"
    assert c.notes == "ok"


def test_control_round_trip_give_up(tmp_path: Path) -> None:
    write_control(tmp_path, "give_up", reason="stuck")
    c = read_control(tmp_path)
    assert c is not None
    assert c.action == "give_up"
    assert c.reason == "stuck"


def test_control_round_trip_report_issue(tmp_path: Path) -> None:
    write_control(tmp_path, "report_issue", reason="docker tag invalid")
    c = read_control(tmp_path)
    assert c is not None
    assert c.action == "report_issue"
    assert c.reason == "docker tag invalid"


def test_control_missing_returns_none(tmp_path: Path) -> None:
    assert read_control(tmp_path) is None


def test_control_malformed_returns_none(tmp_path: Path) -> None:
    control_path(tmp_path).write_text("{not json")
    assert read_control(tmp_path) is None


def test_control_unknown_action_returns_none(tmp_path: Path) -> None:
    control_path(tmp_path).write_text(json.dumps({"action": "explode"}))
    assert read_control(tmp_path) is None


def test_control_non_dict_returns_none(tmp_path: Path) -> None:
    control_path(tmp_path).write_text(json.dumps(["done"]))
    assert read_control(tmp_path) is None


def test_clear_control_is_idempotent(tmp_path: Path) -> None:
    clear_control(tmp_path)  # no file yet
    write_control(tmp_path, "continue")
    clear_control(tmp_path)
    assert not control_path(tmp_path).exists()


def test_write_control_rejects_bad_action(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        write_control(tmp_path, "explode")


# ---------------------------------------------------------------------------
# budget.py
# ---------------------------------------------------------------------------


def test_budget_consume_and_exhaust() -> None:
    b = TokenBudget(limit=300)
    b.consume(100)
    assert not b.exhausted()
    assert b.remaining == 200
    b.consume(200)
    assert b.exhausted()
    assert b.remaining == 0


def test_budget_rejects_negative() -> None:
    b = TokenBudget(limit=100)
    with pytest.raises(ValueError):
        b.consume(-1)


# ---------------------------------------------------------------------------
# loop.py — one test per stop condition
# ---------------------------------------------------------------------------


def _writes_control(action: str, **fields) -> Callable[[Path], None]:
    def _impl(sandbox: Path) -> None:
        write_control(sandbox, action, **fields)
    return _impl


def _loop(
    tmp_path: Path,
    steps: list[ScriptedStep],
    *,
    max_iters: int = 5,
    max_tokens: int = 10_000,
) -> tuple[LoopResult, StubDriver]:
    driver = StubDriver(steps=steps)
    sandbox = tmp_path / "sandbox"
    log_dir = tmp_path / "logs"
    result = ralph_loop(
        driver=driver,
        prompt="follow the tutorial",
        sandbox=sandbox,
        budgets=LoopBudgets(
            max_iters=max_iters,
            max_tokens=max_tokens,
            max_tokens_per_iter=1_000,
        ),
        log_dir=log_dir,
    )
    return result, driver


def test_loop_done_on_first_iteration(tmp_path: Path) -> None:
    steps = [
        ScriptedStep(side_effect=_writes_control("done", task_key="t1")),
    ]
    result, driver = _loop(tmp_path, steps)
    assert result.outcome is LoopOutcome.DONE
    assert result.succeeded
    assert result.iterations == 1
    assert result.terminal_control is not None
    assert result.terminal_control.task_key == "t1"
    assert driver.started == 1 and driver.stopped == 1


def test_loop_done_after_several_continues(tmp_path: Path) -> None:
    steps = [
        ScriptedStep(),  # no control written -> implicit continue
        ScriptedStep(),
        ScriptedStep(side_effect=_writes_control("done", task_key="t2")),
    ]
    result, _ = _loop(tmp_path, steps)
    assert result.outcome is LoopOutcome.DONE
    assert result.iterations == 3
    assert result.terminal_control is not None
    assert result.terminal_control.task_key == "t2"


def test_loop_give_up(tmp_path: Path) -> None:
    steps = [
        ScriptedStep(),
        ScriptedStep(side_effect=_writes_control("give_up", reason="confused")),
    ]
    result, _ = _loop(tmp_path, steps)
    assert result.outcome is LoopOutcome.GAVE_UP
    assert not result.succeeded
    assert result.iterations == 2
    assert result.terminal_control is not None
    assert result.terminal_control.reason == "confused"


def test_loop_report_issue_terminates_on_first_iter(tmp_path: Path) -> None:
    steps = [
        ScriptedStep(side_effect=_writes_control(
            "report_issue", reason="docker rejects '+' in tag")),
        # Extra step that would crash the StubDriver if the loop kept going.
    ]
    result, driver = _loop(tmp_path, steps)
    assert result.outcome is LoopOutcome.REPORTED_ISSUE
    assert not result.succeeded
    assert result.iterations == 1
    assert len(driver.invocations) == 1
    assert result.terminal_control is not None
    assert result.terminal_control.reason == "docker rejects '+' in tag"


def test_loop_max_iters(tmp_path: Path) -> None:
    steps = [ScriptedStep() for _ in range(4)]
    result, driver = _loop(tmp_path, steps, max_iters=4)
    assert result.outcome is LoopOutcome.MAX_ITERS
    assert not result.succeeded
    assert result.iterations == 4
    assert len(driver.invocations) == 4


def test_loop_over_budget(tmp_path: Path) -> None:
    steps = [
        ScriptedStep(tokens_in=300, tokens_out=200),
        ScriptedStep(tokens_in=300, tokens_out=200),
        ScriptedStep(tokens_in=300, tokens_out=200),
    ]
    result, driver = _loop(tmp_path, steps, max_iters=10, max_tokens=1_000)
    assert result.outcome is LoopOutcome.OVER_BUDGET
    assert not result.succeeded
    # 500 + 500 = 1000 reaches the limit; loop must stop at iteration 2
    assert result.iterations == 2
    assert result.tokens_used == 1_000
    assert len(driver.invocations) == 2


def test_loop_malformed_control_is_treated_as_continue(tmp_path: Path) -> None:
    def _malformed(sandbox: Path) -> None:
        control_path(sandbox).write_text("{not json")
    steps = [
        ScriptedStep(side_effect=_malformed),
        ScriptedStep(side_effect=_writes_control("done", task_key="t3")),
    ]
    result, _ = _loop(tmp_path, steps)
    assert result.outcome is LoopOutcome.DONE
    assert result.iterations == 2


def test_loop_clears_control_between_iterations(tmp_path: Path) -> None:
    """A previous iteration's CONTROL.json must not bleed into the next."""
    written_paths: list[Path] = []

    def _check_then_write(sandbox: Path) -> None:
        # the file should NOT exist at the start of any iteration
        assert not control_path(sandbox).exists()
        written_paths.append(control_path(sandbox))
        write_control(sandbox, "continue")

    def _finish(sandbox: Path) -> None:
        assert not control_path(sandbox).exists()
        write_control(sandbox, "done", task_key="cleared")

    steps = [
        ScriptedStep(side_effect=_check_then_write),
        ScriptedStep(side_effect=_check_then_write),
        ScriptedStep(side_effect=_finish),
    ]
    result, _ = _loop(tmp_path, steps)
    assert result.outcome is LoopOutcome.DONE
    assert len(written_paths) == 2


def test_loop_writes_prompt_md(tmp_path: Path) -> None:
    steps = [ScriptedStep(side_effect=_writes_control("done", task_key="t"))]
    _loop(tmp_path, steps)
    assert (tmp_path / "sandbox" / "PROMPT.md").read_text() == "follow the tutorial"


def test_loop_session_lifecycle(tmp_path: Path) -> None:
    """start_session and stop_session are called exactly once even on early exit."""
    steps = [ScriptedStep(side_effect=_writes_control("done", task_key="t"))]
    _, driver = _loop(tmp_path, steps)
    assert driver.started == 1
    assert driver.stopped == 1


# ---------------------------------------------------------------------------
# CLI round-trip — confirms `metasmith e2e checkpoint` writes a payload that
# read_control accepts. Uses the in-process main() entry point so we don't
# depend on the installed console_scripts shim.
# ---------------------------------------------------------------------------


def test_cli_checkpoint_done_round_trip(tmp_path: Path) -> None:
    from metasmith.coms.cli._main import main

    rc = main(["--quiet", "e2e", "checkpoint", "done", "--key", "round-trip",
               "--notes", "ok", "--cwd", str(tmp_path)])
    assert rc == 0
    c = read_control(tmp_path)
    assert c is not None and c.action == "done"
    assert c.task_key == "round-trip"
    assert c.notes == "ok"


def test_cli_checkpoint_give_up_round_trip(tmp_path: Path) -> None:
    from metasmith.coms.cli._main import main

    rc = main(["--quiet", "e2e", "checkpoint", "give_up", "--reason", "stuck",
               "--cwd", str(tmp_path)])
    assert rc == 0
    c = read_control(tmp_path)
    assert c is not None and c.action == "give_up"
    assert c.reason == "stuck"


def test_cli_checkpoint_done_without_key_errors(tmp_path: Path) -> None:
    from metasmith.coms.cli._main import main

    with pytest.raises(SystemExit):
        main(["--quiet", "e2e", "checkpoint", "done", "--cwd", str(tmp_path)])


def test_cli_report_issue_round_trip(tmp_path: Path) -> None:
    from metasmith.coms.cli._main import main

    rc = main(["--quiet", "e2e", "report_issue",
               "--reason", "agent saw EnvironmentNameNotFound",
               "--cwd", str(tmp_path)])
    assert rc == 0
    c = read_control(tmp_path)
    assert c is not None and c.action == "report_issue"
    assert c.reason == "agent saw EnvironmentNameNotFound"


def test_cli_report_issue_without_reason_errors(tmp_path: Path) -> None:
    from metasmith.coms.cli._main import main

    with pytest.raises(SystemExit):
        main(["--quiet", "e2e", "report_issue", "--cwd", str(tmp_path)])


def test_loop_stop_session_called_on_exception(tmp_path: Path) -> None:
    class ExplodingDriver(StubDriver):
        def invoke(self, **kw):  # type: ignore[override]
            self.started_invoked = True
            raise RuntimeError("kaboom")

    driver = ExplodingDriver(steps=[ScriptedStep()])
    with pytest.raises(RuntimeError, match="kaboom"):
        ralph_loop(
            driver=driver,
            prompt="x",
            sandbox=tmp_path / "sb",
            budgets=LoopBudgets(max_iters=1, max_tokens=100, max_tokens_per_iter=10),
            log_dir=tmp_path / "lg",
        )
    assert driver.started == 1
    assert driver.stopped == 1
