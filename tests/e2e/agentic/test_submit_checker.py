"""Unit tests for the submit/checker path (plan T2).

Covers: the ``submit`` control action + Control properties, the loop's
``SUBMITTED`` outcome, the non-agentic checker (metasmith + baseline + validate),
and the ``metasmith e2e submit`` CLI verb. No ``claude`` / real pipeline spawned
— the checker's subprocess is faked via the ``runner`` seam.
"""
from __future__ import annotations

from argparse import Namespace
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from tests.e2e.agentic.drivers.base import IterResult
from tests.e2e.agentic.harness import checker as C
from tests.e2e.agentic.harness.control import (
    SUBMIT_ACTIONS,
    VALID_ACTIONS,
    read_control,
    write_control,
)
from tests.e2e.agentic.harness.loop import (
    LoopBudgets,
    LoopOutcome,
    LoopResult,
    ralph_loop,
)
from tests.e2e.agentic.scenarios.arms import ARM_BY_ID
from tests.e2e.agentic.scenarios.base import VerifyContext


# ---------------------------------------------------------------------------
# control contract
# ---------------------------------------------------------------------------


def test_submit_is_valid_action() -> None:
    assert "submit" in VALID_ACTIONS
    assert SUBMIT_ACTIONS == {"submit", "done"}


def test_control_submit_properties(tmp_path: Path) -> None:
    write_control(tmp_path, "submit", task_key="k1", agent="/a/smith.yml")
    c = read_control(tmp_path)
    assert c is not None
    assert c.action == "submit"
    assert c.task_key == "k1"
    assert c.agent == "/a/smith.yml"
    assert c.is_submission is True


def test_control_baseline_submit(tmp_path: Path) -> None:
    write_control(tmp_path, "submit", entrypoint="workspace/run.sh")
    c = read_control(tmp_path)
    assert c.entrypoint == "workspace/run.sh"
    assert c.task_key is None


def test_control_done_still_submission(tmp_path: Path) -> None:
    write_control(tmp_path, "done", task_key="k")
    c = read_control(tmp_path)
    assert c.is_submission is True


# ---------------------------------------------------------------------------
# loop → SUBMITTED
# ---------------------------------------------------------------------------


@dataclass
class _SubmitDriver:
    name: str = "fake"
    model: str = "haiku"
    calls: int = 0

    def start_session(self, env=None) -> None:
        return None

    def stop_session(self) -> None:
        return None

    def invoke(self, *, prompt, sandbox, env, max_tokens_per_iter,
               log_dir, max_usd_per_iter=None) -> IterResult:
        self.calls += 1
        write_control(sandbox, "submit", task_key="tk", agent="smith")
        return IterResult(exit_code=0, tokens_in=10, tokens_out=0,
                          final_text="", transcript_path=None, duration_s=0.0)


def test_loop_stops_on_submit(tmp_path: Path) -> None:
    driver = _SubmitDriver()
    result = ralph_loop(
        driver=driver, prompt="go", sandbox=tmp_path / "sb",
        budgets=LoopBudgets(max_iters=5, max_tokens=10_000_000),
        log_dir=tmp_path / "logs",
    )
    assert result.outcome is LoopOutcome.SUBMITTED
    assert result.iterations == 1
    assert driver.calls == 1
    assert result.terminal_control.task_key == "tk"


# ---------------------------------------------------------------------------
# checker — fakeable runner
# ---------------------------------------------------------------------------


@dataclass
class _FakeProc:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class _Runner:
    """Records argv; returns a scripted rc per (label inferred from argv)."""
    def __init__(self, rc_by_key=None, stdout_by_key=None):
        self.calls: list[list[str]] = []
        self.rc_by_key = rc_by_key or {}
        self.stdout_by_key = stdout_by_key or {}

    def __call__(self, argv, *, cwd, env, timeout_s):
        self.calls.append(argv)
        key = self._key(argv)
        return _FakeProc(self.rc_by_key.get(key, 0),
                         self.stdout_by_key.get(key, ""))

    @staticmethod
    def _key(argv) -> str:
        if "workflow" in argv:
            i = argv.index("workflow")
            return argv[i + 1]           # run | wait | collect | result-source
        if "data" in argv and "load-remote" in argv:
            return "load-remote"
        return argv[0]


def _vctx(tmp_path: Path, arm_id: str) -> VerifyContext:
    sb = tmp_path / "sb"
    (sb / "workspace").mkdir(parents=True, exist_ok=True)
    return VerifyContext(
        sandbox=sb,
        agent_env={"HOME": str(sb / "home")},
        metasmith_env_name="msm_env",
        installed_env_path=sb / "envs" / "msm_env",
        arm=ARM_BY_ID[arm_id],
    )


def _submitted_result(sandbox: Path, **payload) -> LoopResult:
    write_control(sandbox, "submit", **payload)
    ctrl = read_control(sandbox)
    return LoopResult(outcome=LoopOutcome.SUBMITTED, iterations=1,
                      tokens_used=0, last_iter=None, terminal_control=ctrl)


def test_checker_skips_without_submission(tmp_path: Path) -> None:
    vctx = _vctx(tmp_path, "A10")
    res = LoopResult(outcome=LoopOutcome.MAX_ITERS, iterations=1,
                     tokens_used=0, last_iter=None, terminal_control=None)
    out = C.run_checker(vctx, res, runner=_Runner())
    assert out.kind == "skipped" and out.ok


def test_checker_validate_is_noop(tmp_path: Path) -> None:
    vctx = _vctx(tmp_path, "A10")
    res = _submitted_result(vctx.sandbox, task_key="k", agent="smith")
    r = _Runner()
    out = C.run_checker(vctx, res, action="validate", runner=r)
    assert out.ok and out.kind == "validate"
    assert r.calls == []                 # nothing executed


def test_checker_metasmith_happy_path(tmp_path: Path) -> None:
    vctx = _vctx(tmp_path, "A10")
    res = _submitted_result(vctx.sandbox, task_key="tk", agent="/a/smith.yml")
    runner = _Runner(stdout_by_key={"result-source": '{"address": "file:///r"}'})
    out = C.run_checker(vctx, res, resource_overrides={"spades": {"cpus": 8}},
                        runner=runner)
    assert out.ok and out.kind == "metasmith_run"
    # run → wait → collect → result-source → load-remote, in order.
    order = [runner._key(a) for a in runner.calls]
    assert order == ["run", "wait", "collect", "result-source", "load-remote"]
    # the SPAdes override rode on `workflow run`.
    run_argv = runner.calls[0]
    assert "--override" in run_argv and "spades=cpus:8" in run_argv


def test_checker_metasmith_missing_agent(tmp_path: Path) -> None:
    vctx = _vctx(tmp_path, "A10")
    res = _submitted_result(vctx.sandbox, task_key="tk")   # no agent
    out = C.run_checker(vctx, res, runner=_Runner())
    assert not out.ok
    assert any("agent" in f for f in out.failures)


def test_checker_metasmith_run_fails(tmp_path: Path) -> None:
    vctx = _vctx(tmp_path, "A10")
    res = _submitted_result(vctx.sandbox, task_key="tk", agent="smith")
    runner = _Runner(rc_by_key={"run": 1})
    out = C.run_checker(vctx, res, runner=runner)
    assert not out.ok
    assert any("workflow run" in f for f in out.failures)
    assert [runner._key(a) for a in runner.calls] == ["run"]   # stopped early


def test_checker_baseline_runs_entrypoint(tmp_path: Path) -> None:
    vctx = _vctx(tmp_path, "A7")            # container / ad-hoc → run.sh
    ep = vctx.sandbox / "workspace" / "run.sh"
    ep.write_text("#!/bin/sh\necho hi\n")
    res = _submitted_result(vctx.sandbox, entrypoint=str(ep))
    runner = _Runner()
    out = C.run_checker(vctx, res, runner=runner)
    assert out.ok and out.kind == "baseline_run"
    cmd = runner.calls[0]
    assert cmd[0] == "bash" and cmd[1] == str(ep)


def test_checker_baseline_missing_entrypoint(tmp_path: Path) -> None:
    vctx = _vctx(tmp_path, "A7")
    res = _submitted_result(vctx.sandbox)          # no entrypoint
    out = C.run_checker(vctx, res, runner=_Runner())
    assert not out.ok and any("entrypoint" in f for f in out.failures)


def test_checker_baseline_command_inference(tmp_path: Path) -> None:
    assert C._baseline_command(Path("/x/run.sh"))[0] == "bash"
    assert C._baseline_command(Path("/x/Snakefile"))[0] == "snakemake"
    assert C._baseline_command(Path("/x/main.nf"))[:2] == ["nextflow", "run"]
    assert C._baseline_command(Path("/x/weird.txt")) is None


def test_format_overrides() -> None:
    assert C._format_overrides(None) == []
    assert C._format_overrides({"spades": {"cpus": 8}}) == [
        "--override", "spades=cpus:8"]
    assert C._format_overrides({"s": {"cpus": 4, "memory_gb": 16}}) == [
        "--override", "s=cpus:4,memory_gb:16"]


# ---------------------------------------------------------------------------
# metasmith e2e submit CLI verb
# ---------------------------------------------------------------------------


def _submit_args(tmp_path, **kw) -> Namespace:
    base = dict(cwd=str(tmp_path), key=None, agent=None, entrypoint=None,
                notes=None)
    base.update(kw)
    return Namespace(**base)


def test_cli_submit_metasmith(tmp_path: Path) -> None:
    from metasmith.coms.cli.e2e import _submit
    agent_file = tmp_path / "smith.yml"
    agent_file.write_text("home: local\n")
    _submit(_submit_args(tmp_path, key="tk", agent=str(agent_file)))
    c = read_control(tmp_path)
    assert c.action == "submit" and c.task_key == "tk"
    assert c.agent == str(agent_file.resolve())    # resolved to absolute


def test_cli_submit_baseline(tmp_path: Path) -> None:
    from metasmith.coms.cli.e2e import _submit
    _submit(_submit_args(tmp_path, entrypoint="workspace/run.sh"))
    c = read_control(tmp_path)
    assert c.entrypoint == "workspace/run.sh"      # unresolved (does not exist)


def test_cli_submit_requires_key_or_entrypoint(tmp_path: Path) -> None:
    from metasmith.coms.cli.e2e import _submit
    with pytest.raises(SystemExit):
        _submit(_submit_args(tmp_path))
