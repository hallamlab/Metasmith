"""Unit tests for the ralph loop's budget stop — no `claude`/`opencode` spawned.

The key guarantee: the cumulative TOKEN quota is the exact DNF trigger, and a
single over-quota iteration stops the loop after that one iteration (the
overshoot is bounded to one now-dollar-capped invocation, not runaway).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from tests.metasmith.e2e.agentic.drivers.base import IterResult
from tests.metasmith.e2e.agentic.harness.loop import LoopBudgets, LoopOutcome, ralph_loop


@dataclass
class _FakeDriver:
    """Returns a fixed IterResult each invoke; writes no CONTROL.json."""
    name: str = "fake"
    model: str = "haiku"
    per_iter: IterResult = field(default=None)  # set in test
    calls: int = 0

    def start_session(self, env=None) -> None:
        return None

    def stop_session(self) -> None:
        return None

    def invoke(self, *, prompt, sandbox, env, max_tokens_per_iter,
               log_dir, max_usd_per_iter=None) -> IterResult:
        self.calls += 1
        return self.per_iter


def _iter(total_tokens: int) -> IterResult:
    return IterResult(
        exit_code=0, tokens_in=total_tokens, tokens_out=0,
        tokens_cached=0, tokens_cache_creation=0,
        final_text="", transcript_path=None, duration_s=0.0,
    )


def test_over_quota_stops_after_one_iteration(tmp_path: Path) -> None:
    # One invocation burns 4M tokens against a 3M quota -> stop at iter 1.
    driver = _FakeDriver(per_iter=_iter(4_000_000))
    result = ralph_loop(
        driver=driver,
        prompt="go",
        sandbox=tmp_path / "sb",
        budgets=LoopBudgets(max_iters=20, max_tokens=3_000_000),
        log_dir=tmp_path / "logs",
    )
    assert result.outcome is LoopOutcome.OVER_BUDGET
    assert result.iterations == 1          # bounded overshoot: one iteration
    assert driver.calls == 1               # loop did not re-invoke past the quota
    assert result.tokens_used == 4_000_000


def test_under_quota_runs_to_max_iters(tmp_path: Path) -> None:
    # Small per-iter usage, no CONTROL.json -> exhausts max_iters, not budget.
    driver = _FakeDriver(per_iter=_iter(1_000))
    result = ralph_loop(
        driver=driver,
        prompt="go",
        sandbox=tmp_path / "sb",
        budgets=LoopBudgets(max_iters=3, max_tokens=3_000_000),
        log_dir=tmp_path / "logs",
    )
    assert result.outcome is LoopOutcome.MAX_ITERS
    assert result.iterations == 3
    assert driver.calls == 3
