from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from tests.metasmith.e2e.agentic.drivers.base import AgentDriver, IterResult

from .budget import TokenBudget
from .control import Control, clear_control, read_control


class LoopOutcome(Enum):
    SUBMITTED = "submitted"
    DONE = "done"
    GAVE_UP = "gave_up"
    REPORTED_ISSUE = "reported_issue"
    OVER_BUDGET = "over_budget"
    MAX_ITERS = "max_iters"


@dataclass
class LoopBudgets:
    max_iters: int = 20
    max_tokens: int = 2_000_000
    max_tokens_per_iter: int = 200_000
    max_usd_per_iter: float | None = None


@dataclass
class LoopResult:
    outcome: LoopOutcome
    iterations: int
    tokens_used: int
    last_iter: IterResult | None
    terminal_control: Control | None
    iter_results: list[IterResult] = field(default_factory=list)
    tokens_in: int = 0
    tokens_out: int = 0
    tokens_cached: int = 0
    tokens_cache_creation: int = 0

    @property
    def succeeded(self) -> bool:
        return self.outcome is LoopOutcome.DONE


def _token_rollup(budget: TokenBudget) -> dict:
    return dict(
        tokens_in=budget.tokens_in,
        tokens_out=budget.tokens_out,
        tokens_cached=budget.tokens_cached,
        tokens_cache_creation=budget.tokens_cache_creation,
    )


def ralph_loop(
    *,
    driver: AgentDriver,
    prompt: str,
    sandbox: Path,
    budgets: LoopBudgets,
    log_dir: Path,
    env: dict[str, str] | None = None,
) -> LoopResult:
    sandbox.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    (sandbox / "PROMPT.md").write_text(prompt)
    clear_control(sandbox)

    budget = TokenBudget(budgets.max_tokens)
    iter_results: list[IterResult] = []
    env = dict(env or {})

    driver.start_session(env=env)
    try:
        for i in range(budgets.max_iters):
            iter_log = log_dir / f"iter-{i:03d}"
            iter_log.mkdir(parents=True, exist_ok=True)

            result = driver.invoke(
                prompt=prompt,
                sandbox=sandbox,
                env=env,
                max_tokens_per_iter=budgets.max_tokens_per_iter,
                max_usd_per_iter=budgets.max_usd_per_iter,
                log_dir=iter_log,
            )
            iter_results.append(result)
            budget.record(result)

            control = read_control(sandbox)
            if control is not None and control.action == "submit":
                return LoopResult(
                    outcome=LoopOutcome.SUBMITTED,
                    iterations=i + 1,
                    tokens_used=budget.used,
                    last_iter=result,
                    terminal_control=control,
                    iter_results=iter_results,
                    **_token_rollup(budget),
                )
            if control is not None and control.action == "done":
                return LoopResult(
                    outcome=LoopOutcome.DONE,
                    iterations=i + 1,
                    tokens_used=budget.used,
                    last_iter=result,
                    terminal_control=control,
                    iter_results=iter_results,
                    **_token_rollup(budget),
                )
            if control is not None and control.action == "give_up":
                return LoopResult(
                    outcome=LoopOutcome.GAVE_UP,
                    iterations=i + 1,
                    tokens_used=budget.used,
                    last_iter=result,
                    terminal_control=control,
                    iter_results=iter_results,
                    **_token_rollup(budget),
                )
            if control is not None and control.action == "report_issue":
                return LoopResult(
                    outcome=LoopOutcome.REPORTED_ISSUE,
                    iterations=i + 1,
                    tokens_used=budget.used,
                    last_iter=result,
                    terminal_control=control,
                    iter_results=iter_results,
                    **_token_rollup(budget),
                )

            if budget.exhausted():
                return LoopResult(
                    outcome=LoopOutcome.OVER_BUDGET,
                    iterations=i + 1,
                    tokens_used=budget.used,
                    last_iter=result,
                    terminal_control=control,
                    iter_results=iter_results,
                    **_token_rollup(budget),
                )

            clear_control(sandbox)

        return LoopResult(
            outcome=LoopOutcome.MAX_ITERS,
            iterations=budgets.max_iters,
            tokens_used=budget.used,
            last_iter=iter_results[-1] if iter_results else None,
            terminal_control=None,
            iter_results=iter_results,
            **_token_rollup(budget),
        )
    finally:
        driver.stop_session()
