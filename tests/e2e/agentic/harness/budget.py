"""Token budget for the ralph loop.

The loop accumulates tokens reported by the driver after each iteration
and stops as soon as the cumulative billable total reaches ``limit``.

``used`` (the stop-condition quantity) is the true billable footprint —
fresh input + cache reads + cache writes + output — supplied by the caller
via ``IterResult.tokens_total``. On top of that, the budget also keeps a
four-way running breakdown (in / out / cache_read / cache_creation) purely
for reporting; the stop semantics are unchanged from the old in+out world.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class TokenBudget:
    limit: int
    used: int = 0
    # Four-way running breakdown (reporting only; does not affect stop).
    tokens_in: int = 0
    tokens_out: int = 0
    tokens_cached: int = 0
    tokens_cache_creation: int = 0

    def consume(self, n: int) -> None:
        if n < 0:
            raise ValueError(f"token count must be non-negative, got {n}")
        self.used += n

    def record(self, result) -> None:
        """Fold one iteration's usage into the budget.

        Duck-typed on ``IterResult``: reads the four split counts for the
        running breakdown, then charges ``tokens_total`` against the limit
        (the single stop quantity). Kept separate from ``consume`` so the
        raw-int path stays available and unchanged.
        """
        self.tokens_in += result.tokens_in
        self.tokens_out += result.tokens_out
        self.tokens_cached += getattr(result, "tokens_cached", 0)
        self.tokens_cache_creation += getattr(result, "tokens_cache_creation", 0)
        self.consume(result.tokens_total)

    def exhausted(self) -> bool:
        return self.used >= self.limit

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.used)
