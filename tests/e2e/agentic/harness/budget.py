"""Token budget for the ralph loop.

The loop accumulates tokens reported by the driver after each iteration
and stops as soon as the cumulative total reaches ``limit``. Tracking is
deliberately simple: in + out are summed; we don't try to distinguish
cache reads/writes here.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class TokenBudget:
    limit: int
    used: int = 0

    def consume(self, n: int) -> None:
        if n < 0:
            raise ValueError(f"token count must be non-negative, got {n}")
        self.used += n

    def exhausted(self) -> bool:
        return self.used >= self.limit

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.used)
