from __future__ import annotations

from dataclasses import dataclass


@dataclass
class TokenBudget:
    limit: int
    used: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    tokens_cached: int = 0
    tokens_cache_creation: int = 0

    def consume(self, n: int) -> None:
        if n < 0:
            raise ValueError(f"token count must be non-negative, got {n}")
        self.used += n

    def record(self, result) -> None:
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
