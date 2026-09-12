from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass
class IterResult:
    exit_code: int
    tokens_in: int
    tokens_out: int
    final_text: str
    transcript_path: Path | None
    duration_s: float
    # Three-way token split beyond in/out. Default 0 so drivers that don't
    # report a cache split (opencode) and existing call sites keep working.
    tokens_cached: int = 0
    tokens_cache_creation: int = 0
    extra: dict = field(default_factory=dict)

    @property
    def tokens_total(self) -> int:
        # The loop's budget must count the true billable footprint of the
        # iteration: fresh input + cache reads + cache writes + output. Cache
        # reads are billed (at a discount) and cache creation is billed at a
        # premium, so both belong in the stop-condition total, not just in/out.
        return (
            self.tokens_in
            + self.tokens_out
            + self.tokens_cached
            + self.tokens_cache_creation
        )


@runtime_checkable
class AgentDriver(Protocol):
    name: str
    model: str

    def start_session(self, env: dict[str, str] | None = None) -> None:
        ...

    def stop_session(self) -> None:
        ...

    def invoke(
        self,
        *,
        prompt: str,
        sandbox: Path,
        env: dict[str, str],
        max_tokens_per_iter: int,
        log_dir: Path,
        max_usd_per_iter: float | None = None,
    ) -> IterResult:
        ...
