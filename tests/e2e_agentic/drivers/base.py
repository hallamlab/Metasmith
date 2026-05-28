"""AgentDriver protocol — the swappable per-iteration agent invocation."""
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
    extra: dict = field(default_factory=dict)

    @property
    def tokens_total(self) -> int:
        return self.tokens_in + self.tokens_out


@runtime_checkable
class AgentDriver(Protocol):
    name: str
    model: str

    def start_session(self, env: dict[str, str] | None = None) -> None:
        """Open the driver's session. For drivers with a persistent helper
        process (e.g. ``opencode serve``), the env passed here is the env
        the helper process — and any tool-call subprocesses it spawns —
        will inherit. Drivers that spawn fresh per iteration may ignore it.
        """
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
    ) -> IterResult:
        ...
