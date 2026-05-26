"""ClaudeDriver — invokes the Claude Code CLI in non-interactive mode.

Argv pattern follows the verified production pattern in
``/home/tony/agentic_workspace/awm/services/agent_instances.py:136-152``.
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path

from .base import IterResult
from ._stream import parse_claude_stream, run_streaming


@dataclass
class ClaudeDriver:
    name: str = "claude"
    model: str = "haiku"
    effort: str | None = "medium"
    bin: str = "claude"
    add_dirs: list[Path] = field(default_factory=list)
    extra_argv: list[str] = field(default_factory=list)
    timeout_s: float | None = None

    def start_session(self) -> None:
        if shutil.which(self.bin) is None:
            raise RuntimeError(
                f"`{self.bin}` not found on PATH; install Claude Code first"
            )

    def stop_session(self) -> None:
        return

    def invoke(
        self,
        *,
        prompt: str,
        sandbox: Path,
        env: dict[str, str],
        max_tokens_per_iter: int,
        log_dir: Path,
    ) -> IterResult:
        argv = [
            self.bin,
            "--print",
            "--verbose",
            "--input-format=text",
            "--output-format=stream-json",
            "--include-partial-messages",
            "--permission-mode=bypassPermissions",
            "--model", self.model,
        ]
        if self.effort:
            argv += ["--effort", self.effort]
        for d in [sandbox, *self.add_dirs]:
            argv += ["--add-dir", str(d)]
        argv += self.extra_argv
        argv.append(prompt)

        transcript = log_dir / "stream.jsonl"
        exit_code, duration = run_streaming(
            argv, cwd=sandbox, env=env,
            transcript_path=transcript,
            timeout_s=self.timeout_s,
        )
        summary = parse_claude_stream(transcript)
        return IterResult(
            exit_code=exit_code,
            tokens_in=summary.tokens_in,
            tokens_out=summary.tokens_out,
            final_text=summary.final_text,
            transcript_path=transcript,
            duration_s=duration,
            extra={"raw_events": summary.raw_events, "argv": argv},
        )
