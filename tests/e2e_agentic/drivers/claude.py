"""ClaudeDriver — invokes the Claude Code CLI in non-interactive mode.

Argv pattern follows the verified production pattern in
``/home/tony/agentic_workspace/awm/services/agent_instances.py:136-152``.

Two harness-specific quirks the production pattern doesn't have to worry
about:

1. ``--add-dir`` is variadic (``<directories...>``). When the harness
   redirects HOME, the sandbox path is the only ``--add-dir`` we pass.
   The argv must place ``--add-dir`` BEFORE a non-variadic named flag
   (e.g. ``--model``) so the trailing positional ``prompt`` argument is
   NOT swallowed as another directory. The original ordering put
   ``--add-dir`` immediately before ``prompt`` and silently lost the
   prompt every iteration with a generic ``Input must be provided``
   error.

2. The harness sets ``HOME=<sandbox>/home`` so the agent's tool calls run
   inside the sandbox. Claude Code's OAuth credentials live at
   ``~/.claude/.credentials.json`` — under the redirected HOME that
   path is empty and the CLI exits with ``authentication_failed``.
   ``start_session`` symlinks the host credentials into the sandbox
   HOME, mirroring the opencode driver's auth-bridging pattern.
"""
from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from .base import IterResult
from ._stream import parse_claude_stream, run_streaming


_CLAUDE_CRED_REL = Path(".claude") / ".credentials.json"


def _bridge_claude_auth(sandbox_home: Path) -> None:
    """Symlink the real ``~/.claude/.credentials.json`` into a redirected
    HOME so the Claude CLI keeps its OAuth session. Idempotent."""
    real = Path.home() / _CLAUDE_CRED_REL
    if not real.exists():
        return
    dest = sandbox_home / _CLAUDE_CRED_REL
    if dest.exists() or dest.is_symlink():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.symlink_to(real)


@dataclass
class ClaudeDriver:
    name: str = "claude"
    model: str = "haiku"
    effort: str | None = "medium"
    bin: str = "claude"
    add_dirs: list[Path] = field(default_factory=list)
    extra_argv: list[str] = field(default_factory=list)
    timeout_s: float | None = None

    def start_session(self, env: dict[str, str] | None = None) -> None:
        if shutil.which(self.bin) is None:
            raise RuntimeError(
                f"`{self.bin}` not found on PATH; install Claude Code first"
            )
        if env is not None and "HOME" in env:
            _bridge_claude_auth(Path(env["HOME"]))

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
        ]
        # --add-dir is variadic; it must come BEFORE the next named flag
        # so the trailing positional prompt isn't absorbed as a directory.
        for d in [sandbox, *self.add_dirs]:
            argv += ["--add-dir", str(d)]
        argv += ["--model", self.model]
        if self.effort:
            argv += ["--effort", self.effort]
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
