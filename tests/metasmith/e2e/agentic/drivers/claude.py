from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from .base import IterResult
from ._stream import parse_claude_stream, run_streaming
from ..harness import jail as _jail


_CLAUDE_CRED_REL = Path(".claude") / ".credentials.json"

# Per-model USD/MTok output rate — the most expensive per-token axis, used to
# turn a per-iteration TOKEN budget into a per-invocation DOLLAR cap for the
# `claude` CLI's `--max-budget-usd`. The CLI offers no `--max-turns` or token
# cap, so the dollar cap is the only per-invocation runaway valve available.
# It is deliberately approximate (a legit 200k-token iteration is a few dimes
# of output; a multi-million-token single-shot session crosses the cap) — the
# EXACT stop stays the cumulative token quota checked between iterations
# (harness/loop.py). Cache-read-heavy runaways cost little in dollars, so the
# token quota, not this cap, catches those; this cap catches output/fresh-input
# heavy runaways. Haiku 4.5: input $1.00, output $5.00, cache-write $1.25,
# cache-read $0.10 per MTok (verified via claude-api skill, 2026-07-17).
_OUTPUT_USD_PER_MTOK = {
    "haiku": 5.0,
}
# Headroom so an honest iteration (mostly cache reads + a little output) never
# trips the valve; only a session an order of magnitude past the token budget does.
_USD_SAFETY_FACTOR = 2.0


def _derive_usd_cap(max_tokens_per_iter: int, model: str) -> float | None:
    rate = _OUTPUT_USD_PER_MTOK.get(model)
    if rate is None or max_tokens_per_iter <= 0:
        return None
    return max_tokens_per_iter / 1_000_000 * rate * _USD_SAFETY_FACTOR


def _bridge_claude_auth(sandbox_home: Path) -> None:
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
    jail: bool | None = None
    jail_unshare_user: bool = False

    def _should_jail(self, env: dict[str, str] | None) -> bool:
        if self.jail is not None:
            return self.jail
        return _jail.jail_enabled(env)

    def start_session(self, env: dict[str, str] | None = None) -> None:
        if shutil.which(self.bin) is None:
            raise RuntimeError(
                f"`{self.bin}` not found on PATH; install Claude Code first"
            )
        # When jailing, the host ~/.claude is bind-mounted live into the sandbox
        # HOME (shared, rotating auth) — no copy/symlink. The copy-symlink bridge
        # is the redirection-only fallback (docker dev / no-bwrap hosts), where
        # each sandbox gets its own view of the credentials file.
        if (env is not None and "HOME" in env and not self._should_jail(env)):
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
        max_usd_per_iter: float | None = None,
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
        # Per-invocation runaway valve: explicit override wins, else derive from
        # the per-iteration token budget. `--max-budget-usd` takes exactly one
        # value, so it is safe here (after --effort, before the trailing prompt).
        usd_cap = (
            max_usd_per_iter
            if max_usd_per_iter is not None
            else _derive_usd_cap(max_tokens_per_iter, self.model)
        )
        if usd_cap is not None and usd_cap > 0:
            argv += ["--max-budget-usd", f"{usd_cap:.4f}"]
        argv += self.extra_argv
        argv.append(prompt)

        # Filesystem jail: wrap the claude argv in a bwrap namespace that binds
        # the sandbox rw + system dirs ro + the shared ~/.claude auth, so the
        # bypass-permissions agent cannot read host paths outside the allow-list.
        if self._should_jail(env):
            home = Path(env.get("HOME", sandbox / "home"))
            argv = _jail.wrap(
                argv,
                sandbox=sandbox,
                home=home,
                env=env,
                extra_ro=_jail.extra_ro_from_env(env),
                claude_bin=shutil.which(self.bin),
                chdir=sandbox,
                unshare_user=self.jail_unshare_user,
            )

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
            tokens_cached=summary.tokens_cached,
            tokens_cache_creation=summary.tokens_cache_creation,
            extra={"raw_events": summary.raw_events, "argv": argv},
        )
