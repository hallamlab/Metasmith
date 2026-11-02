from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from .base import IterResult
from ._stream import parse_opencode_stream, run_streaming


def _pick_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port: int, timeout_s: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.2)
    return False


_OPENCODE_DATA_REL = Path(".local") / "share" / "opencode"


def _bridge_opencode_auth(sandbox_home: Path) -> None:
    real = Path.home() / _OPENCODE_DATA_REL
    if not real.exists():
        return
    dest = sandbox_home / _OPENCODE_DATA_REL
    if dest.exists() or dest.is_symlink():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.symlink_to(real)


def _opencode_has_credentials() -> bool:
    if os.environ.get("OPENCODE_API_KEY"):
        return True
    auth_path = Path.home() / ".local" / "share" / "opencode" / "auth.json"
    if auth_path.is_file() and auth_path.stat().st_size > 2:
        return True
    for k in ("OPENROUTER_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY",
              "GROQ_API_KEY", "TOGETHER_API_KEY"):
        if os.environ.get(k):
            return True
    return False


@dataclass
class OpencodeDriver:
    name: str = "opencode"
    model: str = "openrouter/deepseek/deepseek-v4-flash"
    bin: str = "opencode"
    serve_port: int | None = None
    serve_log: Path | None = None
    extra_argv: list[str] = field(default_factory=list)
    # Per-iteration wall-clock cap. opencode is known to wedge silently on
    # provider-side permission gates (cross-`--dir` reads → external_directory
    # asks that pile up with no UI to answer them). A None timeout means the
    # serve loop sits in epoll_wait indefinitely; default to 15 min so a
    # wedged iteration becomes a recoverable iteration boundary.
    timeout_s: float | None = 900.0
    _serve_proc: subprocess.Popen | None = field(default=None, init=False, repr=False)

    def start_session(self, env: dict[str, str] | None = None) -> None:
        if shutil.which(self.bin) is None:
            raise RuntimeError(
                f"`{self.bin}` not found on PATH; install via "
                f"`curl -fsSL https://opencode.ai/install | bash`"
            )
        if not _opencode_has_credentials():
            raise RuntimeError(
                "opencode has no credentials configured; run `opencode auth login` "
                "or export OPENCODE_API_KEY / OPENROUTER_API_KEY"
            )
        if self.serve_port is None:
            self.serve_port = _pick_free_port()
        log = self.serve_log or Path.cwd() / f".opencode-serve-{self.serve_port}.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        self._serve_log_fp = log.open("w")  # type: ignore[attr-defined]
        # opencode's bash tool calls run inside the serve daemon's process
        # tree, so they inherit serve's env — NOT the env we pass to
        # `opencode run` later. The sandbox spoof (CONDARC, HOME redirect)
        # therefore has to be wired in here, at serve-launch time.
        serve_env = dict(env) if env is not None else None
        if serve_env is not None and "HOME" in serve_env:
            sandbox_home = Path(serve_env["HOME"])
            _bridge_opencode_auth(sandbox_home)
        self._serve_proc = subprocess.Popen(
            [self.bin, "serve", "--port", str(self.serve_port)],
            stdout=self._serve_log_fp,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=serve_env,
        )
        if not _wait_for_port(self.serve_port):
            self.stop_session()
            raise RuntimeError(
                f"opencode serve failed to bind on port {self.serve_port}; "
                f"see log: {log}"
            )

    def stop_session(self) -> None:
        proc = self._serve_proc
        if proc is None:
            return
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        finally:
            self._serve_proc = None
            fp = getattr(self, "_serve_log_fp", None)
            if fp is not None:
                fp.close()

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
            self.bin, "run",
            "--attach", f"http://127.0.0.1:{self.serve_port}",
            "--dir", str(sandbox),
            "-m", self.model,
            "--format", "json",
            "--dangerously-skip-permissions",
            *self.extra_argv,
            prompt,
        ]
        transcript = log_dir / "stream.jsonl"
        exit_code, duration = run_streaming(
            argv, cwd=sandbox, env=env,
            transcript_path=transcript,
            timeout_s=self.timeout_s,
        )
        summary = parse_opencode_stream(transcript)
        return IterResult(
            exit_code=exit_code,
            tokens_in=summary.tokens_in,
            tokens_out=summary.tokens_out,
            final_text=summary.final_text,
            transcript_path=transcript,
            duration_s=duration,
            tokens_cached=summary.tokens_cached,
            tokens_cache_creation=summary.tokens_cache_creation,
            extra={"raw_events": summary.raw_events, "argv": argv,
                   "serve_port": self.serve_port},
        )
