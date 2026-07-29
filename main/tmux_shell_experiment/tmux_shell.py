"""
TmuxShell — a tmux-backed re-implementation of metasmith's LiveShell, built to
test the hypothesis: "is tmux more robust than the in-band-marker PTY shell?"

It exposes the same public surface the LiveShell call sites use
(Exec / ExecAsync / AwaitDone / SubShell / RegisterOn* / Dispose, context
manager, ShellResult), so the comparison harness can drive both behind one
factory.

Design (and what it reveals about the hypothesis)
-------------------------------------------------
tmux owns the terminal: a private-socket session runs one bash pane, input is
delivered with `send-keys`, and the pane process survives even if this Python
process dies (a real robustness edge for hours-long `RunWorkflow` runs — you can
`tmux -L <sock> attach` and watch it).

But the *completion + exit-code* signal is the crux, and it splits exactly the
way the LiveShell git history predicted:

  * LOCAL path — robust, no polling. Each command is a tiny sourced script that
    ends by writing `\x1eDONE <token> <nonce> <rc>\x1e` to a dedicated control
    FIFO. A reader thread parses it and wakes AwaitDone. This is an out-of-band
    side channel — clean locally.

  * SSH SUB-SHELL path — the side channel cannot cross the boundary (the remote
    bash has no access to the local FIFO path, just as fd 5 could not traverse
    ssh in commit 7a8008f). So inside a SubShell we fall back to an IN-BAND
    sentinel scraped off the stdout stream — i.e. the exact mechanism LiveShell
    uses, reproduced here. tmux's `wait-for` is likewise local-server-only and
    racy for async fan-out, so it is deliberately not used for completion.

stdout/stderr stay separate (the pane's bash redirects to two FIFOs), so
ShellResult.out vs .err is preserved — the feature-parity question is answered,
not dodged.
"""

from __future__ import annotations
import os
import re
import shlex
import secrets
import subprocess
import tempfile
import time
from contextlib import contextmanager
from threading import Condition
from time import sleep
from typing import Callable

# conftest / harness puts src/ on sys.path
from metasmith.coms.ipc import (
    NonBlockingReader,
    GenerateId,
    RemoveTrailingNewline,
    RemoveLeadingIndent,
)
from metasmith.coms.terminals import ShellResult
from metasmith.logging import Log


class TmuxShell:
    _RS = "\x1e"
    _INIT_TIMEOUT = 5.0

    # DONE on the control FIFO (local path); SUB inline on stdout (sub-shell path).
    _DONE_RE = re.compile(
        r"\x1eDONE (?P<token>[0-9a-f]+) (?P<nonce>[A-Za-z0-9_]+) (?P<rc>-?\d+)\x1e"
    )
    _SUB_RE = re.compile(
        r"\x1eSUB (?P<token>[0-9a-f]+) (?P<nonce>[A-Za-z0-9_]+) (?P<rc>-?\d+)\x1e"
    )

    def __init__(self, tmux_bin: str = "tmux") -> None:
        self._tmux = tmux_bin
        self._token = secrets.token_hex(16)
        self._socket = f"msm_{self._token[:12]}"
        self._target = "msm:0.0"

        self._out_callbacks: list[Callable[[str], None]] = []
        self._err_callbacks: list[Callable[[str], None]] = []
        self._results: dict[str, int] = {}
        self._pending: set[str] = set()
        self._cond = Condition()
        self._closed = False
        self._depth = 0  # SubShell nesting; >0 routes Exec through the in-band path

        self._tmpdir: str | None = None
        self._fifos: dict[str, str] = {}
        self._rfds: dict[str, int] = {}
        self._wfds: dict[str, int] = {}  # keep-alive writers so FIFOs never EOF
        self._readers: list[NonBlockingReader] = []
        self._session_up = False

        try:
            self._tmpdir = tempfile.mkdtemp(prefix="tmuxshell_")
            for name in ("out", "err", "ctl"):
                p = os.path.join(self._tmpdir, f"{name}.fifo")
                os.mkfifo(p)
                self._fifos[name] = p
                # read end first (non-blocking open returns even with no writer),
                # then a persistent writer so a transient script-writer closing
                # never delivers EOF to the reader thread.
                self._rfds[name] = os.open(p, os.O_RDONLY | os.O_NONBLOCK)
                self._wfds[name] = os.open(p, os.O_WRONLY)

            self._readers.append(self._mk_reader("out", self._make_stdout_tee()))
            self._readers.append(self._mk_reader("err", self._make_plain_tee("err", self._err_callbacks)))
            self._readers.append(self._mk_reader("ctl", self._make_ctl_tee()))

            launch = "exec bash > {o} 2> {e}".format(
                o=shlex.quote(self._fifos["out"]),
                e=shlex.quote(self._fifos["err"]),
            )
            self._tmux_cmd("new-session", "-d", "-s", "msm", "-x", "220", "-y", "50", launch)
            self._session_up = True

            # init handshake: a no-op command must round-trip the FIFO pipeline.
            rc = self.Exec("true", timeout=self._INIT_TIMEOUT, quiet=True).exit_code
            if rc is None:
                raise RuntimeError(
                    f"TmuxShell init: bash did not complete a no-op within {self._INIT_TIMEOUT}s"
                )
        except BaseException:
            self._dispose_unsafe()
            raise

    # --- tmux plumbing ------------------------------------------------------

    def _tmux_cmd(self, *args: str, timeout: float = 10.0) -> subprocess.CompletedProcess:
        return subprocess.run(
            [self._tmux, "-L", self._socket, *args],
            capture_output=True, text=True, timeout=timeout,
        )

    def _send(self, text: str, enter: bool = True):
        # -l = literal: tmux sends the bytes verbatim (no key-name parsing),
        # so arbitrary command text is never mis-read as a key like "Enter".
        self._tmux_cmd("send-keys", "-t", self._target, "-l", text)
        if enter:
            self._tmux_cmd("send-keys", "-t", self._target, "Enter")

    def _mk_reader(self, name: str, cb: Callable[[bytes], None]) -> NonBlockingReader:
        r = NonBlockingReader(self._rfds[name])
        r.RegisterCallback(cb)
        return r

    # --- stream tees --------------------------------------------------------

    def _decode(self, b: bytes) -> str:
        return RemoveTrailingNewline(b.decode("utf-8", errors="replace"))

    def _make_ctl_tee(self):
        def _cb(b: bytes):
            msg = self._decode(b)
            if not msg:
                return
            m = self._DONE_RE.search(msg)
            if m and m.group("token") == self._token:
                with self._cond:
                    self._results[m.group("nonce")] = int(m.group("rc"))
                    self._cond.notify_all()
        return _cb

    def _make_stdout_tee(self):
        """stdout reader: also scrapes the in-band SUB sentinel (sub-shell path)."""
        def _cb(b: bytes):
            msg = self._decode(b)
            if not msg:
                return
            m = self._SUB_RE.search(msg)
            if m and m.group("token") == self._token:
                nonce = m.group("nonce")
                with self._cond:
                    if nonce in self._pending:
                        self._results[nonce] = int(m.group("rc"))
                        self._cond.notify_all()
                        return  # strip sentinel from user delivery
            for f in list(self._out_callbacks):
                try: f(msg)
                except Exception as e:
                    Log.Error(f"TmuxShell user callback raised: [{e}]")
        return _cb

    def _make_plain_tee(self, _name: str, cb_lst: list[Callable[[str], None]]):
        def _cb(b: bytes):
            msg = self._decode(b)
            if not msg:
                return
            for f in list(cb_lst):
                try: f(msg)
                except Exception as e:
                    Log.Error(f"TmuxShell user callback raised: [{e}]")
        return _cb

    # --- lifecycle ----------------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.Dispose()

    def Dispose(self):
        if self._closed:
            return
        self._closed = True
        self._dispose_unsafe()

    def _dispose_unsafe(self):
        if self._session_up:
            try: self._tmux_cmd("kill-session", "-t", "msm", timeout=5)
            except Exception: pass
            try: self._tmux_cmd("kill-server", timeout=5)
            except Exception: pass
            self._session_up = False
        # tmux does not always unlink the private socket on kill — do it so
        # dead socket files don't accumulate in /tmp/tmux-<uid>/.
        sock_dir = os.environ.get("TMUX_TMPDIR") or "/tmp"
        sock_path = os.path.join(sock_dir, f"tmux-{os.getuid()}", self._socket)
        try: os.unlink(sock_path)
        except OSError: pass
        for r in self._readers:
            try: r.Dispose()
            except Exception: pass
        self._readers.clear()
        for fd in list(self._wfds.values()) + list(self._rfds.values()):
            try: os.close(fd)
            except OSError: pass
        self._wfds.clear(); self._rfds.clear()
        if self._tmpdir and os.path.isdir(self._tmpdir):
            for f in self._fifos.values():
                try: os.unlink(f)
                except OSError: pass
            try: os.rmdir(self._tmpdir)
            except OSError: pass
        self._tmpdir = None
        with self._cond:
            self._cond.notify_all()

    # --- callbacks ----------------------------------------------------------

    def RegisterOnOut(self, callback: Callable[[str], None]):
        self._out_callbacks.append(callback)

    def RegisterOnErr(self, callback: Callable[[str], None]):
        self._err_callbacks.append(callback)

    def RemoveOnOut(self, callback: Callable[[str], None]):
        if callback in self._out_callbacks: self._out_callbacks.remove(callback)

    def RemoveOnErr(self, callback: Callable[[str], None]):
        if callback in self._err_callbacks: self._err_callbacks.remove(callback)

    # --- exec ---------------------------------------------------------------

    def ExecAsync(self, cmd: str, inherit_stdin: bool = False) -> str | None:
        if self._closed:
            return None
        nonce = GenerateId()
        with self._cond:
            self._pending.add(nonce)
        body = RemoveLeadingIndent(cmd).rstrip()

        if self._depth > 0:
            # SUB-SHELL PATH: side channel can't cross the boundary, so emit an
            # in-band sentinel on stdout (this is what LiveShell does).
            rs = self._RS
            sentinel = (
                f"; printf '{rs}SUB {self._token} {nonce} %s{rs}\\n' \"$?\""
            )
            self._send(body + sentinel)
        else:
            # LOCAL PATH: durable out-of-band completion via the control FIFO.
            if inherit_stdin:
                wrapped = body
            else:
                wrapped = "{\n" + body + "\n} </dev/null"
            rs = self._RS
            script = (
                f"{wrapped}\n"
                f"__rc=$?\n"
                f"printf '{rs}DONE {self._token} {nonce} %s{rs}\\n' \"$__rc\" "
                f"> {shlex.quote(self._fifos['ctl'])}\n"
            )
            path = os.path.join(self._tmpdir, f"cmd_{nonce}.sh")
            with open(path, "w") as fh:
                fh.write(script)
            self._send("source " + shlex.quote(path))
        return nonce

    def AwaitDone(self, _hash: str, timeout: int | float | None = None) -> int | None:
        with self._cond:
            self._cond.wait_for(
                lambda: _hash in self._results or self._closed,
                timeout=timeout,
            )
            rc = self._results.pop(_hash, None)
            self._pending.discard(_hash)
        return rc

    def Exec(self, cmd: str, timeout: float | None = None, history: bool = False,
             quiet: bool = False, inherit_stdin: bool = False) -> ShellResult:
        _out, _err = [], []
        _saved_out = _saved_err = None
        try:
            if quiet:
                _saved_out = list(self._out_callbacks)
                _saved_err = list(self._err_callbacks)
                self._out_callbacks.clear()
                self._err_callbacks.clear()
            if history:
                self.RegisterOnOut(_out.append)
                self.RegisterOnErr(_err.append)

            _hash = self.ExecAsync(cmd, inherit_stdin=inherit_stdin)
            if _hash is None:
                return ShellResult(out=_out, err=_err, exit_code=None)
            exit_code = self.AwaitDone(_hash=_hash, timeout=timeout)
        finally:
            if history:
                self.RemoveOnOut(_out.append)
                self.RemoveOnErr(_err.append)
            if _saved_out is not None:
                self._out_callbacks[:] = _saved_out
                self._err_callbacks[:] = _saved_err
        return ShellResult(out=_out, err=_err, exit_code=exit_code)

    # --- shell-boundary crossing --------------------------------------------

    @contextmanager
    def SubShell(self, entry_cmd: str, *,
                 pop_settle_s: float = 0.3,
                 pop_timeout: float = 5.0):
        """Enter a sub-shell (ssh / nested bash), run in-band-tracked commands,
        then pop back to the local shell and resume the FIFO completion path."""
        # Enter via send-keys; the inner shell becomes foreground. inherit_stdin
        # is irrelevant here because send-keys writes to the pane pty, not a pipe.
        self._send(RemoveLeadingIndent(entry_cmd).rstrip())
        self._depth += 1
        try:
            yield self
        finally:
            self._send("exit")
            self._depth -= 1
            # let the inner shell drain + ssh tear down before resyncing locally
            sleep(pop_settle_s)
            if self._depth == 0:
                # a local no-op round-trips the control FIFO, proving we are
                # back on the local bash and the pipeline is healthy again.
                self.Exec("true", timeout=pop_timeout, quiet=True)
