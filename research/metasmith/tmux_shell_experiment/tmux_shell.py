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
        self._depth = 0

        self._tmpdir: str | None = None
        self._fifos: dict[str, str] = {}
        self._rfds: dict[str, int] = {}
        self._wfds: dict[str, int] = {}
        self._readers: list[NonBlockingReader] = []
        self._session_up = False

        try:
            self._tmpdir = tempfile.mkdtemp(prefix="tmuxshell_")
            for name in ("out", "err", "ctl"):
                p = os.path.join(self._tmpdir, f"{name}.fifo")
                os.mkfifo(p)
                self._fifos[name] = p
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

            rc = self.Exec("true", timeout=self._INIT_TIMEOUT, quiet=True).exit_code
            if rc is None:
                raise RuntimeError(
                    f"TmuxShell init: bash did not complete a no-op within {self._INIT_TIMEOUT}s"
                )
        except BaseException:
            self._dispose_unsafe()
            raise


    def _tmux_cmd(self, *args: str, timeout: float = 10.0) -> subprocess.CompletedProcess:
        return subprocess.run(
            [self._tmux, "-L", self._socket, *args],
            capture_output=True, text=True, timeout=timeout,
        )

    def _send(self, text: str, enter: bool = True):
        self._tmux_cmd("send-keys", "-t", self._target, "-l", text)
        if enter:
            self._tmux_cmd("send-keys", "-t", self._target, "Enter")

    def _mk_reader(self, name: str, cb: Callable[[bytes], None]) -> NonBlockingReader:
        r = NonBlockingReader(self._rfds[name])
        r.RegisterCallback(cb)
        return r


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
                        return
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


    def RegisterOnOut(self, callback: Callable[[str], None]):
        self._out_callbacks.append(callback)

    def RegisterOnErr(self, callback: Callable[[str], None]):
        self._err_callbacks.append(callback)

    def RemoveOnOut(self, callback: Callable[[str], None]):
        if callback in self._out_callbacks: self._out_callbacks.remove(callback)

    def RemoveOnErr(self, callback: Callable[[str], None]):
        if callback in self._err_callbacks: self._err_callbacks.remove(callback)


    def ExecAsync(self, cmd: str, inherit_stdin: bool = False) -> str | None:
        if self._closed:
            return None
        nonce = GenerateId()
        with self._cond:
            self._pending.add(nonce)
        body = RemoveLeadingIndent(cmd).rstrip()

        if self._depth > 0:
            rs = self._RS
            sentinel = (
                f"; printf '{rs}SUB {self._token} {nonce} %s{rs}\\n' \"$?\""
            )
            self._send(body + sentinel)
        else:
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


    @contextmanager
    def SubShell(self, entry_cmd: str, *,
                 pop_settle_s: float = 0.3,
                 pop_timeout: float = 5.0):
        self._send(RemoveLeadingIndent(entry_cmd).rstrip())
        self._depth += 1
        try:
            yield self
        finally:
            self._send("exit")
            self._depth -= 1
            sleep(pop_settle_s)
            if self._depth == 0:
                self.Exec("true", timeout=pop_timeout, quiet=True)
