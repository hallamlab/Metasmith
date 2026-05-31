from __future__ import annotations
import os
import re
import secrets
import time
from contextlib import contextmanager
from typing import IO, Callable
from threading import Condition
import subprocess
from time import sleep
from dataclasses import dataclass, field
import pty

from ..logging import Log
from .ipc import NonBlockingReader, GenerateId, ResetGenerator, RemoveTrailingNewline, RemoveLeadingIndent, CurrentTimeMillis

@dataclass
class ShellResult:
    out: list[str]
    err: list[str]
    exit_code: int | None = None

class TerminalProcess:
    """
    Owns: 1 bash subprocess, 4 pty FDs (out/err master+slave), and two
    NonBlockingReader threads. All allocation is guarded so a partial init
    leaks nothing.
    """
    class Pipe:
        def __init__(self, io:IO[bytes], lock: Condition|None = None) -> None:
            self.IO = io
            if lock is None: lock = Condition()
            self.Lock = lock

        def __enter__(self):
            self.Lock.acquire()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.Lock.release()

    def __init__(self, extra_pass_fds: tuple[int, ...] = ()) -> None:
        # extra_pass_fds kept as a back-compat parameter; no current caller
        # uses it. Defaults to () so behaviour is identical for everyone.
        self._fds: list[int] = []
        self._console: subprocess.Popen | None = None
        self._err_reader: NonBlockingReader | None = None
        self._out_reader: NonBlockingReader | None = None
        try:
            # https://stackoverflow.com/questions/41542960/run-interactive-bash-with-popen-and-a-dedicated-tty-python
            out_master, out_slave = pty.openpty()
            self._fds += [out_master, out_slave]
            err_master, err_slave = pty.openpty()
            self._fds += [err_master, err_slave]

            self._console = subprocess.Popen(
                ["bash"],
                stdin=subprocess.PIPE,
                stdout=out_slave,
                stderr=err_slave,
                pass_fds=extra_pass_fds,
                close_fds=True,
                start_new_session=True, # nextflow needs this
            )

            self.ENCODING = "utf-8"
            assert self._console.stdin is not None
            self._in = TerminalProcess.Pipe(self._console.stdin)
            self._onCloseLock = Condition()
            self._closed = False
            self.pid = self._console.pid
            self._err_reader = NonBlockingReader(err_master)
            self._out_reader = NonBlockingReader(out_master)
        except BaseException:
            self._cleanup_partial()
            raise

    def _cleanup_partial(self):
        for r in (self._err_reader, self._out_reader):
            if r is not None:
                try: r.Dispose()
                except Exception: pass
        if self._console is not None:
            try:
                self._console.terminate()
                try: self._console.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self._console.kill()
                    try: self._console.wait(timeout=2)
                    except subprocess.TimeoutExpired: pass
            except Exception: pass
        for fd in self._fds:
            try: os.close(fd)
            except OSError: pass
        self._fds = []

    def Send(self, payload: bytes):
        if self._closed: raise ConnectionError("terminal disposed")
        stdin = self._in
        with self._in:
            stdin.IO.write(payload)
            stdin.IO.flush()

    def Decode(self, payload: bytes):
        return payload.decode(encoding=self.ENCODING)

    def Write(self, msg: str):
        self.Send(bytes('%s\n' % (msg), encoding=self.ENCODING))

    def RegisterOnOut(self, callback: Callable[[bytes], None]):
        if self._closed: raise ConnectionError("terminal disposed")
        self._out_reader.RegisterCallback(callback)

    def RegisterOnErr(self, callback: Callable[[bytes], None]):
        if self._closed: raise ConnectionError("terminal disposed")
        self._err_reader.RegisterCallback(callback)

    def RemoveOnOut(self, callback: Callable[[bytes], None]):
        self._out_reader.RemoveCallback(callback)

    def RemoveOnErr(self, callback: Callable[[bytes], None]):
        self._err_reader.RemoveCallback(callback)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.Dispose()
        return

    def Dispose(self):
        if self._closed: return
        self._closed = True
        if self._err_reader is not None:
            try: self._err_reader.Dispose()
            except Exception as e: Log.Error(f"TerminalProcess.Dispose() err_reader [{e}]")
        if self._out_reader is not None:
            try: self._out_reader.Dispose()
            except Exception as e: Log.Error(f"TerminalProcess.Dispose() out_reader [{e}]")
        if self._console is not None:
            try:
                self._console.terminate()
                try: self._console.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self._console.kill()
                    try: self._console.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        Log.Error(f"TerminalProcess.Dispose() subprocess did not exit")
            except Exception as e:
                Log.Error(f"TerminalProcess.Dispose() console [{e}]")
        for i, fd in enumerate(self._fds):
            try:
                os.close(fd)
            except OSError as e:
                if "Bad file descriptor" not in str(e):
                    Log.Error(f"TerminalProcess.Dispose() fd:{i} [{e}]")
        self._fds = []

class LiveShell:
    """
    Bash session wrapper with completion signalled by an inline marker pair.

    Per Exec, two top-level statements are written to bash's stdin after
    the user command:

        <user_cmd>
        __rc=$?; printf '\x1eMSM_END_<TOKEN>_<NONCE> %s\x1e\n' "$__rc"; \
                 printf '\x1eMSM_ERR_<TOKEN>_<NONCE>\x1e\n' >&2

    The marker is just bytes — whichever shell is parsing bash's stdin when
    the line is reached emits the marker, and ssh / nested bash / docker
    exec / bash -c forward the bytes back unchanged. This makes Exec work
    uniformly across local bash AND any sub-shell entered via plain
    Exec("ssh host") / Exec("bash -c '...'") / etc., with no setup-in-
    sub-shell required.

    Marker shape rationale:
      - \x1e (ASCII RS) brackets ⇒ invisible, never in natural output.
      - Per-LiveShell 128-bit token ⇒ cheap session filter.
      - Per-Exec random nonce ⇒ identifies which pending Exec.
      - The 'strip from delivery' rule only fires when the parsed nonce is
        in self._pending or equals self._INIT_NONCE. A user echoing the
        marker shape with an unknown nonce passes through verbatim, so user
        output is byte-faithful unless they happen to also know our token
        + a currently-pending nonce (statistically zero).

    stdin isolation (default):
      User cmds run inside `{ ...; } </dev/null` so children cannot greedily
      consume the marker emission line off bash's stdin pipe. Without this,
      `Exec("ssh host cmd")` would wedge: ssh inherits the pipe, reads the
      marker line meant for local bash, and forwards it to the remote where
      it's discarded. The brace group preserves env mutations in the parent
      shell (unlike `(...)` subshells) and propagates $? unchanged.

      For cmds that legitimately need bash's real stdin — sub-shell entries
      (ssh host, bash -c interactive, docker exec -i ...) whose marker is
      meant to travel through to the inner shell — pass inherit_stdin=True
      to Exec / ExecAsync. SubShell.__enter__ does this automatically.

    Known residual limitations (in-band approach inherent):
      - Exec("exec something") replaces bash; no in-band approach survives.
      - Exec("exec 2>/tmp/log") sends the stderr marker to the file forever.
      - User cmd that itself reads stdin (e.g. cat, read x) gets immediate
        EOF from the default /dev/null redirect, which is the right batch
        behavior. Pass inherit_stdin=True only if the cmd is a sub-shell
        entry.
    """

    _INIT_NONCE = "__msm_init__"
    _INIT_TIMEOUT = 5.0

    _RS = "\x1e"
    # Matches one marker line on either stream. Token is captured for filter;
    # nonce identifies the pending Exec; rc is present only on END markers.
    _MARKER_RE = re.compile(
        r"\x1eMSM_(?P<kind>END|ERR)_(?P<token>[0-9a-f]+)_(?P<nonce>[A-Za-z0-9_]+)"
        r"(?: (?P<rc>-?\d+))?\x1e"
    )

    # Test seam: when True, the first marker write in _pop is replaced with a
    # nonce nothing matches, forcing the retry path. Production default False.
    _pop_drop_first_marker = False

    def __init__(self) -> None:
        self._err_callbacks: list[Callable[[str], None]] = []
        self._out_callbacks: list[Callable[[str], None]] = []
        self._results: dict[str, int] = {}
        self._pending: set[str] = set()
        self._sync_received: dict[str, set[str]] = {}  # nonce -> {"out","err"}
        self._cond = Condition()
        self._shell: TerminalProcess | None = None
        self._closed = False
        self._token = secrets.token_hex(16)  # 128-bit session id
        self._last_byte_time = time.monotonic()
        self._depth = 0  # bookkeeping for SubShell nesting; diagnostics only

        try:
            self._shell = TerminalProcess()

            # Tee callbacks on each stream:
            #  - parse marker lines and route to _results / _sync_received
            #  - strip ONLY when the parsed nonce is currently pending
            #    (preserves byte-faithful user output for user-echoed marker
            #     shapes with unknown nonces — see G2 in tests/test_live_shell.py)
            self._shell.RegisterOnErr(self._make_tee("err", self._err_callbacks))
            self._shell.RegisterOnOut(self._make_tee("out", self._out_callbacks))

            # Startup probe: emit a marker pair for _INIT_NONCE with no
            # preceding user command. Bash's $? on a fresh shell is 0, so
            # the END marker carries exit=0; what we actually wait on is
            # the marker pair arriving on both streams.
            with self._cond:
                self._pending.add(self._INIT_NONCE)
                self._sync_received[self._INIT_NONCE] = set()
            self._shell.Write(self._marker_emission_bash(self._INIT_NONCE))
            self._wait_for_init()
        except BaseException:
            self._dispose_unsafe()
            raise

    # --- marker plumbing ----------------------------------------------------

    def _marker_emission_bash(self, nonce: str) -> str:
        # Two top-level statements as a single line (joined by ';'); bash
        # treats them as a list, NOT a compound — so `set -e` cannot
        # short-circuit the second printf even if the first somehow fails.
        # printf does not exit non-zero on stdout writes in normal use.
        rs = self._RS
        return (
            f"__rc=$?; "
            f"printf '{rs}MSM_END_{self._token}_{nonce} %s{rs}\\n' \"$__rc\"; "
            f"printf '{rs}MSM_ERR_{self._token}_{nonce}{rs}\\n' >&2"
        )

    def _make_tee(self, stream_name: str, cb_lst: list[Callable[[str], None]]):
        def _cb(x):
            if self._shell is None: return
            msg = RemoveTrailingNewline(self._shell.Decode(x))
            if len(msg) == 0: return
            # Channel-activity timestamp: updated on every non-empty chunk,
            # including marker lines. Read by _pop to detect quiescence.
            self._last_byte_time = time.monotonic()
            m = self._MARKER_RE.search(msg)
            if m and m.group("token") == self._token:
                nonce = m.group("nonce")
                with self._cond:
                    is_ours = nonce in self._pending or nonce == self._INIT_NONCE
                    if is_ours:
                        kind = m.group("kind")
                        # END markers (stdout) carry the exit code; ERR markers
                        # only signal stderr drain. Either populates _results
                        # if we can parse rc — both streams agree on the value.
                        if kind == "END":
                            rc = m.group("rc")
                            if rc is not None:
                                self._results[nonce] = int(rc)
                        else:
                            # ERR marker — populate _results too if we haven't
                            # already, so the predicate fires even when stdout
                            # tee is delayed (e.g. heavy stdout buffering).
                            # Use a sentinel only when truly absent.
                            self._results.setdefault(nonce, 0)
                        self._sync_received.setdefault(nonce, set()).add(stream_name)
                        self._cond.notify_all()
                        return  # strip marker line from user delivery
                # Fall through if not ours: user output that happens to match
                # the marker shape with an unknown nonce stays in the stream.
            for f in list(cb_lst):
                try: f(msg)
                except Exception as e:
                    Log.Error(f"LiveShell user callback raised: [{e}]")
        return _cb

    def _wait_for_init(self):
        with self._cond:
            ok = self._cond.wait_for(
                lambda: self._is_fully_synced(self._INIT_NONCE) or self._closed,
                timeout=self._INIT_TIMEOUT,
            )
        if not ok:
            raise RuntimeError(
                f"LiveShell init: bash did not respond on both streams within "
                f"{self._INIT_TIMEOUT}s"
            )
        # Reap init bookkeeping so it doesn't linger.
        with self._cond:
            self._results.pop(self._INIT_NONCE, None)
            self._sync_received.pop(self._INIT_NONCE, None)
            self._pending.discard(self._INIT_NONCE)

    # --- lifecycle ----------------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.Dispose()

    def Dispose(self):
        if self._closed: return
        self._closed = True
        self._dispose_unsafe()

    def _dispose_unsafe(self):
        # Called from Dispose or from a partial __init__. Idempotent.
        if self._shell is not None:
            try: self._shell.Dispose()
            except Exception as e: Log.Error(f"LiveShell shell dispose [{e}]")
            self._shell = None
        self._err_callbacks.clear()
        self._out_callbacks.clear()
        with self._cond:
            self._cond.notify_all()

    # --- user callbacks -----------------------------------------------------

    def RegisterOnOut(self, callback: Callable[[str], None]):
        self._out_callbacks.append(callback)

    def RegisterOnErr(self, callback: Callable[[str], None]):
        self._err_callbacks.append(callback)

    def RemoveOnOut(self, callback: Callable[[str], None]):
        if callback in self._out_callbacks: self._out_callbacks.remove(callback)

    def RemoveOnErr(self, callback: Callable[[str], None]):
        if callback in self._err_callbacks: self._err_callbacks.remove(callback)

    # --- exec ---------------------------------------------------------------

    def ExecAsync(self, cmd: str, inherit_stdin: bool = False):
        """Send a command + inline marker emission. Returns the per-Exec nonce.

        inherit_stdin: when False (default), the user cmd runs inside a brace
        group with stdin redirected to /dev/null, so children like one-shot
        ssh, cat, read, or sudo (no -n) cannot greedily consume the marker
        emission line off bash's stdin pipe. Set True only for cmds that
        deliberately enter a long-lived sub-shell (e.g. SubShell entry),
        where the marker is supposed to travel through to the inner shell.
        """
        if self._shell is None: return None
        nonce = GenerateId()
        with self._cond:
            self._pending.add(nonce)
            self._sync_received[nonce] = set()
        body = RemoveLeadingIndent(cmd).rstrip()
        if inherit_stdin:
            self._shell.Write(body)
        else:
            # Brace group preserves env mutations in the parent shell (unlike
            # subshell parens). Explicit '\n' before '}' guarantees the
            # closing brace is its own token regardless of how `cmd` ended.
            # `__rc=$?` on the next written line still captures this group's
            # exit code, which equals the user cmd's exit code.
            self._shell.Write("{\n" + body + "\n} </dev/null")
        self._shell.Write(self._marker_emission_bash(nonce))
        return nonce

    def _is_fully_synced(self, target: str) -> bool:
        """Both stdout and stderr markers have arrived for this nonce."""
        if target not in self._results: return False
        seen = self._sync_received.get(target, set())
        return "out" in seen and "err" in seen

    def AwaitDone(self, _hash: str, timeout: int|float|None = None) -> int|None:
        """
        Block until the command for `_hash` is fully synchronized — both
        stdout and stderr marker lines received. Returns the user command's
        exit code, or None on timeout / shell closed.

        For a batched ExecAsync fan-out on the same shell, wait on the LAST
        enqueued nonce: bash runs commands sequentially, and both stream
        readers process bytes in order, so the last marker arriving implies
        all prior commands also fully synced.
        """
        with self._cond:
            if not self._is_fully_synced(_hash):
                self._cond.wait_for(
                    lambda: self._is_fully_synced(_hash) or self._closed,
                    timeout=timeout,
                )
            exit_code = self._results.pop(_hash, None)
            self._pending.discard(_hash)
            self._sync_received.pop(_hash, None)
        return exit_code

    # --- shell-boundary crossing --------------------------------------------

    @contextmanager
    def SubShell(self, entry_cmd: str, *,
                 pop_quiescence_ms: int = 150,
                 pop_idle_samples: int = 3,
                 pop_timeout: float = 5.0,
                 pop_retries: int = 1):
        """
        Cross into and back out of a sub-shell (ssh, nested bash, docker exec, ...).

        Enter: writes entry_cmd via plain Exec — the marker arrives from the
        sub-shell over its forwarded stdout/stderr, so we know we're in.

        Exit: writes 'exit', waits for output to quiesce (multi-sample idle
        window), then writes a fresh marker-emission line that lands on the
        parent shell. If the first marker is lost (sub-shell still draining
        when written), retries up to `pop_retries` times.

        Use this whenever the entry_cmd transitions to a different shell layer
        that you intend to leave again. Plain Exec("ssh host 'cmd'") one-shot
        commands do NOT need SubShell — they return on their own.
        """
        # inherit_stdin=True: the entry_cmd (ssh / nested bash / ...) is
        # supposed to consume the marker emission line off our stdin pipe
        # and forward it to the inner shell, which executes the printf and
        # the marker travels back to us via stdout/stderr. The default
        # </dev/null wrap would break that mechanism.
        self.Exec(entry_cmd, timeout=pop_timeout, inherit_stdin=True)
        self._depth += 1
        try:
            yield self
        finally:
            try:
                self._pop(
                    quiescence_ms=pop_quiescence_ms,
                    idle_samples=pop_idle_samples,
                    timeout=pop_timeout,
                    retries=pop_retries,
                )
            finally:
                self._depth -= 1

    def _pop(self, *, quiescence_ms: int, idle_samples: int,
             timeout: float, retries: int) -> int | None:
        """
        Write `exit` and synchronize with the parent shell.

        Phase 1: wait for true output quiescence — idle_samples consecutive
        sample windows where (now - _last_byte_time) >= quiescence_ms. A
        floor ensures at least one full quiescence_ms passes even if no
        output ever arrives (silent sub-shells).

        Phase 2: write a fresh marker-emission line and AwaitDone. If the
        marker doesn't return within a short window, retry. The retry is
        safe — a lost marker is a no-op on the dead sub-shell; the new
        marker lands on whichever shell now holds stdin.
        """
        if self._shell is None: return None
        self._shell.Write("exit")
        sample_interval = max(quiescence_ms / 1000.0 / idle_samples, 0.020)
        deadline = time.monotonic() + timeout
        floor_deadline = time.monotonic() + (quiescence_ms / 1000.0)
        consecutive_idle = 0
        while time.monotonic() < deadline:
            sleep(sample_interval)
            if time.monotonic() < floor_deadline:
                continue
            idle_ms = (time.monotonic() - self._last_byte_time) * 1000.0
            if idle_ms >= quiescence_ms:
                consecutive_idle += 1
                if consecutive_idle >= idle_samples:
                    break
            else:
                consecutive_idle = 0
        # Phase 2: send fresh marker; one retry covers a lost first attempt.
        for attempt in range(retries + 1):
            nonce = GenerateId()
            with self._cond:
                self._pending.add(nonce)
                self._sync_received[nonce] = set()
            if attempt == 0 and self._pop_drop_first_marker:
                # Test seam: emit a marker that nothing matches so the retry
                # path is exercised. Use a fresh token so it can't be parsed
                # as ours even by accident.
                bogus = self._marker_emission_bash(nonce).replace(
                    self._token, "0" * len(self._token)
                )
                self._shell.Write(bogus)
            else:
                self._shell.Write(self._marker_emission_bash(nonce))
            per_attempt = 0.5 if attempt < retries else max(1.0, timeout - 1.0)
            rc = self.AwaitDone(nonce, timeout=per_attempt)
            if rc is not None:
                return rc
        return None

    def Exec(self, cmd: str, timeout: float|None = None, history: bool=False, quiet: bool=False, inherit_stdin: bool = False) -> ShellResult:
        _out, _err = [], []
        _log_out = _out.append
        _log_err = _err.append
        _saved_out = _saved_err = None
        try:
            if quiet:
                _saved_out = list(self._out_callbacks)
                _saved_err = list(self._err_callbacks)
                self._out_callbacks.clear()
                self._err_callbacks.clear()
            if history:
                self.RegisterOnOut(_log_out)
                self.RegisterOnErr(_log_err)

            _hash = self.ExecAsync(cmd, inherit_stdin=inherit_stdin)
            if _hash is None:
                return ShellResult(out=_out, err=_err, exit_code=None)
            exit_code = self.AwaitDone(_hash=_hash, timeout=timeout)
        finally:
            if history:
                self.RemoveOnOut(_log_out)
                self.RemoveOnErr(_log_err)
            if _saved_out is not None:
                self._out_callbacks[:] = _saved_out
                self._err_callbacks[:] = _saved_err
        return ShellResult(out=_out, err=_err, exit_code=exit_code)
