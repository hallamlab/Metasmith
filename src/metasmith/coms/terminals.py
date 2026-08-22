from __future__ import annotations
import os
import re
import secrets
import signal
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

def _env_seconds(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if not raw: return default
    try:
        return float(raw)
    except ValueError:
        Log.Warn(f"ignoring [{name}={raw}]: not a number of seconds")
        return default

IDLE_TIMEOUT = _env_seconds("METASMITH_IDLE_TIMEOUT", 300)
PROBE_TIMEOUT = _env_seconds("METASMITH_PROBE_TIMEOUT", 60)
SSH_CONNECT_TIMEOUT = _env_seconds("METASMITH_SSH_CONNECT_TIMEOUT", 15)

@dataclass
class ShellResult:
    out: list[str]
    err: list[str]
    exit_code: int | None = None

class ShellDiedError(ConnectionError):
    def __init__(self, message: str, exit_code: int | None = None, tail: str | None = None):
        if tail:
            message = f"{message}; last output:\n{tail}"
        super().__init__(message)
        self.exit_code = exit_code
        self.tail = tail

class TerminalProcess:
    class Pipe:
        def __init__(self, io:IO[bytes], lock: Condition|None = None) -> None:
            self.IO = io
            if lock is None: lock = Condition()
            self.Lock = lock

        def __enter__(self):
            self.Lock.acquire()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.Lock.release()

    # TERM -> KILL window for the shell (and its group, when we own one).
    _STOP_GRACE_S = 2.0

    def __init__(
        self, extra_pass_fds: tuple[int, ...] = (), new_session: bool = True,
    ) -> None:
        # `new_session` makes this bash a session and process-group leader, so
        # Dispose can signal the whole group. Pass False to leave it in the
        # caller's group -- for a shell that is meant to die with the run that
        # started it, where signalling the group would signal the caller too.
        self._fds: list[int] = []
        self._owns_group = new_session
        self._console: subprocess.Popen | None = None
        self._err_reader: NonBlockingReader | None = None
        self._out_reader: NonBlockingReader | None = None
        try:
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
                start_new_session=new_session,
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
        try: self._stop_console()
        except Exception: pass
        for fd in self._fds:
            try: os.close(fd)
            except OSError: pass
        self._fds = []

    def _signal_console(self, sig: int):
        console = self._console
        if console is None: return
        try:
            if self._owns_group:
                # Signalled even once bash itself has exited: a process group
                # outlives its leader, and that survivor is exactly the orphan
                # a leader-only signal leaves behind.
                os.killpg(console.pid, sig)
            elif console.returncode is None:
                console.send_signal(sig)
        except (ProcessLookupError, PermissionError):
            pass

    def _stop_console(self):
        console = self._console
        if console is None: return
        self._signal_console(signal.SIGTERM)
        try: console.wait(timeout=self._STOP_GRACE_S)
        except subprocess.TimeoutExpired: pass
        self._signal_console(signal.SIGKILL)
        try: console.wait(timeout=self._STOP_GRACE_S)
        except subprocess.TimeoutExpired:
            Log.Error("TerminalProcess: shell did not exit")

    def Send(self, payload: bytes):
        if self._closed: raise ConnectionError("terminal disposed")
        if not self.IsAlive():
            raise ShellDiedError("shell process exited before this command could be written", exit_code=self.ExitCode())
        stdin = self._in
        with self._in:
            try:
                stdin.IO.write(payload)
                stdin.IO.flush()
            except (BrokenPipeError, OSError, ValueError) as e:
                raise ShellDiedError(
                    f"shell process exited while writing ({e})", exit_code=self.ExitCode(),
                ) from e

    def Decode(self, payload: bytes):
        return payload.decode(encoding=self.ENCODING)

    def IsAlive(self) -> bool:
        if self._console is None: return False
        return self._console.poll() is None

    def ExitCode(self) -> int | None:
        if self._console is None: return None
        return self._console.poll()

    def SecondsSinceRead(self) -> float:
        marks = [
            r.SecondsSinceRead()
            for r in (self._out_reader, self._err_reader) if r is not None
        ]
        if not marks: return 0.0
        return min(marks)

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
        try:
            self._stop_console()
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
    _INIT_NONCE = "__msm_init__"
    # FAR backstop for a WEDGED-but-alive bash, measured as absolute elapsed
    # from the start of _wait_for_init. This is NOT the operative limit:
    # liveness governs the wait (a slow-but-alive bash is never killed by the
    # clock — see _wait_for_init). It exists only so a bash that is alive yet
    # never responds (e.g. an rc file that blocks on `read` forever) can't
    # hang the caller indefinitely. Kept generous so it stays clear of any
    # realistic reader-thread starvation under load. Env-overridable for the
    # truly pathological host.
    _INIT_TIMEOUT = float(os.environ.get("METASMITH_LIVESHELL_INIT_TIMEOUT", "300.0"))
    _INIT_POLL_INTERVAL = 0.05

    _RS = "\x1e"
    _MARKER_RE = re.compile(
        r"\x1eMSM_(?P<kind>END|ERR)_(?P<token>[0-9a-f]+)_(?P<nonce>[A-Za-z0-9_]+)"
        r"(?: (?P<rc>-?\d+))?\x1e"
    )

    _pop_drop_first_marker = False

    def __init__(self, new_session: bool = True) -> None:
        self._new_session = new_session
        self._err_callbacks: list[Callable[[str], None]] = []
        self._out_callbacks: list[Callable[[str], None]] = []
        self._results: dict[str, int] = {}
        self._pending: set[str] = set()
        self._sync_received: dict[str, set[str]] = {}
        self._cond = Condition()
        self._shell: TerminalProcess | None = None
        self._closed = False
        self._token = secrets.token_hex(16)
        self._last_byte_time = time.monotonic()
        self._depth = 0

        try:
            self._shell = TerminalProcess(new_session=self._new_session)

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
            self._last_byte_time = time.monotonic()
            m = self._MARKER_RE.search(msg)
            if m and m.group("token") == self._token:
                nonce = m.group("nonce")
                with self._cond:
                    is_ours = nonce in self._pending or nonce == self._INIT_NONCE
                    if is_ours:
                        kind = m.group("kind")
                        if kind == "END":
                            rc = m.group("rc")
                            if rc is not None:
                                self._results[nonce] = int(rc)
                        else:
                            self._results.setdefault(nonce, 0)
                        self._sync_received.setdefault(nonce, set()).add(stream_name)
                        self._cond.notify_all()
                        return
            for f in list(cb_lst):
                try: f(msg)
                except Exception as e:
                    Log.Error(f"LiveShell user callback raised: [{e}]")
        return _cb

    def _wait_for_init(self):
        start = time.monotonic()
        with self._cond:
            while True:
                if self._is_fully_synced(self._INIT_NONCE):
                    break
                if self._closed:
                    raise RuntimeError(
                        "LiveShell init: shell closed before bash responded"
                    )
                if self._shell is None or not self._shell.IsAlive():
                    if self._is_fully_synced(self._INIT_NONCE):
                        break
                    rc = self._shell.ExitCode() if self._shell is not None else None
                    raise RuntimeError(
                        f"LiveShell init: bash exited (rc={rc}) before responding"
                    )
                if time.monotonic() - start >= self._INIT_TIMEOUT:
                    raise RuntimeError(
                        f"LiveShell init: bash alive but unresponsive for "
                        f"{self._INIT_TIMEOUT}s"
                    )
                self._cond.wait(timeout=self._INIT_POLL_INTERVAL)
        with self._cond:
            self._results.pop(self._INIT_NONCE, None)
            self._sync_received.pop(self._INIT_NONCE, None)
            self._pending.discard(self._INIT_NONCE)


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.Dispose()

    def Dispose(self):
        if self._closed: return
        self._closed = True
        self._dispose_unsafe()

    def _dispose_unsafe(self):
        if self._shell is not None:
            try: self._shell.Dispose()
            except Exception as e: Log.Error(f"LiveShell shell dispose [{e}]")
            self._shell = None
        self._err_callbacks.clear()
        self._out_callbacks.clear()
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


    def ExecAsync(self, cmd: str, inherit_stdin: bool = False):
        if self._shell is None: return None
        nonce = GenerateId()
        with self._cond:
            self._pending.add(nonce)
            self._sync_received[nonce] = set()
        body = RemoveLeadingIndent(cmd).rstrip()
        if inherit_stdin:
            self._shell.Write(body)
        else:
            self._shell.Write("{\n:\n" + body + "\n} </dev/null")
        self._shell.Write(self._marker_emission_bash(nonce))
        return nonce

    def _is_fully_synced(self, target: str) -> bool:
        if target not in self._results: return False
        seen = self._sync_received.get(target, set())
        return "out" in seen and "err" in seen

    def AwaitDone(
        self, _hash: str,
        timeout: int|float|None = None,
        idle_timeout: int|float|None = None,
        what: str|None = None,
    ) -> int|None:
        started = time.monotonic()
        deadline = None if timeout is None else started + timeout
        went_silent = False
        shell_died = False
        with self._cond:
            while not self._is_fully_synced(_hash) and not self._closed:
                if self._shell is None or not self._shell.IsAlive():
                    if not self._is_fully_synced(_hash):
                        self._cond.wait(timeout=self._INIT_POLL_INTERVAL)
                        shell_died = not self._is_fully_synced(_hash)
                    break
                remaining = self._INIT_POLL_INTERVAL
                if deadline is not None:
                    left = deadline - time.monotonic()
                    if left <= 0:
                        break
                    remaining = min(remaining, left)
                if idle_timeout is not None:
                    silent = min(
                        self._shell.SecondsSinceRead(),
                        time.monotonic() - started,
                    )
                    if silent >= idle_timeout:
                        went_silent = True
                        break
                    remaining = min(remaining, idle_timeout - silent)
                self._cond.wait(timeout=remaining)
            exit_code = self._results.pop(_hash, None)
            exit_status = self._shell.ExitCode() if self._shell is not None else None
            self._pending.discard(_hash)
            self._sync_received.pop(_hash, None)
        if went_silent:
            raise TimeoutError(
                f"[{what or 'command'}] produced no output for {idle_timeout:g}s"
            )
        if shell_died and not self._closed:
            raise ShellDiedError(
                f"shell exited (rc={exit_status}) while running [{what or 'command'}]",
                exit_code=exit_status,
            )
        return exit_code


    @contextmanager
    def SubShell(self, entry_cmd: str, *,
                 pop_quiescence_ms: int = 150,
                 pop_idle_samples: int = 3,
                 pop_timeout: float = 5.0,
                 pop_retries: int = 1):
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
        for attempt in range(retries + 1):
            nonce = GenerateId()
            with self._cond:
                self._pending.add(nonce)
                self._sync_received[nonce] = set()
            if attempt == 0 and self._pop_drop_first_marker:
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

    def Exec(
        self, cmd: str, timeout: float|None = None, history: bool=False,
        quiet: bool=False, inherit_stdin: bool = False,
        idle_timeout: float|None = None, what: str|None = None,
    ) -> ShellResult:
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
            exit_code = self.AwaitDone(
                _hash=_hash, timeout=timeout,
                idle_timeout=idle_timeout,
                what=what or " ".join(cmd.split())[:80],
            )
        finally:
            if history:
                self.RemoveOnOut(_log_out)
                self.RemoveOnErr(_log_err)
            if _saved_out is not None:
                self._out_callbacks[:] = _saved_out
                self._err_callbacks[:] = _saved_err
        return ShellResult(out=_out, err=_err, exit_code=exit_code)
