from __future__ import annotations
import os
import json
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
    Owns: 1 bash subprocess, 4 pty FDs (out/err master+slave), an optional
    inheritable control fd, and up to 3 NonBlockingReader threads. All
    allocation is guarded so a partial init leaks nothing.
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
        # All resources are tracked in cleanup lists so a failure partway
        # through init releases everything atomically (G5).
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
    Bash session wrapper with out-of-band completion frames.

    Each Exec'd command is followed by a `__msm_done <id>` call that emits
    {"id":<id>,"exit":<exit_code>} on a dedicated control fd (fd 5 in the
    child, distinct from stdin/stdout/stderr). A reader thread parses
    frames and notifies a Condition; AwaitDone sleeps on the Condition
    rather than polling. User stdout/stderr is byte-for-byte the raw bash
    session — no in-band marker stripping, no sentinel collisions.
    """

    _INIT_MARK = "__msm_init__"
    _INIT_TIMEOUT = 5.0
    # Per-Exec sync prefix echoed on both stdout and stderr so the wrapper
    # knows those streams have drained past the user's output before
    # returning. Followed by a per-Exec random hash — collision with user
    # content requires guessing 12 random chars, statistically impossible.
    # The marker lines are stripped from both ShellResult and user callbacks.
    _SYNC_PREFIX = "__msm_sync__."

    def __init__(self) -> None:
        self._err_callbacks: list[Callable[[str], None]] = []
        self._out_callbacks: list[Callable[[str], None]] = []
        self._results: dict[str, int] = {}
        self._pending: set[str] = set()
        self._sync_received: dict[str, set[str]] = {}  # hash -> {"out","err"}
        self._cond = Condition()
        self._shell: TerminalProcess | None = None
        self._ctl_r: int | None = None
        self._ctl_reader: NonBlockingReader | None = None
        self._ctl_fd_in_child: int | None = None
        self._closed = False

        ctl_w: int | None = None
        try:
            # Open the control pipe; pass the write end to the child via
            # pass_fds, which preserves the fd number across fork+exec.
            self._ctl_r, ctl_w = os.pipe()
            os.set_inheritable(ctl_w, True)
            self._ctl_fd_in_child = ctl_w  # same number on the child side

            self._shell = TerminalProcess(extra_pass_fds=(ctl_w,))

            # Child inherited the ctl fd; parent no longer needs the write end.
            os.close(ctl_w)
            ctl_w = None

            # User stdout/stderr: relay to callbacks, but recognize the
            # per-Exec sync marker and use it to confirm stream drain. The
            # marker line is consumed (not delivered) so user output stays
            # byte-for-byte the raw bash session.
            def _tee(stream_name: str, cb_lst: list[Callable[[str], None]]):
                def _cb(x):
                    if self._shell is None: return
                    msg = RemoveTrailingNewline(self._shell.Decode(x))
                    if len(msg) == 0: return
                    if msg.startswith(self._SYNC_PREFIX):
                        hash_id = msg[len(self._SYNC_PREFIX):]
                        with self._cond:
                            self._sync_received.setdefault(hash_id, set()).add(stream_name)
                            self._cond.notify_all()
                        return  # strip sync marker from delivery
                    for f in list(cb_lst):
                        try: f(msg)
                        except Exception as e:
                            Log.Error(f"LiveShell user callback raised: [{e}]")
                return _cb
            self._shell.RegisterOnErr(_tee("err", self._err_callbacks))
            self._shell.RegisterOnOut(_tee("out", self._out_callbacks))

            # Reader for ctl frames.
            self._ctl_reader = NonBlockingReader(self._ctl_r)
            self._ctl_reader.RegisterCallback(self._on_ctl_line)

            # Install the bash-side trampoline, then sync via an init frame.
            self._install_trampoline()
            self._wait_for_init()
        except BaseException:
            # Atomic cleanup of anything allocated so far.
            if ctl_w is not None:
                try: os.close(ctl_w)
                except OSError: pass
            self._dispose_unsafe()
            raise

    def _install_trampoline(self):
        """
        Defines `__msm_done <id>` in the bash session. It emits a single
        JSON line {"id":"<id>","exit":<$?>} to fd _CTL_FD_IN_CHILD.
        """
        fd = self._ctl_fd_in_child
        # Use single-quoted printf to avoid escape surprises; \n is interpreted
        # by printf itself.
        trampoline = (
            f'__msm_done() {{ '
            f"printf '{{\"id\":\"%s\",\"exit\":%d}}\\n' \"$1\" \"$?\" >&{fd}; "
            f'}}'
        )
        self._shell.Write(trampoline)
        # Send an init ping so we know the function is installed and the ctl
        # channel is plumbed before any user Exec runs.
        self._shell.Write(f"__msm_done {self._INIT_MARK}")

    def _wait_for_init(self):
        with self._cond:
            ok = self._cond.wait_for(
                lambda: self._INIT_MARK in self._results,
                timeout=self._INIT_TIMEOUT,
            )
        if not ok:
            raise RuntimeError(
                f"LiveShell trampoline init failed: no frame on ctl fd within "
                f"{self._INIT_TIMEOUT}s"
            )
        # Drop the init marker so it doesn't show up later
        self._results.pop(self._INIT_MARK, None)

    def _on_ctl_line(self, raw: bytes):
        """Reader callback for ctl-fd. Parses one JSON frame per line."""
        try:
            line = raw.decode("utf-8", errors="replace").strip()
            if not line: return
            frame = json.loads(line)
            hash_id = frame.get("id")
            exit_code = frame.get("exit")
            if hash_id is None or exit_code is None: return
            with self._cond:
                self._results[hash_id] = int(exit_code)
                self._cond.notify_all()
        except Exception as e:
            Log.Error(f"LiveShell._on_ctl_line parse error on [{raw!r}]: [{e}]")

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
        if self._ctl_reader is not None:
            try: self._ctl_reader.Dispose()
            except Exception as e: Log.Error(f"LiveShell ctl_reader dispose [{e}]")
            self._ctl_reader = None
        if self._shell is not None:
            try: self._shell.Dispose()
            except Exception as e: Log.Error(f"LiveShell shell dispose [{e}]")
            self._shell = None
        if self._ctl_r is not None:
            try: os.close(self._ctl_r)
            except OSError: pass
            self._ctl_r = None
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

    def ExecAsync(self, cmd: str):
        """Send a command + sync markers + completion trampoline. Returns the frame id."""
        if self._shell is None: return None
        _hash = GenerateId()
        with self._cond:
            self._pending.add(_hash)
            self._sync_received[_hash] = set()
        self._shell.Write(RemoveLeadingIndent(cmd))
        # Capture user's $? immediately, emit drain markers, restore $? via
        # subshell-exit, then call __msm_done which reads $?. This is the
        # only way to interleave book-keeping without clobbering the user
        # command's exit code.
        self._shell.Write(f"__msm_rc=$?")
        self._shell.Write(f'echo "{self._SYNC_PREFIX}{_hash}"')
        self._shell.Write(f'echo "{self._SYNC_PREFIX}{_hash}" 1>&2')
        self._shell.Write(f"(exit $__msm_rc); __msm_done {_hash}")
        return _hash

    def _is_fully_synced(self, target: str) -> bool:
        """True when both stdout/stderr drain markers and the ctl frame have arrived."""
        if target not in self._results: return False
        seen = self._sync_received.get(target, set())
        return "out" in seen and "err" in seen

    def AwaitDone(self, _hash: str, timeout: int|float|None = None) -> int|None:
        """
        Block until the command for _hash is fully synchronized — both
        stdout/stderr drain markers received AND completion frame received.
        Returns the exit code, or None if AwaitDone timed out / shell closed.

        For a batch of ExecAsync calls on the same shell, wait on the LAST
        enqueued hash: bash runs commands sequentially, so the last frame
        arriving implies all prior ones already completed.
        """
        with self._cond:
            if not self._is_fully_synced(_hash):
                self._cond.wait_for(
                    lambda: self._is_fully_synced(_hash) or self._closed,
                    timeout=timeout,
                )
            # Reap state so long-lived shells don't accumulate entries.
            exit_code = self._results.pop(_hash, None)
            self._pending.discard(_hash)
            self._sync_received.pop(_hash, None)
        return exit_code

    def Exec(self, cmd: str, timeout: float|None = None, history: bool=False, quiet: bool=False) -> ShellResult:
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

            _hash = self.ExecAsync(cmd)
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
