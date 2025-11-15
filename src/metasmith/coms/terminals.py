from __future__ import annotations
import os
from pathlib import Path
from typing import IO, Callable, Any
from threading import Condition, Thread
import subprocess
from time import sleep
# from gevent import sleep
# from gevent.lock import Semaphore as Condition
# from gevent import subprocess
from dataclasses import dataclass, field
# import select
import pty
# import random

from ..logging import Log
from .ipc import NonBlockingReader, GenerateId, ResetGenerator, RemoveTrailingNewline, RemoveLeadingIndent, CurrentTimeMillis

@dataclass
class ShellResult:
    out: list[str]
    err: list[str]
    
class TerminalProcess:
    class Pipe:
        def __init__(self, io:IO[bytes], lock: Condition = None) -> None:
            self.IO = io
            if lock is None: lock = Condition()
            self.Lock = lock

        def __enter__(self):
            self.Lock.acquire()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.Lock.release()

    def __init__(self) -> None:
        # https://stackoverflow.com/questions/41542960/run-interactive-bash-with-popen-and-a-dedicated-tty-python
        out_master, out_slave = pty.openpty()
        err_master, err_slave = pty.openpty()
        self._fds = [out_master, err_master, out_slave, err_slave]

        console = subprocess.Popen(
            ["bash"],
            stdin=subprocess.PIPE,
            stdout=out_slave,
            stderr=err_slave,
            close_fds=True,
            start_new_session=True, # nextflow needs this
        )

        self.ENCODING = "utf-8"
        self._console = console
        self._in = TerminalProcess.Pipe(console.stdin)
        self._onCloseLock = Condition()
        self._closed = False
        self.pid = console.pid
        self._err_reader = NonBlockingReader(err_master)
        self._out_reader = NonBlockingReader(out_master)

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
        self._err_reader.Dispose()
        self._out_reader.Dispose()
        self._console.terminate()
        for i, fd in enumerate(self._fds):
            try:
                os.close(fd)
            except OSError as e:
                if "Bad file descriptor" not in e.args:
                    Log.Error(f"TerminalProcess.Dispose() fd:{i} [{e}]")
        self._closed = True

class LiveShell:
    def __init__(self, sleep: Callable[[float], None]=sleep) -> None:
        self._MARK = f"done_{GenerateId()}"
        self._done_stack = set()
        self._err_callbacks = []
        self._out_callbacks = []
        self._sleep = sleep

        self._shell = TerminalProcess()
        def _tee(cb_lst: list[Callable[[str], None]], check=False):
            def _cb(x):
                msg = RemoveTrailingNewline(self._shell.Decode(x))
                if len(msg) == 0: return
                if msg.startswith(self._MARK):
                    _, k = msg.split(".")
                    if k in self._done_stack: self._done_stack.remove(k)
                    return
                for f in cb_lst: f(msg)
            return _cb
        self._shell.RegisterOnErr(_tee(self._err_callbacks))
        self._shell.RegisterOnOut(_tee(self._out_callbacks, check=True))
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.Dispose()

    def Dispose(self):
        if self._shell is None: return
        self._shell.Dispose()
        self._shell = None
        self._err_callbacks.clear()
        self._out_callbacks.clear()

    def RegisterOnOut(self, callback: Callable[[str], None]):
        self._out_callbacks.append(callback)
    
    def RegisterOnErr(self, callback: Callable[[str], None]):
        self._err_callbacks.append(callback)

    def RemoveOnOut(self, callback: Callable[[str], None]):
        if callback in self._out_callbacks: self._out_callbacks.remove(callback)

    def RemoveOnErr(self, callback: Callable[[str], None]):
        if callback in self._err_callbacks: self._err_callbacks.remove(callback)

    def ExecAsync(self, cmd: str):
        _hash = GenerateId()
        self._done_stack.add(_hash)
        self._shell.Write(RemoveLeadingIndent(cmd))
        return _hash

    def AwaitDone(self, timeout: int|float|None = 15, _hash: str=None):
        def _await_done(await_timeout, delta):
            start = CurrentTimeMillis()
            while True:
                if len(self._done_stack)==0: break
                if _hash is not None and _hash not in self._done_stack: break
                if CurrentTimeMillis() - start > await_timeout*1000: return False
                self._sleep(delta)
            return True
        
        start = CurrentTimeMillis()
        _d = 0.5
        while True:
            try:
                if len(self._done_stack)==0: break
                if _hash is not None:
                    if _hash not in self._done_stack: break
                    _mark = _hash
                else:
                    _mark = next(iter(self._done_stack))
                self._shell.Write(f'echo "{self._MARK}.{_mark}"')
            except BrokenPipeError: break
            if _await_done(await_timeout=_d, delta=min(_d/5, 1)): break
            # _d = min(_d*2, 864000) # 10 days
            if timeout is not None and CurrentTimeMillis() - start > timeout*1000: break
        if len(self._done_stack) > 0:
                _mark = next(iter(self._done_stack))
                self._shell.Write(f'echo "{self._MARK}.{_mark}"')

    def Exec(self, cmd: str, timeout: int|None = None, history: bool=False) -> ShellResult:
        _out, _err = [], []
        def _log_err(msg):
            _err.append(msg)
        def _log_out(msg):
            _out.append(msg)
        if history:
            self.RegisterOnOut(_log_out)
            self.RegisterOnErr(_log_err)

        _hash = self.ExecAsync(cmd)
        self.AwaitDone(timeout=timeout, _hash=_hash)
        if history:
            self.RemoveOnOut(_log_out)
            self.RemoveOnErr(_log_err)
        return ShellResult(out=_out, err=_err)
