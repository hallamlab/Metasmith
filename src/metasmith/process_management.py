import os
import re
import signal
from dataclasses import dataclass
from typing import IO, Any, Callable
from threading import Condition
from threading import Thread
from multiprocessing import Queue
import subprocess
import select
import pty
import time

from .utils import StdTime

# removes: colors, escape, control sequences
# https://stackoverflow.com/questions/14693701/how-can-i-remove-the-ansi-escape-sequences-from-a-string-in-python 
def StripANSI(s: str):
    return re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])').sub('', s)

class LiveShell:
    def __init__(self, silent=True) -> None:
        self._shell = None
        self._MARK = "done"
        self._done = False
        self._silent = silent

    def Exec(self, cmd: str):
        def _strip_indent(s):
            lines = s.split("\n")
            if len(lines) == 0: return s
            indent = 0
            for c in lines[0]:
                if c not in {" ", "\t"}: break
                indent += 1
            return "\n".join([l[indent:] for l in lines])
        
        def _await_done(timeout=10, delta=0.1):
            start = StdTime.CurrentTimeMillis()
            while True:
                time.sleep(delta)
                if self._done: break
                if StdTime.CurrentTimeMillis() - start > timeout*1000: return False
            self._done = False
            return True
        
        _out, _err = [], []
        def _decode(x):
            return self._remove_trailing_newline(self._shell.Decode(x))
        def _appender_out(x):
            msg = _decode(x)
            if msg == self._MARK: return
            _out.append(msg)
        def _appender_err(x):
            _err.append(_decode(x))
        self._shell.RegisterOnOut(_appender_out)
        self._shell.RegisterOnErr(_appender_err)

        self._shell.Write(_strip_indent(cmd))
        while True:
            self._shell.Write("echo done")
            if _await_done(timeout=0.5, delta=0.1): break

        self._shell.RemoveOnOut(_appender_out)
        self._shell.RemoveOnErr(_appender_err)
        return _out, _err
    
    def __enter__(self):
        self._shell = TerminalProcess()

        def _check(x):
            if len(x) > len(self._MARK)+2: return
            x = self._remove_trailing_newline(self._shell.Decode(x))
            if x == self._MARK: self._done = True
        self._shell.RegisterOnOut(_check)

        def _tee(prefix):
            def _cb(x):
                msg = self._remove_trailing_newline(self._shell.Decode(x))
                if len(msg) == 0: return
                if msg == self._MARK: return
                print(f"{prefix}{msg}")
            return _cb
        if not self._silent:
            self._shell.RegisterOnErr(_tee("E: "))
            self._shell.RegisterOnOut(_tee("I: "))

        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._shell.Dispose()
        self._shell = None

    def _remove_trailing_newline(self, s):
        while len(s) > 0 and s[-1] in {"\n", "\r"}:
            s = s[:-1]
        return s

class TerminalProcess:
    class Pipe:
        def __init__(self, io:IO[bytes]|None, lock: Condition=Condition(), q: Queue=Queue()) -> None:
            assert io is not None
            self.IO = io
            self.Lock = lock
            self.Q = q

        def __enter__(self):
            self.Lock.acquire()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.Lock.release()

    def __init__(self) -> None:

        # https://stackoverflow.com/questions/41542960/run-interactive-bash-with-popen-and-a-dedicated-tty-python
        out_master, out_slave = pty.openpty()
        err_master, err_slave = pty.openpty()
        self._fds = [out_master, err_master]

        console = subprocess.Popen(
            ["bash"],
            stdin=subprocess.PIPE,
            stdout=out_slave,
            stderr=err_slave,
            close_fds=True
        )

        self.ENCODING = "utf-8"
        self._console = console
        self._in = TerminalProcess.Pipe(console.stdin)
        self._onCloseLock = Condition()
        self._closed = False
        self.pid = console.pid
        self._on_out_callbacks = []
        self._on_err_callbacks = []

        workers: list[Thread] = []
        def reader(fd: int, callbacks):
            _buffer = []
            def _try_read():
                nonlocal _buffer
                changed = False
                while True:
                    # https://stackoverflow.com/a/21429655/13690762
                    r, _, _ = select.select([ fd ], [], [], 0.1)
                    if fd in r:
                        _buffer.append(os.read(fd, 1024))
                        changed = True
                    else:
                        break
                if not changed: return []
                        
                complete_segments = []
                remainder = []
                for line in b''.join(_buffer).splitlines(True):
                    if line.endswith(b'\n'):
                        complete_segments.append(line)
                    else: # must be the remaining segment
                        remainder.append(line)
                _buffer.clear()
                _buffer.extend(remainder)
                return complete_segments

            while True:
                if self.IsClosed(): break
                try:
                    lines = list(_try_read())
                    for line in lines:
                        if line is None: break
                        for cb in callbacks: cb(line)
                except OSError: # fd closed
                    break

        workers.append(Thread(target=reader, args=[out_master, self._on_out_callbacks]))
        workers.append(Thread(target=reader, args=[err_master, self._on_err_callbacks]))
        self._workers = workers
        for w in workers:
            w.daemon = True # stop with program
            w.start()

    def Send(self, payload: bytes):
        stdin = self._in
        with self._in:
            stdin.IO.write(payload)
            stdin.IO.flush()
    
    def Decode(self, payload: bytes):
        return payload.decode(encoding=self.ENCODING)

    def Write(self, msg: str):
        self.Send(bytes('%s\n' % (msg), encoding=self.ENCODING))

    def RegisterOnOut(self, callback: Callable[[bytes], None]):
        self._on_out_callbacks.append(callback)

    def RegisterOnErr(self, callback: Callable[[bytes], None]):
        self._on_err_callbacks.append(callback)

    def RemoveOnOut(self, callback: Callable[[bytes], None]):
        self._on_out_callbacks.remove(callback)

    def RemoveOnErr(self, callback: Callable[[bytes], None]):
        self._on_err_callbacks.remove(callback)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.Dispose()
        return

    def IsClosed(self):
        with self._onCloseLock:
            return self._closed

    def Dispose(self):
        with self._onCloseLock:
            if self._closed:
                return
            self._closed = True
            self._onCloseLock.notify_all()

        for w in self._workers:
            w.join()

        self._console.terminate()
        for i, fd in enumerate(self._fds):
            os.close(fd)

