from __future__ import annotations
import os
import re
from typing import IO, Callable, Any
import gevent
from gevent.lock import Semaphore as Condition
from gevent import Greenlet
from gevent.select import select

# from threading import Condition, Thread
# from select import select
# import subprocess

# from dataclasses import dataclass, field
# import json
# from pathlib import Path
# import pty
# import time
# import random
# from collections import deque
# import hashlib

from ..hashing import KeyGenerator
from ..serialization import StdTime
from ..logging import Log

_kg = KeyGenerator()
def GenerateId(l: int=12):
    return _kg.GenerateUID(l)
def ResetGenerator():
    global _kg
    _kg = KeyGenerator()

def CurrentTimeMillis():
    return StdTime.CurrentTimeMillis()

# removes: colors, escape, control sequences
# https://stackoverflow.com/questions/14693701/how-can-i-remove-the-ansi-escape-sequences-from-a-string-in-python 
def StripANSI(s: str):
    return re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])').sub('', s)

def RemoveTrailingNewline(s):
    while len(s) > 0 and s[-1] in {"\n", "\r"}:
        s = s[:-1]
    return s

def RemoveLeadingIndent(s: str):
    lines = s.split("\n")
    if len(lines) == 0: return s
    indent = 0
    for line in lines:
        if line == "": continue
        for c in line:
            if c not in {" ", "\t"}: break
            indent += 1
        break
    cleaned = "\n".join([l[indent:] for l in lines])
    cleaned = cleaned.strip()
    if lines[-1][indent:] == "": cleaned += "\n"
    return cleaned

def AwaitCheck(check: Callable[[], bool], timeout: float):
    """
    returns when @check succeeds or raises TimeoutError after @timeout seconds
    """
    timeout*=1000
    start = CurrentTimeMillis()
    dt = 0.02
    while True:
        if check(): return
        now = CurrentTimeMillis()
        remain = start+timeout-now
        if remain<=0: raise TimeoutError()
        gevent.sleep(min(dt, remain))
        dt *= 2

MAX_READERS = 256
_readers = set()
class NonBlockingReader:
    def __init__(self, io_handle: int, on_close: Callable[[NonBlockingReader], None] = None, sep: bytes = b"\n") -> None:
        self._callbacks = []
        self._lock = Condition()
        self._notify_out, self._notify_in = os.pipe() # https://stackoverflow.com/a/57341500/13690762
        self._on_close = on_close
        self._sep = sep
        self._worker = None
        self._is_closed = False
        self._io_handle = io_handle
        self._start(io_handle)

    def _start(self, io_handle: int):
        if len(_readers)>MAX_READERS:
            raise ConnectionError("too many readers")
        _readers.add(self)

        def reader(fd: int, callbacks: list[Callable[[bytes], None]]):
            _buffer = []
            def _try_read():
                nonlocal _buffer
                changed = False
                i=0
                while True:
                    # https://stackoverflow.com/a/21429655/13690762
                    r, _, _ = select([ fd, self._notify_out ], [], [], 60) # allows unblock with notify_out
                    # Log.Debug(f"r* [{id(self)}] [closed: {self.IsClosed()}] [notified: {self._notify_out in r}] ")
                    if self.IsClosed():
                        return []
                    chunk = os.read(fd, 4096)
                    if len(chunk) == 0:
                        with self._lock:
                            i+=1
                            print(i, end="\r")
                            self._lock.wait(0.1)
                            # gevent.sleep(1/100)
                        continue
                    _buffer.append(chunk)
                    changed = True
                    if self._sep in chunk: break # line complete
                if not changed: return []

                complete_segments = []
                remainder = []
                lines = b''.join(_buffer).split(self._sep)
                for i, line in enumerate(lines):
                    if i < len(lines)-1:
                        complete_segments.append(line)
                    else: # last chunk
                        if len(line) > 0: remainder.append(line) # save for later if incomplete
                _buffer.clear()
                _buffer.extend(remainder)
                return complete_segments

            initial_wait, max_wait = 0.1, 600
            clear = True
            wait = 0.1
            def reset_wait():
                nonlocal wait, clear
                wait = initial_wait
                clear = True
            
            def scaling_wait():
                nonlocal wait, clear
                with self._lock:
                    self._lock.wait(wait)
                if not clear:
                    wait = min(wait*2, max_wait)
                clear = False

            while not self.IsClosed():
                try:
                    lines = list(_try_read())
                    for line in lines:
                        # Log.Debug(f"--- {line}")
                        for cb in callbacks: cb(line)
                    reset_wait()
                except OSError as e: # fd closed
                    if e.errno == 9: # Bad file descriptor
                        break
                    else: # likely a race condition
                        # scaling_wait()
                        gevent.sleep(1/100)
                except KeyboardInterrupt:
                    with self._lock:
                        self._is_closed=True
                    break
            if callable(self._on_close): self._on_close(self)

        # self._worker = Thread(target=reader, args=[io_handle, self._callbacks])
        self._worker = Greenlet(reader, io_handle, self._callbacks)
        self._worker.start()

    def RegisterCallback(self, callback: Callable[[bytes], None]):
        self._callbacks.append(callback)

    def RemoveCallback(self, callback: Callable[[bytes], None]):
        self._callbacks.remove(callback)

    def IsClosed(self):
        with self._lock:
            return self._is_closed

    def Dispose(self):
        try:
            while True:
                with self._lock:
                    self._is_closed = True

                if self._worker is None: break
                # if not self._worker.is_alive(): break
                if self._worker.dead: break
                try:
                    os.write(self._notify_in, b"dispose") # unblock reader
                except OSError:
                    pass
                
                try:
                    self._worker.join(1)
                    break
                except RuntimeError as e:
                    Log.Error(f"NonBlockingReader.Dispose() [{e}]")
                    break
                except TimeoutError:
                    continue # try again

            for fd in [self._notify_in, self._notify_out]:
                try:
                    os.close(fd)
                except OSError as e:
                    pass
                #     Log.Error(f"NonBlockingReader.Dispose() fd:{fd} [{e}]")
        finally:
            if self in _readers:
                _readers.remove(self)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.Dispose()
