from __future__ import annotations
import os
import re
from typing import IO, Callable, Any

from threading import Condition, Thread
from select import select
import subprocess
from time import sleep, monotonic

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
    """Dedent by the COMMON indent, not by the first non-empty line's.

    Measuring off the first line and slicing that many characters off every line
    destroys any line that is deliberately less indented -- and a heredoc body is
    exactly that. An embedded

        python3 - <<'PY'
    lines of script at column 0
    PY

    lost 8 characters from every line of the script and the terminator lost its
    own line, so the shell never saw `PY` and the command died on an unterminated
    heredoc. Taking the minimum leaves such a block untouched.

    Whitespace-only lines carry no indent information (a blank line inside an
    otherwise indented block is usually truly empty), so they do not drag the
    minimum to zero.
    """
    lines = s.split("\n")
    if len(lines) == 0: return s
    def _indent_of(line: str):
        n = 0
        for c in line:
            if c not in {" ", "\t"}: return n
            n += 1
        return None  # whitespace-only
    indents = [i for i in (_indent_of(l) for l in lines) if i is not None]
    indent = min(indents) if indents else 0
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
        sleep(min(dt, remain))
        dt *= 2

MAX_READERS = 256
# Pathological-input guard: cap how much we'll buffer for an incomplete
# line. Hitting this cap emits one Log.Warning and drops the head of the
# buffer; the stream stays alive (G8).
MAX_LINE_BYTES = 1 << 20  # 1 MiB
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
        # Marked on every non-empty read, *before* the line split. A caller
        # bounding an operation by silence needs to know the far end is still
        # emitting bytes -- rsync's progress redraws a line with carriage
        # returns and may not complete one for minutes on a large file, so a
        # mark taken per line would call a healthy transfer dead.
        self._last_read_at = monotonic()
        self._start(io_handle)

    def _start(self, io_handle: int):
        if len(_readers)>MAX_READERS:
            raise ConnectionError("too many readers")
        _readers.add(self)

        def reader(fd: int, callbacks: list[Callable[[bytes], None]]):
            _buffer = []
            _buffer_bytes = 0
            _eof = [False]
            _truncation_warned = [False]
            def _try_read():
                nonlocal _buffer, _buffer_bytes
                changed = False
                while True:
                    # https://stackoverflow.com/a/21429655/13690762
                    r, _, _ = select([ fd, self._notify_out ], [], [], 60) # allows unblock with notify_out
                    if self.IsClosed():
                        return []
                    if fd not in r:
                        # select woke for notify_out (dispose) or timed out; loop and re-check
                        continue
                    try:
                        chunk = os.read(fd, 4096)
                    except OSError:
                        # fd closed underneath us
                        _eof[0] = True
                        break
                    if len(chunk) == 0:
                        # Real EOF on the fd. Flush any remainder and stop.
                        _eof[0] = True
                        break
                    self._last_read_at = monotonic()
                    _buffer.append(chunk)
                    _buffer_bytes += len(chunk)
                    # Bound the buffer: if a single line exceeds MAX_LINE_BYTES,
                    # drop from the head, warn once, and keep going.
                    if _buffer_bytes > MAX_LINE_BYTES:
                        joined = b''.join(_buffer)
                        joined = joined[-MAX_LINE_BYTES:]
                        _buffer = [joined]
                        _buffer_bytes = len(joined)
                        if not _truncation_warned[0]:
                            Log.Warn(f"NonBlockingReader: truncating oversized incomplete line on fd {fd}")
                            _truncation_warned[0] = True
                    changed = True
                    if self._sep in chunk: break # line complete
                if not changed and not _eof[0]: return []

                joined = b''.join(_buffer)
                lines = joined.split(self._sep)
                complete_segments = []
                remainder = b''
                for i, line in enumerate(lines):
                    if i < len(lines)-1:
                        complete_segments.append(line)
                    else:
                        remainder = line
                _buffer.clear()
                _buffer_bytes = 0
                if _eof[0]:
                    # On EOF, emit whatever's left as a final segment
                    if len(remainder) > 0:
                        complete_segments.append(remainder)
                else:
                    if len(remainder) > 0:
                        _buffer.append(remainder)
                        _buffer_bytes = len(remainder)
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
                        for cb in list(callbacks):
                            try:
                                cb(line)
                            except Exception as cb_err:
                                Log.Error(f"NonBlockingReader callback raised: [{cb_err}]")
                    reset_wait()
                    if _eof[0]:
                        # EOF reached on fd. Stop the reader cleanly.
                        with self._lock:
                            self._is_closed = True
                        break
                except OSError as e: # fd closed
                    if e.errno == 9: # Bad file descriptor
                        break
                    else: # likely a race condition
                        scaling_wait()
                except KeyboardInterrupt:
                    with self._lock:
                        self._is_closed=True
                    break
            if callable(self._on_close): self._on_close(self)

        self._worker = Thread(target=reader, args=[io_handle, self._callbacks])
        # self._worker = Greenlet(reader, io_handle, self._callbacks)
        self._worker.start()

    def RegisterCallback(self, callback: Callable[[bytes], None]):
        self._callbacks.append(callback)

    def RemoveCallback(self, callback: Callable[[bytes], None]):
        self._callbacks.remove(callback)

    def IsClosed(self):
        with self._lock:
            return self._is_closed

    def SecondsSinceRead(self) -> float:
        """How long this stream has been silent, in seconds."""
        return monotonic() - self._last_read_at

    def Dispose(self):
        try:
            while True:
                with self._lock:
                    self._is_closed = True

                if self._worker is None: break
                if not self._worker.is_alive(): break
                # if self._worker.dead: break
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
