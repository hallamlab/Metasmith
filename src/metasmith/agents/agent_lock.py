"""One state-changing operation at a time per agent home, across threads and processes on this machine."""
from __future__ import annotations

import fcntl
import functools
import hashlib
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path

LOCK_DIR = Path(os.environ.get("XDG_CACHE_HOME") or Path.home()/".cache")/"metasmith"/"agent-locks"


class AgentBusyError(RuntimeError):
    pass


# Re-entry by the thread that holds a lock is allowed, since RunWorkflow stages
# first. flock belongs to an open file description, so a second open by another
# thread of this process is refused exactly as another process would be.
_held: dict[str, tuple[int, int, int]] = {}
_held_guard = threading.Lock()


def _lock_path(address: str) -> Path:
    return LOCK_DIR/f"{hashlib.sha256(address.encode()).hexdigest()[:16]}.lock"


def _read_note(path: Path) -> str:
    try:
        return path.read_text().strip() or "another operation"
    except OSError:
        return "another operation"


def Busy(address: str) -> str | None:
    path = _lock_path(address)
    with _held_guard:
        if str(path) in _held:
            return _read_note(path)
    if not path.exists():
        return None
    fd = os.open(path, os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    except BlockingIOError:
        return _read_note(path)
    finally:
        os.close(fd)
    return None


@contextmanager
def Exclusive(address: str, what: str):
    path = _lock_path(address)
    key, tid = str(path), threading.get_ident()
    with _held_guard:
        held = _held.get(key)
        if held is not None:
            fd, depth, owner = held
            if owner != tid:
                raise AgentBusyError(f"agent [{address}] is busy: {_read_note(path)}")
            _held[key] = (fd, depth + 1, owner)
    if held is None:
        LOCK_DIR.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise AgentBusyError(f"agent [{address}] is busy: {_read_note(path)}")
        os.ftruncate(fd, 0)
        os.write(fd, f"{what} (pid {os.getpid()}, since {time.strftime('%Y-%m-%d %H:%M:%S')})".encode())
        with _held_guard:
            _held[key] = (fd, 1, tid)
    try:
        yield
    finally:
        with _held_guard:
            fd, depth, owner = _held[key]
            if depth > 1:
                _held[key] = (fd, depth - 1, owner)
            else:
                del _held[key]
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)


def exclusive(what: str):
    def decorate(method):
        @functools.wraps(method)
        def locked(self, *args, **kwargs):
            with Exclusive(self.home.address, what):
                return method(self, *args, **kwargs)
        return locked
    return decorate
