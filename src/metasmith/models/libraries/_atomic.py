"""Writing a library index without leaving a half-written one behind.

`Save()` used to be a bare truncating `open(..., "w")` followed by a
`yaml.dump`. That is fine for a library one process builds and then reads, and
it is not fine for one several concurrent plans load: a reader arriving mid-dump
sees a truncated document, and a dump that raises leaves the index destroyed
rather than stale.

`os.replace` is what buys correctness here -- it is atomic within a filesystem,
so a reader sees either the whole old file or the whole new one and never a
prefix. The lock is a *courtesy* on top: it serialises two writers so the last
one to finish wins cleanly instead of both racing, and it is advisory, bounded,
and unreliable on NFS. Do not read it as mutual exclusion; read `os.replace` as
the guarantee and the lock as noise reduction.

The temp name carries pid and thread for the same reason `gui/store.py` does:
one fixed `.tmp` beside the target makes two concurrent writes fight over one
path, and the loser fails at the rename with an error that names nothing useful.
"""

from __future__ import annotations

import os
import socket
import threading
import time
from pathlib import Path

import yaml


#: How long to wait for another writer's lock before giving up and writing
#: anyway. Giving up is deliberate: the write is atomic regardless, so a stale
#: lock left by a killed process must not wedge every future save.
_LOCK_WAIT_S = 5.0
_LOCK_POLL_S = 0.05


def _acquire(lock: Path) -> bool:
    """O_EXCL create, following `caching/promote.py`'s shape. Best effort."""
    payload = f"{os.getpid()} {socket.gethostname()} {time.time():.6f}\n"
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        os.write(fd, payload.encode())
        os.close(fd)
        return True
    except FileExistsError:
        return False
    except OSError:
        # A read-only or otherwise unwritable metadata directory. The caller is
        # about to fail on the real write with a message that says so; do not
        # pre-empt it with a confusing one about a lock file.
        return True


def write_yaml_atomic(path: Path, data: dict, *, sort_keys: bool = True) -> None:
    """Replace `path` with `data`, atomically, under a best-effort lock."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = path.with_name(path.name + ".lock")
    held = False
    deadline = time.time() + _LOCK_WAIT_S
    while time.time() < deadline:
        if _acquire(lock):
            held = True
            break
        time.sleep(_LOCK_POLL_S)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident():x}.tmp")
    try:
        with open(tmp, "w") as f:
            yaml.dump(data, f, sort_keys=sort_keys)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    finally:
        if held:
            lock.unlink(missing_ok=True)
