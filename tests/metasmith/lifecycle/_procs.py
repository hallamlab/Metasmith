"""Observing real process trees: liveness, group liveness, and polling.

Kept out of conftest.py so test modules can import it by name -- there is no
package here, so a relative import would have no parent.
"""
import os
import time
from pathlib import Path


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    return True


def group_alive(pgid: int) -> bool:
    # PermissionError means the group exists and is someone else's -- alive, and
    # not ours to stop. Only ProcessLookupError means gone.
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    return True


def pids_matching(needle: str) -> list[int]:
    # /proc rather than pgrep: pgrep -f would also match this test process, whose
    # own cmdline mentions the needle.
    out = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or entry.name == str(os.getpid()):
            continue
        try:
            cmd = (entry/"cmdline").read_bytes().replace(b"\0", b" ").decode()
        except OSError:
            continue
        if needle in cmd:
            out.append(int(entry.name))
    return out


def wait_until(predicate, timeout: float = 15.0, interval: float = 0.1) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()
