"""The mechanism everything else rests on: a signal to a process group.

Nothing of metasmith is in the first two tests. They are the characterization of
the assumption -- that `kill -TERM -<pgid>` reaches a shell's children and that
signalling the leader alone does not -- and they are what would speak up on a
platform where it does not hold.
"""
import os
import signal
import subprocess
import time

import pytest

from metasmith.coms.terminals import LiveShell, TerminalProcess

from _procs import alive, group_alive, pids_matching, wait_until


def _children_of(proc, *needles: str) -> list[int]:
    found: list[int] = []
    for needle in needles:
        found += [p for p in pids_matching(needle) if p != proc.pid]
    return found


def test_group_kill_reaches_children(spawner):
    proc = spawner("sleep 400 & sleep 401 & wait")
    assert wait_until(lambda: len(_children_of(proc, "sleep 400", "sleep 401")) == 2)
    children = _children_of(proc, "sleep 400", "sleep 401")

    os.killpg(proc.pid, signal.SIGTERM)
    proc.wait(timeout=5)

    assert wait_until(lambda: not any(alive(p) for p in children)), (
        "a group TERM left children of the group leader running"
    )
    assert wait_until(lambda: not group_alive(proc.pid))


def test_leader_only_kill_orphans_children(spawner):
    proc = spawner("sleep 402 & sleep 403 & wait")
    assert wait_until(lambda: len(_children_of(proc, "sleep 402", "sleep 403")) == 2)
    children = _children_of(proc, "sleep 402", "sleep 403")

    os.kill(proc.pid, signal.SIGKILL)
    proc.wait(timeout=5)
    time.sleep(0.5)

    assert all(alive(p) for p in children), (
        "this platform kills a shell's children with the shell; the group "
        "machinery this suite tests would then be unnecessary, not wrong"
    )
    for p in children:
        os.kill(p, signal.SIGKILL)


def test_terminal_process_dispose_leaves_no_descendants():
    # TerminalProcess owns the session it created, so Dispose signals the group.
    shell = LiveShell()
    shell.Exec("sleep 404 & echo started", history=True)
    assert wait_until(lambda: pids_matching("sleep 404"))
    children = pids_matching("sleep 404")

    shell.Dispose()

    assert wait_until(lambda: not any(alive(p) for p in children)), (
        "LiveShell.Dispose orphaned a process the shell started"
    )


def test_dispose_without_a_session_signals_only_the_shell():
    # new_session=False is for a shell that must stay inside the caller's group
    # -- the run driver's. Signalling the group there would signal the caller.
    term = TerminalProcess(new_session=False)
    assert os.getpgid(term.pid) == os.getpgid(0)
    term.Write("sleep 405 &")
    assert wait_until(lambda: pids_matching("sleep 405"))
    orphans = pids_matching("sleep 405")

    term.Dispose()

    assert wait_until(lambda: not alive(term.pid))
    assert all(alive(p) for p in orphans)
    for p in orphans:
        os.kill(p, signal.SIGKILL)
