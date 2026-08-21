"""Spawning real process trees, and cleaning them up whatever the test did.

Everything a lifecycle test starts goes through `spawner`, which group-kills in
teardown unconditionally -- a test that fails partway would otherwise leave live
processes behind, and the next test would see them.
"""
import os
import signal
import subprocess

import pytest


@pytest.fixture
def spawner(tmp_path):
    started: list[subprocess.Popen] = []

    def _spawn(script: str, **kw) -> subprocess.Popen:
        # Run from a file rather than `bash -c`: with -c the leader's own
        # cmdline is the script text, so it matches every command the script
        # mentions and a /proc scan cannot tell leader from descendant.
        path = tmp_path/f"spawn.{len(started)}.sh"
        path.write_text(script)
        kw.setdefault("stdout", subprocess.DEVNULL)
        kw.setdefault("stderr", subprocess.DEVNULL)
        proc = subprocess.Popen(
            ["bash", str(path)], start_new_session=True, **kw,
        )
        started.append(proc)
        return proc

    yield _spawn

    for proc in started:
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(proc.pid, sig)
            except (ProcessLookupError, PermissionError):
                pass
            try:
                proc.wait(timeout=2)
                break
            except subprocess.TimeoutExpired:
                pass
