from __future__ import annotations

import os
import time

import pytest

from metasmith.coms.ipc import NonBlockingReader
from metasmith.gui import jobs
from metasmith.gui.jobs import Job, JobRunner, LogCapture
from metasmith.logging import Log

pytestmark = pytest.mark.gui


def _wait_for(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_a_reader_opened_inside_a_capture_logs_into_its_job():
    job = Job(id="j-1", kind="t", label="t")
    rfd, wfd = os.pipe()
    with LogCapture(job):
        reader = NonBlockingReader(rfd)
        reader.RegisterCallback(lambda line: Log.Info(f"> {line}"))
    os.write(wfd, b"from the remote\n")
    try:
        assert _wait_for(lambda: any("from the remote" in l for l in job.lines()))
    finally:
        os.close(wfd)
        reader.Dispose()


def test_a_failure_shows_its_error_and_keeps_the_traceback_out_of_the_log():
    runner = JobRunner()

    def boom(job):
        raise RuntimeError("the agent said no")

    job = runner.submit("t", "t", boom)
    assert job.wait(10)
    assert job.lines() == ["ERROR: the agent said no"]


def test_finished_jobs_are_pruned(monkeypatch):
    monkeypatch.setattr(jobs, "_MAX_FINISHED", 3)
    runner = JobRunner()
    done = [runner.submit("t", "t", lambda job: None) for _ in range(6)]
    for job in done:
        assert job.wait(10)
    last = runner.submit("t", "t", lambda job: None)
    assert last.wait(10)
    kept = [j["id"] for j in runner.list()]
    assert len(kept) <= 4 and last.id in kept
