from __future__ import annotations

import contextvars
import logging
import queue
import threading
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

from .store import utcnow

PENDING = "pending"
RUNNING = "running"
DONE = "done"
FAILED = "failed"
TERMINAL = {DONE, FAILED}

_MAX_LINES = 5000
_MAX_FINISHED = 200

_server_log = logging.getLogger("metasmith.gui.jobs")


@dataclass
class Job:
    id: str
    kind: str
    label: str
    subject: dict = field(default_factory=dict)
    status: str = PENDING
    created_at: str = field(default_factory=utcnow)
    started_at: str | None = None
    finished_at: str | None = None
    result: Any = None
    error: str | None = None

    def __post_init__(self):
        self._lines: list[str] = []
        self._lock = threading.Lock()
        self._subscribers: set[queue.Queue] = set()
        self._done = threading.Event()


    def emit(self, line: str):
        for part in str(line).rstrip("\n").split("\n"):
            with self._lock:
                self._lines.append(part)
                if len(self._lines) > _MAX_LINES:
                    del self._lines[: len(self._lines) - _MAX_LINES]
                subscribers = list(self._subscribers)
            for q in subscribers:
                q.put(part)

    def lines(self) -> list[str]:
        with self._lock:
            return list(self._lines)

    def finish(self):
        with self._lock:
            subscribers = list(self._subscribers)
        self._done.set()
        for q in subscribers:
            q.put(None)

    def subscribe(self) -> Iterator[str]:
        q: queue.Queue = queue.Queue()
        with self._lock:
            backlog = list(self._lines)
            self._subscribers.add(q)
        try:
            for line in backlog:
                yield line
            while True:
                if self._done.is_set() and q.empty():
                    return
                try:
                    line = q.get(timeout=0.5)
                except queue.Empty:
                    continue
                if line is None:
                    continue
                yield line
        finally:
            with self._lock:
                self._subscribers.discard(q)

    def wait(self, timeout: float | None = None) -> bool:
        return self._done.wait(timeout)

    def summary(self) -> dict:
        return {
            "id": self.id,
            "kind": self.kind,
            "label": self.label,
            "subject": self.subject,
            "status": self.status,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
            "result": self.result,
        }


class JobRunner:
    def __init__(self):
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()
        self._counter = 0

    def _next_id(self, kind: str) -> str:
        with self._lock:
            self._counter += 1
            return f"{kind}-{self._counter}"

    def submit(
        self,
        kind: str,
        label: str,
        fn: Callable[[Job], Any],
        subject: dict | None = None,
    ) -> Job:
        job = Job(id=self._next_id(kind), kind=kind, label=label, subject=subject or {})
        with self._lock:
            self._jobs[job.id] = job
            self._prune()

        def _run():
            job.status = RUNNING
            job.started_at = utcnow()
            try:
                job.result = fn(job)
                job.status = DONE
            except Exception as exc:
                job.status = FAILED
                job.error = str(exc) or exc.__class__.__name__
                job.emit(f"ERROR: {job.error}")
                _server_log.error("job [%s] failed\n%s", job.id, traceback.format_exc())
            finally:
                job.finished_at = utcnow()
                job.finish()

        threading.Thread(target=_run, name=f"msm-gui-{job.id}", daemon=True).start()
        return job

    def _prune(self):
        finished = sorted(
            (j for j in self._jobs.values() if j.status in TERMINAL),
            key=lambda j: j.finished_at or "",
        )
        for j in finished[: max(0, len(finished) - _MAX_FINISHED)]:
            del self._jobs[j.id]

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list(self, subject_key: str | None = None, subject_value: str | None = None) -> list[dict]:
        with self._lock:
            jobs = list(self._jobs.values())
        if subject_key is not None:
            jobs = [j for j in jobs if j.subject.get(subject_key) == subject_value]
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return [j.summary() for j in jobs]

    def active(self) -> list[Job]:
        with self._lock:
            return [j for j in self._jobs.values() if j.status not in TERMINAL]


_active_job: contextvars.ContextVar[Job | None] = contextvars.ContextVar("msm_gui_job", default=None)


class _JobLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord):
        job = _active_job.get()
        if job is None:
            return
        try:
            job.emit(self.format(record))
        except Exception:
            pass


_handler_installed = False
_handler_lock = threading.Lock()


def install_log_capture():
    global _handler_installed
    with _handler_lock:
        if _handler_installed:
            return
        from .. import logging as msm_logging

        handler = _JobLogHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logging.getLogger(msm_logging.Log.GetName()).addHandler(handler)
        _handler_installed = True


class LogCapture:
    def __init__(self, job: Job):
        self.job = job
        self._token = None

    def __enter__(self):
        install_log_capture()
        self._token = _active_job.set(self.job)
        return self

    def __exit__(self, *exc):
        _active_job.reset(self._token)
        return False
