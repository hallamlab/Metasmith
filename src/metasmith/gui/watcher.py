from __future__ import annotations

import threading
from datetime import datetime, timezone

from ..logging import Log
from ..ops import runtime as op_runtime
from .store import Project, utcnow

_STATUS_TO_STATE = {
    "completed": "completed",
    "errored": "failed",
}

_LAUNCH_STATES = {"staging", "staged", "launching"}

_DRIVER_GRACE_S = 30.0

_ORPHAN_GRACE_S = 60.0


def _age_s(stamp: str | None) -> float:
    if not stamp:
        return 0.0
    try:
        t = datetime.fromisoformat(stamp)
    except ValueError:
        return 0.0
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - t).total_seconds())


class RunWatcher:
    def __init__(
        self,
        project: Project,
        interval_s: float = 10.0,
        instance_id: str | None = None,
        jobs=None,
    ):
        self.project = project
        self.interval_s = interval_s
        self.instance_id = instance_id
        self.jobs = jobs
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="msm-gui-watcher", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._wake.set()

    def watch(self, workflow: str, run: str):
        self._wake.set()

    def _loop(self):
        while not self._stop.is_set():
            try:
                self.poll_once()
            except Exception:
                pass
            self._wake.wait(self.interval_s)
            self._wake.clear()

    def poll_once(self) -> list[dict]:
        out = []
        for rec in self.project.live_runs():
            try:
                out.append(self._probe(rec))
            except Exception as exc:
                out.append({"run": rec.name, "error": str(exc)})
        return [x for x in out if x]

    def _launch_is_owned(self, rec) -> bool:
        launcher = rec.record.get("launched_by")
        if self.instance_id is None or launcher != self.instance_id:
            return False
        if self.jobs is None:
            return True
        mine = [
            j for j in self.jobs.active()
            if j.subject.get("workflow") == rec.workflow and j.subject.get("run") == rec.name
        ]
        if mine:
            return True
        return _age_s(rec.record.get("created_at")) < _ORPHAN_GRACE_S

    def _probe(self, rec) -> dict | None:
        agent_name = rec.record.get("agent")
        key = rec.record.get("task_key")
        if rec.state in _LAUNCH_STATES:
            if self._launch_is_owned(rec):
                return None
            self.project.update_run(rec.workflow, rec.name, {
                "state": "failed",
                "finished_at": utcnow(),
                "error": (
                    f"the server that was {rec.state} this run is gone; "
                    f"nothing on the agent was left running, so it can be launched again"
                ),
            })
            return {"run": rec.name, "state": "failed"}
        if not agent_name or not key or not self.project.agent_exists(agent_name):
            return None
        launch_age_s = _age_s(rec.record.get("launched_at"))
        driver_grace = launch_age_s <= _DRIVER_GRACE_S
        try:
            probe = op_runtime.wait(
                str(self.project.agent_path(agent_name)),
                key,
                timeout_s=0.0,
                poll_s=1.0,
                run=rec.record.get("run_number"),
                grace_s=(0.0 if not driver_grace else _DRIVER_GRACE_S),
            )
        except Exception as exc:
            # A run stuck here never recovers on its own -- the same failure
            # repeats identically every tick -- so unlike a `None` return
            # (genuinely "still running, ask again later") this has to reach
            # the run record, or the GUI shows a live run forever with nothing
            # to explain why it never finishes.
            Log.Warn(f"probe failed for run [{rec.workflow}/{rec.name}]: {exc}")
            self.project.update_run(rec.workflow, rec.name, {
                "probe_error": str(exc), "probe_error_at": utcnow(),
            })
            return {"run": rec.name, "error": str(exc)}

        status = probe.get("status", "")
        state = _STATUS_TO_STATE.get(status)
        if state is None:
            # "timeout" is the expected, steady-state answer for a run that is
            # genuinely still going (the watcher's own probe uses timeout_s=0,
            # so almost every live tick reports it) -- nothing to do but ask
            # again next tick. "missing" means `agent.log` itself never
            # appeared, which is only expected for the few seconds it takes
            # the driver to start up; past the driver grace window it means
            # the driver never ran here at all (most often: a later run
            # silently took over this task_key's shared workspace before this
            # one wrote anything -- see `Project.create_run`), and nothing
            # will ever change that on its own.
            if status == "missing" and not driver_grace:
                self.project.update_run(rec.workflow, rec.name, {
                    "state": "failed",
                    "finished_at": utcnow(),
                    "error": (
                        "no log ever appeared for this run -- it was likely superseded "
                        "by a later run on the same workspace before it could start"
                    ),
                    "probe_error": None, "probe_error_at": None,
                })
                return {"run": rec.name, "state": "failed"}
            if rec.record.get("probe_error"):
                self.project.update_run(rec.workflow, rec.name, {
                    "probe_error": None, "probe_error_at": None,
                })
            return None
        patch = {"state": state, "finished_at": utcnow(), "probe_error": None, "probe_error_at": None}
        if state == "failed":
            patch["error"] = "the run stopped without reporting completion"
        self.project.update_run(rec.workflow, rec.name, patch)
        return {"run": rec.name, "state": state}
