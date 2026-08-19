from __future__ import annotations

import threading
from datetime import datetime, timezone

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
        try:
            probe = op_runtime.wait(
                str(self.project.agent_path(agent_name)),
                key,
                timeout_s=0.0,
                poll_s=1.0,
                run=rec.record.get("run_number"),
                grace_s=(
                    0.0
                    if _age_s(rec.record.get("launched_at")) > _DRIVER_GRACE_S
                    else _DRIVER_GRACE_S
                ),
            )
        except Exception as exc:
            return {"run": rec.name, "error": str(exc)}

        state = _STATUS_TO_STATE.get(probe.get("status", ""))
        if state is None:
            return None
        patch = {"state": state, "finished_at": utcnow()}
        if state == "failed":
            patch["error"] = "the run stopped without reporting completion"
        self.project.update_run(rec.workflow, rec.name, patch)
        return {"run": rec.name, "state": state}
