"""Re-attaching to runs the server did not start.

`RunWorkflow` launches Nextflow detached on the agent and returns immediately, so
the GUI is never the run's parent process. Nothing about a live run is held in
memory: the run record on disk is the only thing that knows it exists, which is
what lets a restarted server -- or one whose browser tab was closed hours ago --
pick it back up.

Polling is server-side and rate-limited on purpose. Every probe against a remote
agent is an SSH round trip, so letting each open page drive its own poll would
multiply round trips by the number of tabs.
"""
from __future__ import annotations

import threading

from ..ops import runtime as op_runtime
from .store import Project, utcnow

# probe status -> run state. "timeout" means the sentinel has not appeared yet,
# which is the normal answer for a run that is still going.
_STATUS_TO_STATE = {
    "completed": "completed",
    "errored": "failed",
}


class RunWatcher:
    def __init__(self, project: Project, interval_s: float = 10.0):
        self.project = project
        self.interval_s = interval_s
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
        """Nudge the loop. Membership comes from disk, so there is nothing to register."""
        self._wake.set()

    def _loop(self):
        while not self._stop.is_set():
            try:
                self.poll_once()
            except Exception:
                pass  # a watcher that dies is worse than one that misses a cycle
            self._wake.wait(self.interval_s)
            self._wake.clear()

    def poll_once(self) -> list[dict]:
        out = []
        for rec in self.project.live_runs():
            out.append(self._probe(rec))
        return [x for x in out if x]

    def _probe(self, rec) -> dict | None:
        agent_name = rec.record.get("agent")
        key = rec.record.get("task_key")
        if not agent_name or not key or not self.project.agent_exists(agent_name):
            return None
        if rec.state in {"staging", "launching"}:
            return None  # the launching job owns these states
        try:
            probe = op_runtime.wait(
                str(self.project.agent_path(agent_name)),
                key,
                timeout_s=0.0,
                poll_s=1.0,
                run=rec.record.get("run_number"),
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
