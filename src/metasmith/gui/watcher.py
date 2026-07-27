"""Re-attaching to runs the server did not start.

`RunWorkflow` launches Nextflow detached on the agent and returns immediately, so
the GUI is never the run's parent process. Nothing about a live run is held in
memory: the run record on disk is the only thing that knows it exists, which is
what lets a restarted server -- or one whose browser tab was closed hours ago --
pick it back up.

Polling is server-side and rate-limited on purpose. Every probe against a remote
agent is an SSH round trip, so letting each open page drive its own poll would
multiply round trips by the number of tabs.

The other half of that promise is reconciliation. `staging` and `launching` are
owned by a daemon thread in *this* process and by nothing on disk, so a server
that is restarted -- or Ctrl-C'd, or whose launch thread died on a hung SSH --
leaves runs claiming to be staged by a thread that no longer exists. Every run
records which server instance launched it; a run in one of those states whose
launcher is gone is resolved here rather than sitting at `staging` for good.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone

from ..ops import runtime as op_runtime
from .store import Project, utcnow

# probe status -> run state. "timeout" means the sentinel has not appeared yet,
# which is the normal answer for a run that is still going.
_STATUS_TO_STATE = {
    "completed": "completed",
    "errored": "failed",
}

# The states a launch job owns while it is running. Nothing on the agent can
# move these -- there is no task staged yet to probe -- so they are resolved by
# asking whether the thread that owns them still exists.
_LAUNCH_STATES = {"staging", "staged", "launching"}

# How long a freshly-launched run is left alone before "the driver is gone"
# is believed: nextflow writes its PID lock a moment after it is started.
_DRIVER_GRACE_S = 30.0

# And how long a run in a launch state is left alone when this process has no
# job for it at all. That is normally a launch submitted microseconds ago and
# not yet registered, and only after this is it a launch that never started.
_ORPHAN_GRACE_S = 60.0


def _age_s(stamp: str | None) -> float:
    """Seconds since an ISO timestamp; 0 for one that cannot be read.

    Unreadable reads as *young*, so a record with a mangled timestamp is left
    alone rather than declared dead.
    """
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
        # who "this server" is, and what work it is currently running. Both are
        # per-process by nature: that is exactly what makes them able to answer
        # "was this run launched by something still alive?"
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
            # per run, not per cycle: one unreadable record or one unreachable
            # agent must not stop the other runs from being looked at
            try:
                out.append(self._probe(rec))
            except Exception as exc:
                out.append({"run": rec.name, "error": str(exc)})
        return [x for x in out if x]

    def _launch_is_owned(self, rec) -> bool:
        """Is the thread that put this run in a launch state still around?

        No: another server instance's id, or this instance's with no job for it
        that is still running. The job is found by subject rather than by a
        recorded id, so there is no window between writing the record and
        knowing what the job was called.
        """
        launcher = rec.record.get("launched_by")
        if self.instance_id is None or launcher != self.instance_id:
            return False
        if self.jobs is None:
            return True  # nothing here can say otherwise; leave it alone
        mine = [
            j for j in self.jobs.active()
            if j.subject.get("workflow") == rec.workflow and j.subject.get("run") == rec.name
        ]
        if mine:
            return True
        # this instance launched it and has no live job for it. Usually that is
        # a launch submitted a moment ago and not yet started; after a minute
        # it is a thread that died without patching the record.
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
                # a run that has been going for a while has no launch window
                # left to protect, and this is the only way `errored` is
                # reachable at all with a zero timeout
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
