"""Watching, tailing, and cancelling a run that is already detached.

The block the monolith had already fenced off with its own banner. These are the
only agent methods that deliberately avoid holding an `AgentShell` open: a wait
that lasts hours must not pin a connection for the duration, so each one is a
one-shot exec that reconnects, asks, and drops.

A mixin for the same reason as `workflow_ops` -- they read the agent's home and
shell -- and separate from it because the lifecycle is different: these run
against a workflow the client already launched and may have stopped watching.
"""

from __future__ import annotations

from pathlib import Path

from ..constants import AgentPaths
from ..coms.terminals import LiveShell, ShellResult
from ..models.remote import SshSource
from ..models.workflow import WorkflowTask
from .shell import AgentShell


class _RunControl:
    # -- lightweight detached-run helpers -----------------------------------

    def _remote_oneshot(self, cmd: str, timeout: int = 30) -> "ShellResult":
        """One-shot exec on the agent host without holding AgentShell open.

        Local home → transient LiveShell with quiet=True.
        SSH home   → direct subprocess `ssh host '<cmd>'`.
        Returns ShellResult with .out and .err populated.
        """
        if self._is_ssh():
            import subprocess
            ssh_src = SshSource.Parse(self.home.address)
            try:
                proc = subprocess.run(
                    ["ssh", "-o", f"ConnectTimeout={min(timeout, 30)}", "-o", "BatchMode=yes", ssh_src.host, cmd],
                    capture_output=True, text=True, timeout=timeout,
                )
                return ShellResult(
                    out=[ln for ln in proc.stdout.splitlines()],
                    err=[ln for ln in proc.stderr.splitlines()],
                )
            except subprocess.TimeoutExpired as exc:
                return ShellResult(out=[], err=[f"ssh timeout after {timeout}s: {exc}"])
        else:
            with LiveShell() as sh:
                res = sh.Exec(cmd, history=True, quiet=True, timeout=timeout)
            return res

    def _task_workspace(self, task_key: str) -> Path:
        """workspace = runs/<key>/ (parent of _metasmith)"""
        return AgentPaths.to_task(task_key, root=self.home.GetPath()).parent.parent

    def _resolve_run_dir(self, task_key: str, run: int | None) -> Path:
        """Resolve runs/<key>/_metasmith/logs.<ts>/ once at call time."""
        workspace = self._task_workspace(task_key)
        internals = workspace / AgentPaths.INTERNALS
        if run is None:
            latest = internals / "logs.latest"
            res = self._remote_oneshot(f"readlink -f {latest}", timeout=15)
            for line in res.out:
                line = line.strip()
                if line.startswith(str(internals)) or "/logs." in line:
                    return Path(line)
            return latest  # fall back to symlink path
        else:
            res = self._remote_oneshot(
                f"ls -1d {internals}/logs.* 2>/dev/null | grep -v latest | sort",
                timeout=15,
            )
            dirs = [Path(ln.strip()) for ln in res.out if ln.strip()]
            assert 1 <= run <= len(dirs), f"run {run} out of range (1..{len(dirs)})"
            return dirs[run - 1]

    def WaitForWorkflow(
        self,
        task: WorkflowTask | str,
        timeout_s: float = 3600.0,
        poll_s: float = 5.0,
        run: int | None = None,
        sentinel: str = "run completed at",
        since_mtime: float | None = None,
        grace_s: float = 5.0,
    ) -> dict:
        """Block until `sentinel` appears in agent.log of the selected run.

        Returns: {task_key, status, run_dir, elapsed_s, last_log_mtime, tail}
        status ∈ {"completed", "timeout", "missing", "errored"}.

        `grace_s` is how long "the driver is gone and nothing said it finished"
        has to hold before it is believed. It exists for the caller that waits
        from the moment of launch: a nextflow that has not written its PID lock
        yet looks exactly like one that died. A caller *polling* an already-old
        run has no such window to protect and should pass 0 -- otherwise, with
        `timeout_s` also 0, the timeout branch fires first and `errored` is
        unreachable, which is how a crashed run stayed `running` forever.
        """
        import time
        task_key = task._key if isinstance(task, WorkflowTask) else str(task)
        run_dir = self._resolve_run_dir(task_key, run)
        agent_log = run_dir / "agent.log"
        workspace = self._task_workspace(task_key)
        pid_lock = workspace / "PID.lock"

        start = time.monotonic()
        cur_poll = poll_s
        max_poll = 15.0
        last_mtime: float = 0.0

        while True:
            elapsed = time.monotonic() - start
            cmd = (
                f"if [ -e {agent_log} ]; then "
                f"stat -c %Y {agent_log}; "
                f"grep -c '{sentinel}' {agent_log} 2>/dev/null || echo 0; "
                f"else echo MISSING; echo 0; fi; "
                f"[ -e {pid_lock} ] && echo PID_ALIVE || echo PID_GONE"
            )
            res = self._remote_oneshot(cmd, timeout=30)
            lines = [ln.strip() for ln in res.out if ln.strip()]
            mtime_line = lines[0] if lines else ""
            count_line = lines[1] if len(lines) > 1 else "0"
            pid_line = lines[-1] if lines else "PID_GONE"

            log_exists = mtime_line != "MISSING"
            try:
                last_mtime = float(mtime_line) if log_exists else 0.0
            except ValueError:
                last_mtime = 0.0
            try:
                count = int(count_line)
            except ValueError:
                count = 0

            fresh = (since_mtime is None) or (last_mtime > since_mtime)
            if log_exists and count > 0 and fresh:
                tail = self.TailWorkflowLog(task_key, source="agent", lines=20, run=run)
                return {
                    "task_key": task_key,
                    "status": "completed",
                    "run_dir": str(run_dir),
                    "elapsed_s": elapsed,
                    "last_log_mtime": last_mtime,
                    "tail": tail.get("lines", []),
                }
            if log_exists and pid_line == "PID_GONE" and count == 0 and elapsed >= grace_s:
                tail = self.TailWorkflowLog(task_key, source="agent", lines=20, run=run)
                return {
                    "task_key": task_key,
                    "status": "errored",
                    "run_dir": str(run_dir),
                    "elapsed_s": elapsed,
                    "last_log_mtime": last_mtime,
                    "tail": tail.get("lines", []),
                }
            if elapsed > timeout_s:
                tail_lines: list[str] = []
                if log_exists:
                    tail = self.TailWorkflowLog(task_key, source="agent", lines=20, run=run)
                    tail_lines = tail.get("lines", [])
                return {
                    "task_key": task_key,
                    "status": "timeout" if log_exists else "missing",
                    "run_dir": str(run_dir),
                    "elapsed_s": elapsed,
                    "last_log_mtime": last_mtime,
                    "tail": tail_lines,
                }
            time.sleep(cur_poll)
            cur_poll = min(max_poll, cur_poll * 1.3)

    def TailWorkflowLog(
        self,
        task: WorkflowTask | str,
        source: str = "agent",
        lines: int = 50,
        run: int | None = None,
    ) -> dict:
        """Read the last N lines from agent.log or main.log of the selected run."""
        assert source in ("agent", "main"), f"source must be 'agent' or 'main', got [{source}]"
        task_key = task._key if isinstance(task, WorkflowTask) else str(task)
        run_dir = self._resolve_run_dir(task_key, run)
        if source == "agent":
            log_path = run_dir / "agent.log"
        else:
            log_path = run_dir / AgentPaths.MAIN_LOG_FILE
        res = self._remote_oneshot(
            f"[ -e {log_path} ] && tail -n {lines} {log_path} || echo __MSM_MISSING__",
            timeout=30,
        )
        out = res.out
        exists = not (len(out) == 1 and out[0].strip() == "__MSM_MISSING__")
        return {
            "task_key": task_key,
            "source": source,
            "run": run,
            "run_dir": str(run_dir),
            "file": str(log_path),
            "exists": exists,
            "lines": out if exists else [],
        }

    def CancelWorkflow(self, task: WorkflowTask | str, timeout_s: float = 30.0) -> dict:
        """Best-effort cancel an active run by removing workspace/PID.lock.

        The launcher (see RunWorkflow) watches PID.lock and gracefully kills
        nextflow when it disappears. Falls back to pkill if the lock is gone
        but the driver is still alive.
        """
        import time
        task_key = task._key if isinstance(task, WorkflowTask) else str(task)
        workspace = self._task_workspace(task_key)
        pid_lock = workspace / "PID.lock"

        probe = self._remote_oneshot(f"[ -e {pid_lock} ] && cat {pid_lock} || echo __MSM_NONE__", timeout=15)
        first = probe.out[0].strip() if probe.out else "__MSM_NONE__"
        if first == "__MSM_NONE__":
            return {
                "task_key": task_key,
                "method": "noop",
                "killed_pid": None,
                "status": "not_running",
                "detail": "PID.lock not present",
            }
        try:
            pid = int(first)
        except ValueError:
            pid = None

        self._remote_oneshot(f"rm -f {pid_lock}", timeout=15)

        start = time.monotonic()
        while time.monotonic() - start < timeout_s:
            alive = self._remote_oneshot(
                f"[ -e /proc/{pid} ] && echo ALIVE || echo GONE" if pid else "echo GONE",
                timeout=15,
            )
            if alive.out and alive.out[0].strip() == "GONE":
                return {
                    "task_key": task_key,
                    "method": "pidfile",
                    "killed_pid": pid,
                    "status": "cancelled",
                    "detail": "PID.lock removed; driver exited",
                }
            time.sleep(1.0)

        # fallback
        self._remote_oneshot(f"pkill -f 'run_workflow.*key={task_key}' || true", timeout=15)
        return {
            "task_key": task_key,
            "method": "pkill_fallback",
            "killed_pid": pid,
            "status": "cancelled",
            "detail": "PID.lock removal did not stop driver within timeout; pkill fallback issued",
        }

    def ListWorkflowRuns(self, task: WorkflowTask | str) -> list[dict]:
        """List all runs (logs.<ts> directories) for a task."""
        task_key = task._key if isinstance(task, WorkflowTask) else str(task)
        internals = self._task_workspace(task_key) / AgentPaths.INTERNALS
        res = self._remote_oneshot(
            f"ls -1d {internals}/logs.* 2>/dev/null | grep -v latest | sort",
            timeout=15,
        )
        runs: list[dict] = []
        for i, line in enumerate(ln.strip() for ln in res.out if ln.strip()):
            ts = line.rsplit(".", 1)[-1] if "." in line else ""
            runs.append({"index": i + 1, "path": line, "timestamp": ts})
        return runs
