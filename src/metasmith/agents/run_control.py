from __future__ import annotations

from pathlib import Path

from ..constants import AgentPaths
from ..coms.terminals import LiveShell, ShellResult
from ..models.remote import SshSource
from ..models.workflow import WorkflowTask
from .shell import AgentShell

# Time for a signal to take effect before a reap pass re-counts survivors.
REAP_SETTLE_S = 1.0


# Two scopes, because "is the workload stopped?" and "is anything left of this
# run?" are different questions. RUN covers everything start.sh rooted, the
# driver included; WORKLOAD covers only what the driver put to work -- nextflow
# and its group, tool containers, relay jobs. A cancel asks the WORKLOAD
# question, because the driver is meant to outlive it and finish snapshotting
# logs and promoting the cache.
SCOPE_RUN = "run"
SCOPE_WORKLOAD = "workload"

# What a run leaves behind on the agent host, in three overlapping views. The
# process group is the cheap one; the METASMITH_RUN environ scan is the backstop
# that catches anything which setsid()'d out of the group (it is inherited and
# cannot be shed); docker labels catch containers whose client is already gone.
# Emitted as `KIND|field|field` lines because this crosses a shell.
_SCAN_SCRIPT = r"""
WS="__WS__"
RELAY="__RELAY__"
SCOPE="__SCOPE__"
TOKEN=$(head -n1 "$WS/__TOKENF__" 2>/dev/null)
PGID=$(head -n1 "$WS/__PGIDF__" 2>/dev/null)
NXF=$(head -n1 "$WS/__PIDF__" 2>/dev/null)
echo "TOKEN|$TOKEN"
echo "PGID|$PGID"
echo "NXFPID|$NXF"
# Not `GROUPS`: that is a bash builtin array of the caller's gids, and an
# assignment to it is silently ignored.
PGIDS="$NXF"
[ "$SCOPE" = "run" ] && PGIDS="$PGID $NXF"
for g in $PGIDS; do
    ps -eo pid=,pgid=,args= 2>/dev/null | awk -v g="$g" '$2==g {pid=$1; $1=""; $2=""; sub(/^[ \t]+/,""); print "PROC|" pid "|" $0}'
done
if [ "$SCOPE" = "run" ] && [ -n "$TOKEN" ]; then
    for e in /proc/[0-9]*/environ; do
        [ -O "$e" ] || continue
        p=${e#/proc/}; p=${p%/environ}
        # This shell and its parent carry the token when a scan is issued from
        # inside the run it is scanning; counting them would make `empty` unreachable.
        { [ "$p" = "$$" ] || [ "$p" = "$PPID" ]; } && continue
        tr '\0' '\n' 2>/dev/null < "$e" | grep -qxF "METASMITH_RUN=$TOKEN" || continue
        echo "TOKEN_PROC|$p|$(tr '\0' ' ' 2>/dev/null < /proc/$p/cmdline)"
    done
fi
if [ -n "$TOKEN" ] && command -v docker >/dev/null 2>&1; then
    docker ps --filter "label=__LABEL__=$TOKEN" --format 'CONTAINER|{{.ID}}|{{.Image}}' 2>/dev/null
fi
if [ -x "$RELAY" ]; then
    "$RELAY" --io "$(dirname "$RELAY")/$(hostname)" status 2>/dev/null \
        | sed -n 's/^  - active jobs: /RELAY|/p'
fi
"""

# One kill pass, at one scope. Signal is a parameter so the caller can walk
# TERM -> KILL; the group kill is not atomic against a fan-out that is still
# spawning, which is why this sweeps rather than firing once.
_REAP_SCRIPT = r"""
WS="__WS__"
RELAY="__RELAY__"
SIG="__SIG__"
SCOPE="__SCOPE__"
TOKEN=$(head -n1 "$WS/__TOKENF__" 2>/dev/null)
PGID=$(head -n1 "$WS/__PGIDF__" 2>/dev/null)
NXF=$(head -n1 "$WS/__PIDF__" 2>/dev/null)
# Not `GROUPS`: that is a bash builtin array of the caller's gids, and an
# assignment to it is silently ignored.
PGIDS="$NXF"
[ "$SCOPE" = "run" ] && PGIDS="$PGID $NXF"
for g in $PGIDS; do
    kill -"$SIG" -"$g" 2>/dev/null
done
if [ "$SCOPE" = "run" ] && [ -n "$TOKEN" ]; then
    for e in /proc/[0-9]*/environ; do
        [ -O "$e" ] || continue
        p=${e#/proc/}; p=${p%/environ}
        { [ "$p" = "$$" ] || [ "$p" = "$PPID" ]; } && continue
        tr '\0' '\n' 2>/dev/null < "$e" | grep -qxF "METASMITH_RUN=$TOKEN" || continue
        kill -"$SIG" "$p" 2>/dev/null
    done
fi
if [ -n "$TOKEN" ]; then
    if command -v docker >/dev/null 2>&1; then
        for c in $(docker ps -q --filter "label=__LABEL__=$TOKEN" 2>/dev/null); do
            docker kill "$c" >/dev/null 2>&1
        done
    fi
    if [ -x "$RELAY" ]; then
        "$RELAY" --io "$(dirname "$RELAY")/$(hostname)" kill-run "$TOKEN" 2>/dev/null
    fi
fi
echo __MSM_REAPED__
"""


def RenderScan(script: str, *, workspace, relay, **extra: str) -> str:
    subs = {
        "__WS__": str(workspace),
        "__RELAY__": str(relay),
        "__TOKENF__": AgentPaths.RUN_TOKEN_FILE,
        "__PGIDF__": AgentPaths.RUN_PGID_FILE,
        "__PIDF__": AgentPaths.PID_LOCK_FILE,
        "__LABEL__": AgentPaths.RUN_LABEL,
        "__SCOPE__": SCOPE_RUN,
    } | extra
    for k, v in subs.items():
        script = script.replace(k, v)
    return script


class _RunControl:

    def _remote_oneshot(self, cmd: str, timeout: int = 30) -> "ShellResult":
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
        return AgentPaths.to_task(task_key, root=self.home.GetPath()).parent.parent

    def _resolve_run_dir(self, task_key: str, run: int | None) -> Path:
        workspace = self._task_workspace(task_key)
        internals = workspace / AgentPaths.INTERNALS
        if run is None:
            latest = internals / "logs.latest"
            res = self._remote_oneshot(f"readlink -f {latest}", timeout=15)
            for line in res.out:
                line = line.strip()
                if line.startswith(str(internals)) or "/logs." in line:
                    return Path(line)
            return latest
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
        import time
        task_key = task._key if isinstance(task, WorkflowTask) else str(task)
        run_dir = self._resolve_run_dir(task_key, run)
        agent_log = run_dir / "agent.log"
        workspace = self._task_workspace(task_key)
        pid_lock = workspace / AgentPaths.PID_LOCK_FILE
        run_pgid = workspace / AgentPaths.RUN_PGID_FILE

        start = time.monotonic()
        cur_poll = poll_s
        max_poll = 15.0
        last_mtime: float = 0.0

        while True:
            elapsed = time.monotonic() - start
            # Labelled, not positional: `grep -c` prints its 0 *and* exits 1,
            # so a bare `|| echo 0` fallback emits the count twice and shifts
            # every line after it.
            cmd = (
                f"if [ -e {agent_log} ]; then "
                f"echo \"MTIME $(stat -c %Y {agent_log})\"; "
                f"echo \"COUNT $(grep -c '{sentinel}' {agent_log} 2>/dev/null)\"; "
                f"else echo 'MTIME MISSING'; echo 'COUNT 0'; fi; "
                f"[ -e {pid_lock} ] && echo 'PID ALIVE' || echo 'PID GONE'; "
                # PID.lock goes when nextflow exits, but the driver runs on for
                # as long as promotion, results and log gathering take, and it
                # is what writes the sentinel. RUN.pgid holds the driver's own
                # process group; while anything is left in it the run is alive.
                f"if [ -s {run_pgid} ]; then "
                f"kill -0 -\"$(cat {run_pgid})\" 2>/dev/null "
                f"&& echo 'DRIVER ALIVE' || echo 'DRIVER GONE'; "
                f"else echo 'DRIVER UNKNOWN'; fi"
            )
            res = self._remote_oneshot(cmd, timeout=30)
            fields: dict[str, str] = {}
            for ln in res.out:
                label, _, value = ln.strip().partition(" ")
                if label and value:
                    fields.setdefault(label, value.strip())
            mtime_line = fields.get("MTIME", "")
            count_line = fields.get("COUNT", "0")
            pid_line = fields.get("PID", "GONE")
            driver_line = fields.get("DRIVER", "UNKNOWN")

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
            # A workspace with no RUN.pgid cannot answer, and answering "wait"
            # there would hang forever on a run that really did die.
            driver_alive = driver_line == "ALIVE"
            if (
                log_exists and pid_line == "GONE" and not driver_alive
                and count == 0 and elapsed >= grace_s
            ):
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

    def ReadWorkflowTrace(
        self,
        task: WorkflowTask | str,
        run: int | None = None,
    ) -> dict:
        task_key = task._key if isinstance(task, WorkflowTask) else str(task)
        run_dir = self._resolve_run_dir(task_key, run)
        trace = run_dir / AgentPaths.NXF_TRACE_FILE
        res = self._remote_oneshot(
            f"[ -e {trace} ] && cat {trace} || echo __MSM_MISSING__",
            timeout=30,
        )
        out = res.out
        exists = not (len(out) == 1 and out[0].strip() == "__MSM_MISSING__")
        return {
            "task_key": task_key,
            "run": run,
            "run_dir": str(run_dir),
            "file": str(trace),
            "exists": exists,
            "lines": out if exists else [],
        }

    def _cat_if_exists(self, path: Path) -> tuple[bool, list[str]]:
        res = self._remote_oneshot(
            f"[ -e {path} ] && cat {path} || echo __MSM_MISSING__", timeout=30,
        )
        out = res.out
        exists = not (len(out) == 1 and out[0].strip() == "__MSM_MISSING__")
        return exists, (out if exists else [])

    def ReadCacheHits(self, task: WorkflowTask | str, run: int | None = None) -> dict:
        # `runner.py` copies `_metasmith/trace.jsonl` into a finished run's own
        # `logs.*` dir once it's done, so that copy is the first thing to try --
        # it is this run's data, permanently. Only a run still in flight (or one
        # collected before that copy existed) has none there yet, and the
        # live workspace file is only that run's data while it's still the one
        # occupying the shared workspace -- compare `logs.*` basenames rather
        # than `_resolve_run_dir`'s full paths, since the `run=None` (latest)
        # resolution goes through `readlink -f` and fully resolves any symlink
        # in the agent home path, while the explicit-`run` resolution does not.
        task_key = task._key if isinstance(task, WorkflowTask) else str(task)
        run_dir = self._resolve_run_dir(task_key, run)
        per_run_path = run_dir / "trace.jsonl"
        exists, lines = self._cat_if_exists(per_run_path)
        if exists:
            return {"task_key": task_key, "run": run, "run_dir": str(run_dir), "file": str(per_run_path), "exists": True, "lines": lines}
        if run is not None and run_dir.name != self._resolve_run_dir(task_key, None).name:
            return {"task_key": task_key, "run": run, "run_dir": str(run_dir), "file": str(per_run_path), "exists": False, "lines": []}
        live_path = self._task_workspace(task_key) / "_metasmith" / "trace.jsonl"
        exists, lines = self._cat_if_exists(live_path)
        return {"task_key": task_key, "run": run, "run_dir": str(run_dir), "file": str(live_path), "exists": exists, "lines": lines}

    def _scan_cmd(self, script: str, task_key: str, **extra: str) -> str:
        return RenderScan(
            script,
            workspace=self._task_workspace(task_key),
            relay=AgentPaths.to_relay(self.home.GetPath()),
            **extra,
        )

    @staticmethod
    def _parse_scan(lines) -> dict:
        report: dict = {
            "token": None, "run_pgid": None, "nextflow_pid": None,
            "processes": [], "token_processes": [], "containers": [], "relay_jobs": [],
        }
        for raw in lines:
            parts = raw.strip().split("|")
            if len(parts) < 2: continue
            kind, rest = parts[0], parts[1:]
            match kind:
                case "TOKEN":     report["token"] = rest[0] or None
                case "PGID":      report["run_pgid"] = rest[0] or None
                case "NXFPID":    report["nextflow_pid"] = rest[0] or None
                case "PROC":      report["processes"].append({"pid": rest[0], "cmd": "|".join(rest[1:])})
                case "TOKEN_PROC":report["token_processes"].append({"pid": rest[0], "cmd": "|".join(rest[1:])})
                case "CONTAINER": report["containers"].append({"id": rest[0], "image": "|".join(rest[1:])})
                case "RELAY":
                    jobs = [j.strip() for j in "|".join(rest).split(",") if j.strip()]
                    report["relay_jobs"] += jobs
        by_pid = {p["pid"]: p for p in report["processes"] + report["token_processes"]}
        report["survivors"] = sorted(by_pid.values(), key=lambda p: int(p["pid"]))
        return report

    def InspectWorkflowProcesses(
        self, task: WorkflowTask | str, scope: str = SCOPE_RUN,
    ) -> dict:
        task_key = task._key if isinstance(task, WorkflowTask) else str(task)
        res = self._remote_oneshot(
            self._scan_cmd(_SCAN_SCRIPT, task_key, __SCOPE__=scope), timeout=60,
        )
        report = self._parse_scan(res.out)
        report["task_key"] = task_key
        report["scope"] = scope
        # A relay job list is per host, not per run: only entries tagged with
        # this run's token belong to it.
        token = report.get("token")
        report["relay_jobs"] = [
            j for j in report["relay_jobs"] if token and j.endswith(f"run={token}")
        ]
        report["empty"] = not (
            report["survivors"] or report["containers"] or report["relay_jobs"]
        )
        return report

    @staticmethod
    def _found(report: dict) -> list:
        return report["survivors"] + report["containers"] + report["relay_jobs"]

    def ReapWorkflow(
        self, task: WorkflowTask | str, passes: int = 3, scope: str = SCOPE_RUN,
    ) -> dict:
        import time
        task_key = task._key if isinstance(task, WorkflowTask) else str(task)
        before = self.InspectWorkflowProcesses(task_key, scope)
        rungs: list[str] = []
        report = before
        for i in range(max(1, passes)):
            sig = "TERM" if i == 0 else "KILL"
            self._remote_oneshot(
                self._scan_cmd(_REAP_SCRIPT, task_key, __SIG__=sig, __SCOPE__=scope),
                timeout=60,
            )
            rungs.append(sig)
            time.sleep(REAP_SETTLE_S)
            report = self.InspectWorkflowProcesses(task_key, scope)
            if report["empty"]: break
        return {
            "task_key": task_key,
            "scope": scope,
            "rungs": rungs,
            "stopped": self._found(before),
            "survived": self._found(report),
        }

    def CancelWorkflow(self, task: WorkflowTask | str, timeout_s: float = 30.0) -> dict:
        # A ladder: each rung is the fallback for the one above.
        #   1. remove PID.lock -- the driver's supervisor TERMs nextflow's own
        #      process group, and nextflow's shutdown hook is what scancels grid
        #      jobs, so this rung must be given a real chance before escalating.
        #   2. KILL that group directly, for a wedged JVM.
        #   3. reap, for whatever left the group or was never in it.
        import time
        task_key = task._key if isinstance(task, WorkflowTask) else str(task)
        workspace = self._task_workspace(task_key)
        pid_lock = workspace / AgentPaths.PID_LOCK_FILE

        def _finish(rung: str, pid, detail: str) -> dict:
            # Workload scope: the driver is still winding down -- snapshotting
            # logs, promoting the cache -- and killing it here would throw away
            # exactly what the two-group design exists to preserve.
            report = self.InspectWorkflowProcesses(task_key, SCOPE_WORKLOAD)
            stopped, survived = self._found(report), []
            if stopped:
                reaped = self.ReapWorkflow(task_key, scope=SCOPE_WORKLOAD)
                rung, survived = "reap", reaped["survived"]
                stopped = reaped["stopped"]
            return {
                "task_key": task_key,
                "method": rung,
                "rung": rung,
                "killed_pid": pid,
                "stopped": stopped,
                "survived": survived,
                "status": "cancelled" if not survived else "cancelling",
                "detail": detail,
            }

        probe = self._remote_oneshot(f"[ -e {pid_lock} ] && cat {pid_lock} || echo __MSM_NONE__", timeout=15)
        first = probe.out[0].strip() if probe.out else "__MSM_NONE__"
        if first == "__MSM_NONE__":
            report = self.InspectWorkflowProcesses(task_key, SCOPE_WORKLOAD)
            survived = self._found(report)
            return {
                "task_key": task_key,
                "method": "noop",
                "rung": "noop",
                "killed_pid": None,
                "stopped": [],
                "survived": survived,
                "status": "not_running" if not survived else "cancelling",
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
                return _finish("pidfile", pid, "PID.lock removed; driver exited")
            time.sleep(1.0)

        if pid:
            self._remote_oneshot(f"kill -KILL -{pid} 2>/dev/null || true", timeout=15)
        return _finish(
            "group_kill", pid,
            f"nextflow did not exit within {timeout_s:g}s of PID.lock removal; "
            "its process group was killed",
        )

    def ListWorkflowRuns(self, task: WorkflowTask | str) -> list[dict]:
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
