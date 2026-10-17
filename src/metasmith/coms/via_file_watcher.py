from pathlib import Path
from typing import Callable, Any, TypeVar, TypeAlias
from time import monotonic, sleep
from dataclasses import dataclass, field
import os, signal

from .ipc import CurrentTimeMillis, GenerateId, ResetGenerator
from .ipc import RemoveLeadingIndent, RemoveTrailingNewline
from ..constants import AgentPaths
from ..logging import Log
from .terminals import ShellResult

# These mirror GROUP_KILL_GRACE, PICKUP_DEADLINE and ORPHAN_GRACE in
# bash_relay/src/remote_shell.rs; the two clients must behave identically.
GROUP_KILL_GRACE_S = 5.0
PICKUP_DEADLINE_S = 15.0
ORPHAN_GRACE_S = 5.0

def _pid_is_relay(pid: int) -> bool:
    if pid <= 1: return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    try:
        argv0 = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")[0]
    except OSError:
        return True
    return b"msm_relay" in argv0

def _pid_namespace() -> str | None:
    try:
        return os.readlink("/proc/self/ns/pid")
    except OSError:
        return None

def _watcher_alive(watcher_path: Path) -> bool:
    try:
        fields = (watcher_path/"active").read_text().split()
    except OSError:
        return False
    # An `active` with no pid comes from a watcher older than the field, and a pid from another
    # namespace (a host watcher read from inside a container) names nothing here: neither can be checked.
    if not fields or not fields[0].isdigit(): return True
    if len(fields) > 1 and fields[1] != _pid_namespace(): return True
    return _pid_is_relay(int(fields[0]))

@dataclass
class Job:
    key: str
    out_log: Path
    err_log: Path
    done_path: Path
    out_i: int = 0
    err_i: int = 0
    submitted: float = field(default_factory=monotonic)
    stranded_since: float | None = None

    def Stranded(self, watcher_path: Path) -> str | None:
        if self.done_path.exists(): return None
        start = self.out_log.with_suffix(".start")
        watcher_alive = _watcher_alive(watcher_path)
        if start.exists():
            if not watcher_alive: return "no live watcher to pick it up"
            if monotonic()-self.submitted >= PICKUP_DEADLINE_S:
                return f"the watcher did not pick it up within {PICKUP_DEADLINE_S:.0f}s"
            return None
        if watcher_alive or self._group_alive(self._get_pgid(self.out_log.with_suffix(".pid"))):
            return None
        return "its watcher died and the job is gone"

    def _get_pgid(self, pidf: Path):
        # The .pid file holds a process GROUP id: the relay's launcher.sh runs
        # each job under `set -m`, so the recorded number leads a group holding
        # the tool the job started. Signalling the number alone stops the job's
        # shell and orphans that tool. 0 and negatives address our own group or
        # everything we may signal, so a truncated file yields -1 and no signal.
        try:
            with open(pidf) as f:
                pid = [l.replace("\n", "").strip() for l in f.readlines()]
            pgid = int(pid[0]) if len(pid)>0 else -1
            return pgid if pgid > 1 else -1
        except:
            pass
        return -1

    def _signal_group(self, pgid: int, sig: int) -> bool:
        if pgid <= 1: return False
        try:
            os.killpg(pgid, sig)
            return True
        except (ProcessLookupError, PermissionError):
            return False

    def SignalStop(self):
        pidf = self.out_log.with_suffix(".pid")
        if not pidf.exists(): return
        self._signal_group(self._get_pgid(pidf), signal.SIGINT)

    def _group_alive(self, pgid: int) -> bool:
        # PermissionError means the group exists and is someone else's -- alive,
        # and not ours to stop. Only ProcessLookupError means gone.
        if pgid <= 1: return False
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            pass
        return True

    def _kill_group_escalating(self, pgid: int, grace: float = GROUP_KILL_GRACE_S):
        if not self._signal_group(pgid, signal.SIGTERM): return
        deadline = monotonic() + grace
        while monotonic() < deadline:
            if not self._group_alive(pgid): return
            sleep(0.1)
        self._signal_group(pgid, signal.SIGKILL)

    def Dispose(self, timeout: float=3):
        pidf = self.out_log.with_suffix(".pid")
        donef = self.out_log.with_suffix(".done")
        for _ in range(int(timeout*10)):
            if donef.exists(): break
            sleep(0.1)
        
        code = 1
        if not donef.exists():
            self._kill_group_escalating(self._get_pgid(pidf))
        else:
            with open(donef) as f:
                code = f.readline().strip()
            try:
                code = int(code)
            except:
                code = 1

        to_del = [
            self.out_log,
            self.err_log,
        ]
        if donef.exists():
            to_del += [
                donef, pidf,
                self.out_log.with_suffix(".owner"),
                self.out_log.with_suffix(".orphan"),
                self.out_log.with_suffix(".run"),
            ]
        for p in to_del:
            p.unlink(missing_ok=True)
        return code

class RemoteShell:
    def __init__(self, watcher_path: Path, timeout: int=3, setup_commands: list[str]|None = None) -> None:
        self._out_callbacks: list[Callable[[str], None]] = []
        self._err_callbacks: list[Callable[[str], None]] = []
        self._watcher_path = watcher_path
        self._active_jobs: dict[str, Job] = {}
        self._setup_commands: list[str] = setup_commands if setup_commands else []
        self._timeout = timeout
        self._exit_codes: dict[str, int] = {}
        assert watcher_path.exists(), watcher_path

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.Dispose()

    def RegisterOnOut(self, callback: Callable[[str], None]):
        self._out_callbacks.append(callback)
    
    def RegisterOnErr(self, callback: Callable[[str], None]):
        self._err_callbacks.append(callback)

    def RemoveOnOut(self, callback: Callable[[str], None]):
        if callback in self._out_callbacks: self._out_callbacks.remove(callback)

    def RemoveOnErr(self, callback: Callable[[str], None]):
        if callback in self._err_callbacks: self._err_callbacks.remove(callback)

    def ExecAsync(self, cmd: str):
        script = RemoveLeadingIndent(cmd)
        k = GenerateId()
        script_path = self._watcher_path/f"{k}.compile"
        with open(script_path, "w") as f:
            for line in self._setup_commands:
                if not line.endswith("\n"):
                    line += "\n"
                f.write(line)
            f.write(script)
        self._active_jobs[k] = Job(
            key = k,
            out_log=script_path.with_suffix(".out"),
            err_log=script_path.with_suffix(".err"),
            done_path=script_path.with_suffix(".done"),
        )
        # Who asked for this job, and which run it belongs to. Written before
        # the rename so the watcher never sees a dispatchable job without them:
        # the owner pid is how it reclaims a job whose requester died, the run
        # token is how a cancel reaches one run's jobs in a workspace every run
        # on the host shares.
        script_path.with_suffix(".owner").write_text(f"{os.getpid()}\n")
        token = os.environ.get(AgentPaths.RUN_TOKEN_ENV, "").strip()
        if token:
            script_path.with_suffix(".run").write_text(f"{token}\n")
        script_path.rename(script_path.with_suffix(".start"))
        return k

    def AwaitDone(self, timeout: int|float|None=15, _key: str|None=None):
        start = CurrentTimeMillis()
        def is_done():
            if _key is None:
                return len(self._active_jobs)==0
            else:
                return _key not in self._active_jobs

        def check_log(log_path: Path, start: int, callbacks: list[Callable]):
            if not log_path.exists(): return start
            with open(log_path, "r", errors='replace') as f:
                f.seek(start)
                lines = f.readlines()
            i = start
            if len(lines)>0:
                for line in lines:
                    if not line.endswith("\n"): break
                    i += len(line)
                    for cb in callbacks:
                        cb(line[:-1])
            return i

        dt = 0.1
        MAX_DT = 0.5
        while not is_done():
            finished = []
            stranded = []
            for k, j in self._active_jobs.items():
                if j.done_path.exists():
                    finished.append(k)
                    continue
                why = j.Stranded(self._watcher_path)
                if why is None:
                    j.stranded_since = None
                    continue
                if j.stranded_since is None:
                    j.stranded_since = monotonic()
                if monotonic()-j.stranded_since < ORPHAN_GRACE_S:
                    continue
                # A start file that vanishes under us was just dispatched, not stranded.
                start_file = j.out_log.with_suffix(".start")
                try:
                    start_file.unlink(missing_ok=True)
                except OSError:
                    continue
                if not j.done_path.exists():
                    stranded.append((k, why))
            if len(finished)>0: sleep(dt)
            for k, j in self._active_jobs.items():
                j.out_i = check_log(j.out_log, j.out_i, self._out_callbacks)
                j.err_i = check_log(j.err_log, j.err_i, self._err_callbacks)
            for k in finished:
                self._exit_codes[k] = self._active_jobs[k].Dispose()
                del self._active_jobs[k]
            for k, why in stranded:
                Log.Error(f"relay job [{k}] will never finish: {why}")
                self._active_jobs[k].Dispose(timeout=0)
                self._exit_codes[k] = 1
                del self._active_jobs[k]
            now = CurrentTimeMillis()
            if timeout is not None and now-start>timeout*1000: break
            sleep(dt)
            dt = min(dt+0.1, MAX_DT)

    def Exec(self, cmd: str, timeout: int|float|None=None, history: bool=False) -> ShellResult:
        _out, _err = [], []
        def _on_out(msg):
            _out.append(msg)
        def _on_err(msg):
            _err.append(msg)
        if history:
            self.RegisterOnOut(_on_out)
            self.RegisterOnErr(_on_err)
        key = self.ExecAsync(cmd)
        self.AwaitDone(timeout=timeout, _key=key)
        if key in self._active_jobs:
            code = self._active_jobs.pop(key).Dispose()
        else:
            code = self._exit_codes.pop(key, 1)
        if history:
            self.RemoveOnOut(_on_out)
            self.RemoveOnErr(_on_err)
        return ShellResult(out=_out, err=_err, exit_code=code)

    def Dispose(self):
        for j in self._active_jobs.values():
            j.SignalStop()
        for j in self._active_jobs.values():
            j.Dispose(self._timeout)
