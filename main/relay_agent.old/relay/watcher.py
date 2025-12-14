from pathlib import Path
import os, signal
import time
import json
from dataclasses import dataclass, field

from .logging import Log
from .coms.ipc import CurrentTimeMillis, GenerateId

@dataclass
class Status:
    alive: bool
    pid: int = -1
    jobs: list[str] = field(default_factory=list)

    @classmethod
    def Load(cls, status_file: Path):
        if not status_file.exists(): return None
        try:
            with open(status_file) as j:
                d = json.load(j)
            return Status(**d)
        except:
            return None

    def Save(self, status_file: Path):
        with open(status_file, "w") as j:
            json.dump({k:v for k, v in self.__dict__.items() if not k.startswith("_") and not callable(v)}, j, indent=4)

def _try_kill_jobs(workspace: Path):
    for f in workspace.iterdir():
        if not f.name.endswith(".pid"): continue
        try:
            with open(f) as fh:
                pid = fh.readline().strip()
                pid = int(pid)
            os.kill(pid, signal.SIGTERM)
        except:
            pass

def Wipe(workspace: Path):
    _try_kill_jobs(workspace)
    safe_wait = False
    for f in workspace.iterdir():
        if f.name == "main.log": continue
        if f.name == "active":
            safe_wait = True
        f.unlink()
    return safe_wait

def RunWatcher(workspace: Path):
    def run_watcher():# -> Any:
        Log.AddLogFile(workspace/"main.log")
        CWD = Path(os.getcwd())
        Log.Info(f"starting watcher relay at [{workspace}] with cwd [{CWD}]")
        Log.SetStdout(on=False)

        if Wipe(workspace): time.sleep(1)

        launcher = workspace/"launcher.sh"
        active = workspace/"active"
        active.touch(0o644)
        with open(launcher, "w") as f:
            script = [
                "#!/bin/bash",
                "SCRIPT=$1",
                "PIDF=$2",
                "DONEF=$3",
                f"cd {CWD}",
                f"bash {workspace}/$SCRIPT &",
                f"cd {workspace}",
                "PID=$!",
                "echo $PID > $PIDF",
                "wait $PID",
                "STATUS=$?",
                "rm $PIDF",
                "rm $SCRIPT",
                "echo $STATUS > $DONEF",
            ]
            f.write("\n".join(script))

        def dispatch(f: Path):
            Log.Info(">"*25)
            with open(f) as h:
                for l in h:
                    if l[-1] == "\n": l = l[:-1]
                    Log.Info(l)
            Log.Info("<"*25)
            live = f"{f.stem}.running"
            os.system(f"""\
                cd {workspace}
                mv {f.name} {live}
                nohup bash {launcher} {live} {f.stem}.pid {f.stem}.done >{f.stem}.out 2>{f.stem}.err &
            """)

        try:
            while True:
                active_jobs: list[str] = []
                status_checks: list[Path] = []
                found = False
                for f in workspace.iterdir():
                    if f.name == active.name:
                        found = True
                    if f.name.endswith(".start"):
                        dispatch(f)
                    if f.name.endswith(".pid"):
                        active_jobs.append(f.stem)
                    if f.name.endswith(".check"):
                        status_checks.append(f)
                if not found:
                    Log.Info(f"stopping")
                    break
                for f in status_checks:
                    Status(jobs=active_jobs, alive=True, pid=os.getpid()).Save(f.with_suffix(".status"))
                time.sleep(0.1)
        finally:
            if active.exists(): active.unlink()
        
    while True:
        try:
            workspace.mkdir(parents=True, exist_ok=True)
            run_watcher()
            break
        except KeyboardInterrupt:
            break
        except Exception as e:
            Log.Error(f"error [{e}], restarting")
        finally:
            Log.Info(f"stopped watcher relay")
            _try_kill_jobs(workspace)

def StopWatcher(workspace: Path):
    active = (workspace/"active")
    if active.exists(): active.unlink()
    Log.Info(f"signalled watcher to stop")

def CheckStatus(workspace: Path, timeout=2):
    active = workspace/"active"
    if not active.exists(): return Status(alive=False)
    sig = workspace/f"{GenerateId()}.check"
    result = sig.with_suffix(".status")
    sig.touch()
    start = CurrentTimeMillis()
    try:
        while CurrentTimeMillis()-start <= timeout*1000:
            time.sleep(0.02)
            if not result.exists(): continue
            status = Status.Load(result)
            if status is None: continue
            return status
        return Status(alive=False)
    finally:
        if sig.exists(): sig.unlink()
        if result.exists(): result.unlink()
