from pathlib import Path
import os

from .logging import Log

def RunWatcher(workspace: Path):
    Log.AddLogFile(workspace/"main.log")
    CWD = Path(os.getcwd())
    Log.Info(f"starting watcher relay at [{workspace}] with cwd [{CWD}]")
    Log.SetStdout(on=False)
    launcher = workspace/"launcher.sh"
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
            "mv $SCRIPT $DONEF",
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

    while True:
        _break = False
        for f in workspace.iterdir():
            if f.name == "exit":
                Log.Info(f"stopping")
                _break=True; break
            if f.name.endswith(".start"):
                dispatch(f)
        if _break: break
    Log.Info(f"stopped")
