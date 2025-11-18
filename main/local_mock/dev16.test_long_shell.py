from pathlib import Path
from metasmith.coms.ipc import LiveShell

with LiveShell() as shell:
    shell.RegisterOnOut(lambda x: print(x))
    shell.Exec("""
        for i in {1..28800}; do
            echo $i
            sleep 1
        done
    """, timeout=None)

print("resumed")
