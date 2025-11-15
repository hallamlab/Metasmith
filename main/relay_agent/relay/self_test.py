from pathlib import Path

from .coms.via_ws import WsRequest, WsClient, RemoteShell

def run(workspace: Path):
    p = WsClient(workspace)
    try:
        res = p.Transact(WsRequest(endpoint="status"), timeout=2)
    finally:
        p.Dispose()
    print(f">>> status")
    print(res)

    print(f">>> bash")
    with RemoteShell(workspace) as shell:
        script = f"""\
            echo "hello world"
            echo $$
            pwd -P
            ls -lh
            ls "non existent file"
            date
        """
        shell.RegisterOnOut(lambda data: print(f"  {data}"))
        shell.RegisterOnErr(lambda data: print(f"E {data}"))
        shell.ExecAsync(script)
        shell.AwaitDone()
                
    print("test complete")
