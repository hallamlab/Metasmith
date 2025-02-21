from pathlib import Path

from .coms.ipc import IpcRequest, PipeClient, RemoteShell

def run(workspace: Path):
    server_path = workspace/"main.in"
    with PipeClient(server_path) as p:
        res = p.Transact(IpcRequest(endpoint="connect"), timeout=1)
    if res.status != 200:
        print("error", res.data.get("error"))
        return
    
    channel = Path(res.data.get("path"))
    if channel is None:
        print("error", "no channel path")
        return
    channel_path = workspace/channel
    print(f"connecting as [{channel.stem}]")
    
    with PipeClient(channel_path) as p:
        res = p.Transact(IpcRequest(endpoint="echo", data=dict(asdf=1)), timeout=2)
        print(f">>> echo")
        print(res)

    print(f">>> bash")
    with RemoteShell(server_path) as shell:
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
