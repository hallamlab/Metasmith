from pathlib import Path

from .coms.ipc import IpcRequest, PipeClient

def run(workspace: Path):
    server_path = workspace/"main.in"
    with PipeClient(server_path) as p:
        res = p.Transact(IpcRequest(endpoint="connect"), timeout=1)
    if res.status != "200":
        print("error", res.data.get("error"))
        return

    channel_path = Path(res.data.get("path"))
    print(f"connecting as [{channel_path.stem}]")
    if channel_path is None:
        print("error", "no channel path")
        return
    
    with PipeClient(channel_path) as p:
        res = p.Transact(IpcRequest(endpoint="echo", data=dict(asdf=1)), timeout=2)
        print(f">>> echo")
        print(res)
        
        print(f">>> bash")
        script = f"""\
            echo "hello world"
            echo $$
            pwd -P
            ls -lh
            ls "non existent file"
            date
        """
        script = [line.strip() for line in script.split("\n") if line.strip()]
        script = "\n".join(f"  {line}" for line in script)
        print(f"script:")
        print(script)
        res = p.Transact(IpcRequest(endpoint="bash", data=dict(
            script=script,
        )), timeout=2)
        print(f"status: {res.status}")
        print(f"std_out")
        for line in res.data.get("out", []):
            print(f"  {line}")
        print(f"std_err")
        for line in res.data.get("err", []):
            print(f"  {line}")
        
    print("test complete")
