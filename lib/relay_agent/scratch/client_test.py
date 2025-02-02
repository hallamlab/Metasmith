#!/usr/bin/env python3

import os, sys
from pathlib import Path
import time

from relay_agent.relay.ipc import PipeClient, IpcRequest, IpcResponse

def run():
    server_path = Path("./cache/main.in")
    with PipeClient(server_path) as p:
        res = p.Transact(IpcRequest(endpoint="connect"), timeout=1)
    if res.status != "200":
        print("error", res.data.get("error"))
        return

    channel_path = Path(res.data.get("path"))
    print(f"connecting to [{channel_path.stem}]")
    if channel_path is None:
        print("error", "no channel path")
        return
    
    with PipeClient(channel_path) as p:
        # p._reader.RegisterCallback(lambda x: print(x.decode()))
        res = p.Transact(IpcRequest(endpoint="echo", data=dict(asdf=1)), timeout=2)
        print(f"echo: {res}")
        
        res = p.Transact(IpcRequest(endpoint="bash", data=dict(
            script=f"""\
            echo "hello world"
            echo $$
            pwd -P
            date
            ls -lh
            """
        )), timeout=2)
        print(f">{res.status}")
        print(f"out")
        for line in res.data.get("out", []):
            print(f"  {line}")
        print(f"err")
        for line in res.data.get("err", []):
            print(f"  {line}")
        
    print(f"shutdown")
    with PipeClient(server_path) as p:
        res = p.Transact(IpcRequest(endpoint="shutdown"), timeout=1)
    print(f"{res.status} {res.data}")
run()
