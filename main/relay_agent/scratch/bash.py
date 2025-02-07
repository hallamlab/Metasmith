#!/usr/bin/env python3

from multiprocessing import Pipe
import os, sys
from pathlib import Path
import time

from relay_agent.relay.ipc import PipeClient, IpcRequest, IpcResponse, PipeServer

def run():
    ws = Path("./cache")
    server_path = ws/"main.in"
    with PipeClient(server_path) as p:
        res = p.Transact(IpcRequest(endpoint="connect"), timeout=1)
    if res.status != 200:
        print("error", res.data.get("error"))
        return

    channel_path = Path(res.data.get("path"))
    print(f"connecting as [{channel_path.stem}]")
    if channel_path is None:
        print("error", "no channel path")
        return
    channel_path = ws/channel_path

    def _make_callback(pre: str):
        def _handler(channel: PipeServer, raw: str):
            print(pre, raw)
        return _handler
    
    k = channel_path.stem
    with \
        PipeServer(ws, _make_callback("O> "), id=k+".bash_out") as live_out, \
        PipeServer(ws, _make_callback("E> "), id=k+".bash_err") as live_err, \
        PipeClient(channel_path) as p:

        for stream, path in [
            ("out", live_out._server_path),
            ("err", live_err._server_path),
        ]:
            res = p.Transact(IpcRequest(endpoint="register_bash_listener", data=dict(
                stream=stream,
                channel=str(path.resolve()),
            )))
            assert res.status == 200
            print(res)

        res = p.Transact(
            IpcRequest(
                endpoint="bash",
                data=dict(
                    script=f"""\
                        echo "start"
                        for i in $(seq 1 10000000); do
                            x=$i
                        done
                        echo "end"
                    """,
                ),
            ),
        )
        
        print(res)
        time.sleep(3)

run()
