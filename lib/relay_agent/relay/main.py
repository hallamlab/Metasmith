import os
from pathlib import Path
import time
from threading import Condition, Thread
from datetime import datetime as dt

from .ipc import CurrentTimeMillis, GenerateId, LiveShell, PipeServer, IpcRequest, IpcResponse

WS = Path(os.curdir).resolve()
GRACE = 3*1000

def Timestamp(timestamp: dt|None = None):
    ts = dt.now() if timestamp is None else timestamp
    FORMAT = '%Y-%m-%d %H:%M:%S'
    return f"{ts.strftime(FORMAT)}"

def log(x, timestamp=True):
    if timestamp:
        m = f"{Timestamp()}> {x}"
    else:
        m = x
    print(m)

class Endpoints:
    def _err(self, msg: str):
        return dict(error = msg)

    def echo(self, data: dict):
        return 200, data
    
    def bash(self, data: dict):
        K = "script"
        if K not in data: return 400, self._err(f"missing required field: [{K}]")
        with LiveShell() as shell:
            out, err = shell.Exec(data[K])
        return 200, dict(out=out, err=err)

def _handle_connection(channel: PipeServer, raw: str):
    log(f"request from: [{channel._id}]")
    req = IpcRequest.Parse(raw)
    def _err(msg: str):
        return dict(error = msg)
    def _handle() -> IpcResponse:
        if not req.IsValid(): return IpcResponse(
            status="400", data=_err(req.parse_error)
        )
        ep = Endpoints()
        ENDPOINTS = {k:v for k, v in Endpoints.__dict__.items() if k[0]!="_" and callable(v)}
        if req.endpoint not in ENDPOINTS: return IpcResponse(
            status="400", data=_err(f"invalid endpoint: [{req.endpoint}]")
        )
        status, data = ENDPOINTS[req.endpoint](ep, req.data)
        return IpcResponse(
            status=status, data=data
        )
    res = _handle()
    res.message_id = req.message_id
    channel.Send(res.Serialize())

def RunServer(workspace: Path):
    connections: dict[str, tuple[int, PipeServer]] = {}
    lock = Condition()
    running = True

    def new_connection(channel: PipeServer, raw: str):
        req = IpcRequest.Parse(raw)
        def _err(msg: str):
            return dict(error = msg)
        def _handle() -> IpcResponse:
            log(f"new connection")
            if not req.IsValid(): return IpcResponse(
                status="400", data=_err(req.parse_error)
            )
            if req.endpoint == "connect":
                new_channel = PipeServer(workspace, _handle_connection, id = GenerateId())
                log(f"new client assigned to: [{new_channel._id}]")
                connections[new_channel._id] = CurrentTimeMillis(), new_channel
                return IpcResponse(
                    status="200", data=dict(path=str(new_channel._client_path))
                )
            elif req.endpoint == "shutdown":
                with lock:
                    nonlocal running
                    running = False
                return IpcResponse(
                    status="200", data=dict(message="shutting down")
                )
            else:
                return IpcResponse(
                    status="400", data=_err(f"invalid endpoint: [{req.endpoint}]")
                )
        res = _handle()
        res.message_id = req.message_id
        channel.Send(res.Serialize())

    main_channel = PipeServer(workspace, new_connection, overwrite=True)
    def run():
        log("ready")
        while main_channel.IsOpen() and running:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                break

    def check_reap():
        now = CurrentTimeMillis()
        for id, (start, channel) in list(connections.items()):
            if now - start < GRACE: continue
            if channel.IsOpen() and channel._client_path.exists(): continue
            channel.Dispose()
            del connections[id]
            log(f"channel reaped: [{id}]")

    lock = Condition()
    def reaper_process():
        while True:
            with lock:
                if not running: break
                check_reap()
            time.sleep(1)
    reaper = Thread(target=reaper_process, args=[])

    try:
        reaper.start()
        run()
    finally:
        log("", timestamp=False)
        log("closing main channel")
        main_channel.Dispose()

        try:
            log("stopping reaper")
            with lock:
                running = False
            reaper.join()
        except KeyboardInterrupt:
            pass

        log("closing client channels")
        for id, (start, channel) in list(connections.items()):
            channel.Dispose()
