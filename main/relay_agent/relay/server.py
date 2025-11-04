import asyncio
import os, sys
from dataclasses import dataclass, field, fields
from pathlib import Path
from enum import Enum
from typing import Callable
from contextlib import asynccontextmanager
import socketio
from fastapi import FastAPI # just for lifespan manager
import uvicorn
import signal

from .coms.via_ws import Sender, Reciever, CLIENT_TO_SERVER, SERVER_TO_CLIENT, WsClient, WsRequest, WsResponse, LockFile, P
from .logging import Log, _formatter

class SERVER_HEALTH(Enum):
    DEAD = 0
    ALIVE = 1
    STALE = 2

@dataclass
class ServerStatus:
    health: SERVER_HEALTH
    pid: int = -1

def RunServer(workspace: Path):    
    lockf: LockFile|None = None
    address, port = None, None
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # on start
        yield
        # on exit
        if lockf is not None: lockf.Dispose()

    # Create Async Socket.IO server
    sio = socketio.AsyncServer(async_mode='asgi')
    api_app = FastAPI(lifespan=lifespan)
    app = socketio.ASGIApp(sio, api_app)

    async def on_send(channel: str, raw: dict):
        # print(f"send [{channel}] [{raw}]")
        await sio.emit(channel, raw)
    sender = Sender(on_send, SERVER_TO_CLIENT, CLIENT_TO_SERVER, lambda: True)
    reciever = Reciever(sender)
    async def inbound(sid, raw: dict):
        # print(sid, raw)
        await reciever.NewRequest(raw)
    sio.on(CLIENT_TO_SERVER, inbound)
    async def ack(sid, raw: dict):
        await sender.Acknowledge(raw)
    sio.on(SERVER_TO_CLIENT, ack)

    def endpoint(endpoint: str|None=None):
        def _register_function(handler: Callable[[WsRequest], P[WsResponse|None]]):
            ep = handler.__name__ if endpoint is None else endpoint
            reciever.AddHandler(ep, handler)
        return _register_function

    # =============================================
    # endpoints

    @endpoint()
    async def status(req: WsRequest):
        return WsResponse(
            200, 
            data=dict(
                address=address,
                port=port,
                pid=os.getpid(),
            )
        )
    
    @endpoint()
    async def shutdown(req: WsRequest):
        os.kill(os.getpid(), signal.SIGINT)

    @endpoint()
    async def ping(req: WsRequest):
        k = req.data.get("client")
        if k is None: return
        # refresh terminal time for culling in shell ep

    test_last_key = None
    @endpoint()
    async def shell(req: WsRequest):
        d = req.data
        cmd = d.get("cmd")
        k = d.get("client")
        nonlocal test_last_key
        test_last_key = k
        return WsResponse(204)
        # check for stale terminals
        # remove registered callbacks

    @endpoint()
    async def register_on_out(req: WsRequest):
        d = req.data
        cmd = d.get("cmd")
        k = d.get("client")
    
    @endpoint()
    async def register_on_err(req: WsRequest):
        d = req.data
        cmd = d.get("cmd")
        k = d.get("client")

    # =============================================

    async def main():
        LOGGING_CONFIG = {
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {
                "default": {
                    "()": "uvicorn.logging.DefaultFormatter",
                    "fmt": _formatter._formatter,
                    "use_colors": False,
                },
            },
            "handlers": {
                "default": {
                    "formatter": "default",
                    "class": "logging.handlers.RotatingFileHandler",
                    "filename": str(workspace/"main.log"),
                    "backupCount": 5,
                    "maxBytes": 104857600, # 100MB
                },
            },
            "loggers": {
                "uvicorn": {"handlers": ["default"], "level": "INFO"},
            },
            "root": {
                "handlers": [],
                "level": "INFO"
            }
        }
        config = uvicorn.Config(app, port=0, access_log=False, lifespan="on", log_config=LOGGING_CONFIG)
        server = uvicorn.Server(config)
        asyncio.create_task(server.serve())

        # Wait until the server has started and bound to a port
        while not server.started:
            await asyncio.sleep(0.1)

        # Iterate through the server's sockets to find the bound port
        nonlocal lockf, address, port
        found = False
        for server_instance in server.servers:
            for socket in server_instance.sockets:
                _address, _port = socket.getsockname()
                address = _address
                port = _port
                lockf = LockFile(workspace, _port)
                found = True
        assert found, "failed to retrieve assigned port"

        i = 0
        while True:
            if lockf is not None:
                if not lockf._file.exists(): 
                    os.kill(os.getpid(), signal.SIGINT)
                    lockf = None
            # ----
            # test
            # private channel per client
            i += 1
            # success = await sender.RobustSend(WsRequest(
            #     endpoint="stream",
            #     data=dict(channel="out", buf=f"{i}")
            # ), channel=test_last_key)
            # print("sent", success, test_last_key)
            # ----

            await asyncio.sleep(0.1) # keep server alive

    async def safe():
        try:
            await main()
        except (KeyboardInterrupt, asyncio.exceptions.CancelledError):
            pass
    
    try:
        asyncio.run(safe())
    except KeyboardInterrupt:
        pass

def _get_connection(workspace: Path, timeout=5):
    return WsClient(workspace, timeout=timeout)

def CheckStatus(workspace: Path):
    try:
        client = _get_connection(workspace)
    except (ConnectionError, TimeoutError):
        candidate_lock_files = LockFile._get_candidates(workspace)
        return ServerStatus(
            health=SERVER_HEALTH.DEAD if len(candidate_lock_files)==0 else SERVER_HEALTH.STALE
        )
    res = client.Transact(
        WsRequest(
            endpoint="status",
        )
    )

    return ServerStatus(
        health=SERVER_HEALTH.ALIVE,
        pid = res.data.get("pid", -1)
    )

def StopServer(workspace: Path):
    try:
        client = _get_connection(workspace, timeout=1)
        client.Transact(
            WsRequest(
                endpoint="shutdown",
            ),
            timeout=0
        )
    except (ConnectionError, TimeoutError):
        return

def Bounce(workspace: Path):
    try:
        client = _get_connection(workspace, timeout=1)
        client.Transact(
            WsRequest(
                endpoint="shutdown",
            ),
            timeout=0
        )
        # client.
    except (ConnectionError, TimeoutError):
        return
    pass
