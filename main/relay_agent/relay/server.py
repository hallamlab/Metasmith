import asyncio
import os, sys
from dataclasses import dataclass, field, fields
from pathlib import Path
from enum import Enum
from typing import Callable
from contextlib import asynccontextmanager
import socketio
from socketio.exceptions import BadNamespaceError
from fastapi import FastAPI # just for lifespan manager
import uvicorn
import signal

from .coms.via_ws import Sender, Reciever, CLIENT_TO_SERVER, SERVER_TO_CLIENT, WsClient, WsRequest, WsResponse, LockFile, P
from .coms.ipc import CurrentTimeMillis
from .coms.terminals import LiveShell
from .logging import Log, _formatter

class SERVER_HEALTH(Enum):
    DEAD = 0
    ALIVE = 1
    STALE = 2

@dataclass
class ServerStatus:
    health: SERVER_HEALTH
    pid: int = -1
    n_clients: int = 0

@dataclass
class Client:
    key: str
    # shell: LiveShell = field(default_factory=lambda: LiveShell(sleep=lambda t: asyncio.sleep(t)))
    shell: LiveShell = field(default_factory=LiveShell)
    last_active: int = field(default_factory=CurrentTimeMillis)
    last_flush: int = field(default_factory=lambda: 0)
    out: list[str] = field(default_factory=list)
    err: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.shell.RegisterOnOut(lambda x: self.out.append(x))
        self.shell.RegisterOnErr(lambda x: self.err.append(x))

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
        try:
            await sio.emit(channel, raw)
        except BadNamespaceError:
            pass # due to shutting down
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

    clients: dict[str, Client] = {}
    @endpoint()
    async def status(req: WsRequest):
        now = CurrentTimeMillis()
        return WsResponse(
            200, 
            data=dict(
                address=address,
                port=port,
                pid=os.getpid(),
                clients=[
                    dict(
                        key=c.key,
                        last_active=now-c.last_active,
                        last_flush=now-c.last_flush,
                        out_buf_len = len(c.out),
                        err_buf_len = len(c.err),
                    )
                    for c in clients.values()
                ],
            )
        )
    
    @endpoint()
    async def shutdown(req: WsRequest):
        for c in clients.values():
            c.shell.Dispose()
        os.kill(os.getpid(), signal.SIGINT)

    @endpoint()
    async def ping(req: WsRequest):
        k = req.data.get("client")
        if k is None: return
        c = clients.get(k)
        if c is None: return
        # refresh terminal time for culling in shell ep
        c.last_active = CurrentTimeMillis()

    @endpoint()
    async def shell(req: WsRequest):
        d = req.data
        k = d.get("client")
        if k is None: return WsResponse(400, dict(err="[client] required"))
        cmd = d.get("script")
        if cmd is None: return WsResponse(400, dict(err="[script] required"))        
        if k not in clients:
            client = Client(k)
            clients[k] = client
        else:
            client = clients[k]
        client.shell.ExecAsync(cmd)
        client.last_active = CurrentTimeMillis()
        return WsResponse(204)

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

        async def flush_buffer(channel, client: Client):
            now = CurrentTimeMillis()
            # sends no faster than this, but paced by outer loop
            if now - client.last_flush <= 900: return
            match(channel):
                case "out":
                    buf = client.out
                case "err":
                    buf = client.err
            if len(buf)==0: return
            l = len(buf)
            success = await sender.RobustSend(
                WsRequest(
                    endpoint="stream",
                    data=dict(channel=channel, buf=buf)
                ),
                channel=c.key,
                timeout=1,
            )
            if success:
                if l == len(buf): 
                    buf.clear()
                else:
                    match(channel):
                        case "out":
                            client.out = buf[l:]
                        case "err":
                            client.err = buf[l:]
            client.last_flush = now

        while True:
            loop_start = CurrentTimeMillis()
            if lockf is not None:
                if not lockf._file.exists(): 
                    os.kill(os.getpid(), signal.SIGINT)
                    lockf = None
            # check for stale terminals
            async def cull(c: Client):
                now = CurrentTimeMillis()
                if now - c.last_active <= 1000: return False
                c.shell.Dispose()
                return True
            todo = list(clients.values())
            for c in todo:
                culled = await cull(c)
                if culled: del clients[c.key]
            # flush io buffers
            to_send = []
            for c in clients.values():
                to_send.append(flush_buffer("out", c))
                to_send.append(flush_buffer("err", c))
            await asyncio.gather(*to_send)

            loop_delta = CurrentTimeMillis()-loop_start
            remain = max(0, 1000-loop_delta)
            await asyncio.sleep(remain/1000) # keep server alive

    async def safe():
        try:
            await main()
        except (KeyboardInterrupt, asyncio.exceptions.CancelledError, BadNamespaceError):
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
    data = res.data
    return ServerStatus(
        health = SERVER_HEALTH.ALIVE,
        pid = data.get("pid", -1),
        n_clients = len(data.get("clients", []))
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
