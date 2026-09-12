import os
from pathlib import Path
from threading import Thread, Condition
import time
from dataclasses import dataclass
import uvicorn

from metasmith.coms.via_ws import WsRequest, WsResponse, LockFile
from metasmith.coms.terminals import LiveShell
from metasmith.coms.via_ws import Sender, Reciever, CLIENT_TO_SERVER, SERVER_TO_CLIENT, WsRequest, WsResponse


from local.constants import WORKSPACE_ROOT

import socketio
import asyncio
from asyncio import Task
from collections import deque

@dataclass
class Client:
    k: str
    shell: LiveShell
    task: Task

print(os.getpid())
sio = socketio.AsyncServer(async_mode='asgi')
app = socketio.ASGIApp(sio)

async def on_send(channel: str, raw: dict):
    print(f"send [{channel}] [{raw}]")
    await sio.emit(channel, raw)
sender = Sender(on_send, SERVER_TO_CLIENT, CLIENT_TO_SERVER, lambda: True)
reciever = Reciever(sender)
async def inbound(sid, raw: dict):
    print(sid, raw)
    await reciever.NewRequest(raw)
sio.on(CLIENT_TO_SERVER, inbound)
async def ack(sid, raw: dict):
    await sender.Acknowledge(raw)
sio.on(SERVER_TO_CLIENT, ack)

async def test(x: WsRequest):
    print("respond test")
    return WsResponse(200, dict(echo=x.data))
reciever.AddHandler("test", test)

port = 8000
workspace = Path("./cache/ws_server_test")
lockf = LockFile(workspace, port)
try:
    uvicorn.run(app, port=port)
finally:
    lockf.Dispose()
