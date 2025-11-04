# _imported = False
# if not _imported:
#     import sys
#     if 'threading' in sys.modules:
#             raise Exception('threading module loaded before patching!')
#     import gevent.monkey; gevent.monkey.patch_thread()
#     _imported=True

import os
from pathlib import Path
from threading import Thread, Condition
import time
from attr import dataclass
import uvicorn

from metasmith.coms.via_ws import WsRequest, WsResponse, WsServer
from metasmith.coms.terminals import LiveShell
from metasmith.coms.via_ws import Sender, Reciever, CLIENT_TO_SERVER, SERVER_TO_CLIENT, WsRequest, WsResponse

# from flask import Flask
# from flask_socketio import SocketIO, emit
# import gevent
# from gevent.pool import Pool
# from gevent.pywsgi import WSGIServer

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

# print("Starting Socket.IO server on http://localhost:8000")
print(os.getpid())
# Create Async Socket.IO server
sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')
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

# connected_clients: dict[str, Client] = {}

# # Event handlers
# @sio.event
# async def connect(sid, environ):
#     """Handle client connection"""
#     print(f"Client connected: {sid}")
#     shell = LiveShell()
#     lines = deque()
#     shell.RegisterOnOut(lambda x: lines.append(('std_out', x)))
#     shell.RegisterOnErr(lambda x: lines.append(('std_err', x)))
#     async def check():
#         while True:
#             while len(lines)>0:
#                 channel, x = lines.popleft()
#                 await sio.emit(channel, data=dict(line=x), to=sid)
#             await sio.sleep(1)
#     task = asyncio.create_task(check())
#     connected_clients[sid] = Client(sid, shell, task)
#     await sio.emit('client', {'sid': sid}, to=sid)

# @sio.event
# async def disconnect(sid):
#     """Handle client disconnection"""
#     print(f"Client disconnected: {sid}")
#     if sid in connected_clients:
#         c = connected_clients[sid]
#         c.shell.Dispose()
#         c.task.cancel()
#         try:
#             await c.task
#         except asyncio.CancelledError:
#             pass
#         del connected_clients[sid]

# # @sio.event
# # async def echo(sid, data):
# #     await sio.emit("echo", room=sid)


# @sio.event
# async def bash(sid, data):
#     # await sio.emit("echo", room=sid)
#     # print(sid, data)
#     client = connected_clients[sid]
#     script = data.get("script")
#     print(script)
#     if script is None: return
#     client.shell.ExecAsync(script)

# # Background task example
# async def background_task():
#     """Example of a background task that sends periodic updates"""
#     count = 0
#     while True:
#         await asyncio.sleep(1)
#         count += 1
#         await sio.emit('ping', {
#             'count': count,
#             'message': f'Server update #{count}',
#             'connected_clients': len(connected_clients)
#         })
#         print(f"Sent server update #{count} to {len(connected_clients)} clients")
# sio.start_background_task(background_task)
uvicorn.run(app, host='0.0.0.0', port=8000)

# worker = Thread(target=_serve)
# worker.daemon = True
# worker.start()

# while True:
#     try:
#         time.sleep(1)
#     except KeyboardInterrupt:
#         break


# app = Flask(f"{WsServer}")
# socketio = SocketIO(app, async_mode='gevent', ping_interval=15)

# @app.route('/')
# def home():
#     return f"home"

# @socketio.on("connect")
# def c(data):
#     print("connect")

# @socketio.on("disconnect")
# def d(data):
#     print("disconnect")

# @socketio.on("asdf")
# def m(data):
#     print(f">> [{data}]")

# # @socketio.event
# # def handle_req(data):
# #     print(f"[{data}]")
# #     emit("echo", dict(echo=data))

# ws = Path("./cache/ws_server_test")
# import logging
# def make_logger(name):
#     logger = logging.getLogger(__file__+name)
#     logger.handlers.clear()
#     log_path = ws/name
#     file_handler = logging.FileHandler(log_path)
#     logger.addHandler(file_handler)
#     logger.propagate = False
#     return logger
# log_out = make_logger("main.log")
# log_err = make_logger("main.err")
# pool=Pool(1000)
# worker = WSGIServer(
#     ('localhost', 43000),
#     app,
#     log=log_out,
#     error_log=log_err,
#     spawn=pool,
# )
# worker.serve_forever()
# # worker.start()

# # try:
# #     while True:
# #         # gevent.wait(timeout=1/30)
# #         gevent.sleep(1/30)
# # except KeyboardInterrupt:
# #     worker.stop()
# #     worker.close()
# #     exit(0)


# ws = Path("./cache/ws_server_test")
# shell = LiveShell(sleep=gevent.sleep)
# print("shell")
# # shell = None
# try:
#     server = WsServer(ws)
#     print("server")
#     shell.RegisterOnOut(lambda x: server.outbound._send(WsRequest(endpoint="bash_out", data=dict(out=x)).Pack())) 
#     shell.RegisterOnErr(lambda x: server.outbound._send(WsRequest(endpoint="bash_err", data=dict(out=x)).Pack())) 

#     @server.Endpoint("bash")
#     def bash(req: WsRequest):
#         cmd = req.data.get("script")
#         if not cmd: return WsResponse(400, data=dict(err="missing [script]"))
#         if shell: shell.ExecAsync(cmd)
#         return WsResponse(204)

#     server.Start()
#     print("start")
#     i = 0
#     while True:
#         # print(i, end="\r", flush=True)
#         i+=1
#         gevent.sleep(1/30)
# except KeyboardInterrupt:
#     print("exit")
#     server.Dispose()
# finally:
#     if shell: shell.Dispose()
