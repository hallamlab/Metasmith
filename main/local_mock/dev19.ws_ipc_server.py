# _imported = False
# if not _imported:
#     import sys
#     if 'threading' in sys.modules:
#             raise Exception('threading module loaded before patching!')
#     import gevent.monkey; gevent.monkey.patch_thread()
#     _imported=True

from pathlib import Path
# from threading import Thread
import time

from metasmith.coms.via_ws import WsRequest, WsResponse, WsServer
from metasmith.coms.terminals import LiveShell

from flask import Flask
from flask_socketio import SocketIO, emit
import gevent
from gevent.pool import Pool
from gevent.pywsgi import WSGIServer

from local.constants import WORKSPACE_ROOT


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


ws = Path("./cache/ws_server_test")
shell = LiveShell(sleep=gevent.sleep)
print("shell")
# shell = None
try:
    server = WsServer(ws)
    print("server")
    shell.RegisterOnOut(lambda x: server.outbound._send(WsRequest(endpoint="bash_out", data=dict(out=x)).Pack())) 
    shell.RegisterOnErr(lambda x: server.outbound._send(WsRequest(endpoint="bash_err", data=dict(out=x)).Pack())) 

    @server.Endpoint("bash")
    def bash(req: WsRequest):
        cmd = req.data.get("script")
        if not cmd: return WsResponse(400, data=dict(err="missing [script]"))
        if shell: shell.ExecAsync(cmd)
        return WsResponse(204)

    server.Start()
    print("start")
    i = 0
    while True:
        # print(i, end="\r", flush=True)
        i+=1
        gevent.sleep(1/30)
except KeyboardInterrupt:
    print("exit")
    server.Dispose()
finally:
    if shell: shell.Dispose()
