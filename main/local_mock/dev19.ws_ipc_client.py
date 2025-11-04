from pathlib import Path
from metasmith.coms.ipc import RemoveLeadingIndent
from metasmith.coms.terminals import LiveShell
from metasmith.coms.via_ws import WsClient, WsRequest, WsResponse
import socketio
import time
import asyncio
from threading import Thread, Condition
# import gevent
# import socket
from local.constants import WORKSPACE_ROOT
from metasmith.logging import Log
from metasmith.coms.via_ws import Sender, Reciever, CLIENT_TO_SERVER, SERVER_TO_CLIENT, WsRequest, WsResponse, CurrentTimeMillis

import logging
logging.getLogger('asyncio').setLevel(logging.WARNING) # avoid printing "Using selector: EpollSelector"
async def main():
    port = 8000
    sio = socketio.AsyncClient()
    try:
        # @sio.event
        # async def connect():
        #     print(f"connected")

        # client_id = None
        # async def client(data):
        #     nonlocal client_id
        #     client_id = data.get("sid")
        #     print(f"assigned to [{client_id}]")
        # sio.on("client", client)

        # @sio.event
        # async def std_out(data):
        #     line = data.get("line")
        #     Log.Info(line)

        # @sio.event
        # async def std_err(data):
        #     line = data.get("line")
        #     Log.Error(line)
        async def on_send(channel: str, raw: dict):
            await sio.emit(channel, raw)
        sender = Sender(on_send, CLIENT_TO_SERVER, SERVER_TO_CLIENT, lambda: sio.connected)
        reciever = Reciever(sender)
        async def inbound(raw: dict):
            await reciever.NewRequest(raw)
        sio.on(SERVER_TO_CLIENT, inbound)
        async def ack(raw: dict):
            # print(f"ack", raw)
            await sender.Acknowledge(raw)
        sio.on(CLIENT_TO_SERVER, ack)

        async def test(x: WsRequest):
            print(x)
        reciever.AddHandler("response", test)
        await sio.connect(f'ws://localhost:{port}', transports=['websocket'])
        await sender.RobustSend(WsRequest("test", dict(a=1)))
        await sio.wait()  # Keeps the client running
        # while True:
        #     print(CurrentTimeMillis(), end="\r")
        #     await asyncio.sleep(0)
    except ConnectionError as e:
        print(f"Connection failed: {e}")

# with LiveShell() as shell:
#     shell.RegisterOnOut(lambda x: print(x))
#     shell.ExecAsync("while true; do echo 1; sleep 1; done")
#     # shell.ExecAsync("echo asdf")
#     time.sleep(3)

def _run():
    asyncio.run(main())
worker = Thread(target=_run)
worker.daemon = True
worker.start()

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        break

    
# # sio.on("connect", lambda: print('connected'))
# # sio.on("disconnect", lambda: print('dis'))
# sio.on("message", lambda x: print(x))
# sio.on("c2s", lambda x: print("c", x))
# sio.on("s2c", lambda x: print("s", x))
# sio.on("test", lambda x: print("test", x))

# port = None
# PORTF_PRE, PORTF_EXT = "ws_port", "lock"
# workspace = Path("./cache/ws_server_test")
# for f in workspace.iterdir():
#     if not (f.name.startswith(PORTF_PRE) and f.name.endswith(PORTF_EXT)): continue
#     toks = f.name.split(".")
#     port = int(toks[1]) # the middle of 3
# sio.connect(f'ws://localhost:{port}', transports=['websocket'])
# sio.emit("test", dict(x=1))
# sio.emit("c2s", WsRequest(
#     endpoint="echo",
#     data=dict(a=1),
# ).Pack())

# # sio.wait()
# # sio.sleep(1)
# for i in range(100):
#     gevent.sleep(1/100)
# # sio.sleep(1)
# # time.sleep(1/30)
# print(1)
# sio.disconnect()
# exit(0)

# for i in range(1000):
#     ws = Path("./cache/ws_server_test")
#     client = WsClient(ws)
#     print(f"{i} start")
#     @client.Endpoint("bash_out")
#     def on_out(req: WsRequest):
#         # print(req.data.get("out"))
#         return

#     @client.Endpoint("bash_err")
#     def on_err(req: WsRequest):
#         print(req)
#         return

#     res = client.Transact(
#         WsRequest(
#             endpoint="bash",
#             data=dict(script=f"""\
#                 echo "asdf"
#             """),
#         )
#     )
#     # print(res.status)
#     gevent.sleep(1)
#     # client.Dispose()
#     break
