from pathlib import Path
from metasmith.coms.via_ws import WsClient, WsRequest, WsResponse
import socketio
import time
import gevent
# import socket
from local.constants import WORKSPACE_ROOT

# sio = socketio.Client()
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

for i in range(1000):
    ws = Path("./cache/ws_server_test")
    client = WsClient(ws)
    print(f"{i} start")
    @client.Endpoint("bash_out")
    def on_out(req: WsRequest):
        # print(req.data.get("out"))
        return

    @client.Endpoint("bash_err")
    def on_err(req: WsRequest):
        print(req)
        return

    res = client.Transact(
        WsRequest(
            endpoint="bash",
            data=dict(script=f"""\
                echo "asdf"
            """),
        )
    )
    # print(res.status)
    gevent.sleep(1)
    # client.Dispose()
    break
