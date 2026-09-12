from pathlib import Path
from metasmith.coms.ipc import RemoveLeadingIndent
from metasmith.coms.terminals import LiveShell
from metasmith.coms.via_ws import CON_STATE, WsClient, WsRequest, WsResponse, LockFile
from metasmith.coms.via_ws import RemoteShell
import socketio
import time
import asyncio
from threading import Thread, Condition
from local.constants import WORKSPACE_ROOT
from metasmith.logging import Log
from metasmith.coms.via_ws import Sender, Reciever, CLIENT_TO_SERVER, SERVER_TO_CLIENT, WsRequest, WsResponse, CurrentTimeMillis
import logging
logging.getLogger('asyncio').setLevel(logging.WARNING)

workspace = Path("/home/tony/workspace/tools/Metasmith/main/relay_agent/XPS-laptop")

with RemoteShell(workspace) as shell:
    shell.RegisterOnOut(lambda x: print(x))
    shell.ExecAsync(f"date; sleep 3; echo start")
    for i in range(5):
        shell.ExecAsync(f"echo {i}")
    x  = shell.Exec("date", history=True)
    print(x)
