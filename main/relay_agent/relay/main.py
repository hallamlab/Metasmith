import os, sys
from pathlib import Path
import time
from typing import Callable
from threading import Condition, Thread
from datetime import datetime as dt
from dataclasses import dataclass, field

from .coms.ipc import CurrentTimeMillis, RemoveTrailingNewline, GenerateId, TerminalProcess, PipeServer, PipeClient, IpcRequest, IpcResponse
from .logging import Log

WS = Path(os.curdir).resolve()
GRACE = 3*1000
MAIN_ID = "main"

def log(x, timestamp=True):
    Log.Info(x)

def RunServer(workspace: Path):
    if (workspace/f"{MAIN_ID}.in").exists():
        Log.Error(f"relay server already running in [{workspace}]")
        os._exit(1)

    try:
        pid = os.fork()
        if pid > 0:
            Log.Info(f"relay server started with pid: [{pid}]")
            # Exit parent process
            return
    except OSError as e:
        Log.Error(f"fork failed: {e.errno} ({e.strerror})")
        return
    # forked child

    Log.SetLogFile(workspace/"log")
    _start_msg = "starting relay server"
    Log.Info("="*len(_start_msg))
    Log.Info(_start_msg)

    @dataclass
    class Client:
        key: str
        birthtime: int
        channel: PipeServer
        terminal: TerminalProcess | None = None
        listeners: dict[Path, tuple[str, Callable, PipeClient]] = field(default_factory=dict)

        def _err(self, msg: str):
            return dict(error = msg)

        def shutdown(self, data: dict):
            with lock:
                nonlocal running
                running = False
            return 200, dict(message="shutting down")

        def status(self, data: dict):
            return 200, dict(clients=list(connections.keys()))

        def echo(self, data: dict):
            return 200, data
        
        def _ensure_terminal(self):
            if self.terminal is None:
                self.terminal = TerminalProcess()

        def bash(self, data: dict):
            cmd = data.get("script")
            if cmd is None: return 400, self._err(f"missing required field: [script]")
            self._ensure_terminal()
            if len(cmd) == 0 or cmd[-1] != "\n": cmd += "\n"
            self.terminal.Write(cmd)
            return 204, dict()

        def register_bash_listener(self, data: dict):
            stream = data.get("stream")
            valid_streams = {"out", "err"}
            if stream not in valid_streams: return 400, self._err(f"invalid stream [{stream}], not one of [{', '.join(valid_streams)}]")
            raw_path = data.get("channel")
            if raw_path is None: return 400, self._err(f"missing required field [channel] as server channel path")
            channel_path = workspace/raw_path
            if not channel_path.exists(): return 400, self._err(f"channel path does not exist [{channel_path}]")
            channel = PipeClient(channel_path)
            def _callback(x: bytes):
                msg = RemoveTrailingNewline(self.terminal.Decode(x))
                channel.Send(msg)
            self._ensure_terminal()
            if stream == "out":
                self.terminal.RegisterOnOut(_callback)
            elif stream == "err":
                self.terminal.RegisterOnErr(_callback)
            self.listeners[raw_path] = stream, _callback, channel
            return 200, dict(message="listener registered", id=str(channel._id))

        def remove_bash_listener(self, data: dict):
            raw_path = data.get("channel")
            if raw_path is None: return 400, self._err(f"missing required field [channel] as server channel path")
            key = raw_path
            if key not in self.listeners: return 404, self._err(f"listener not found")
            stream, cb, channel = self.listeners[key]
            channel.Dispose()
            if stream == "out":
                self.terminal.RemoveOnOut(cb)
            else:
                self.terminal.RemoveOnErr(cb)
            del self.listeners[key]
            return 200, dict(message="listener removed", id=str(channel._id))

        def _dispose(self):
            if self.terminal is not None:
                self.terminal.Dispose()
            for key in list(self.listeners.keys()):
                _, _, listener = self.listeners[key]
                listener.Dispose()
                del self.listeners[key]
            self.channel.Dispose()
        
    connections: dict[str, Client] = {}
    lock = Condition()
    running = True
    workspace = workspace.resolve()
    workspace.mkdir(parents=True, exist_ok=True)

    def _handle_connection(client: Client, raw: str):
        channel = client.channel
        log(f"request from: [{channel._id}]")
        req = IpcRequest.Parse(raw)
        def _err(msg: str, status=400):
            Log.Error(f"[{channel._id}]: {msg}")
            return IpcResponse(status=status, data=dict(error=msg))
        def _handle() -> IpcResponse:
            if not req.IsValid(): return _err(req.parse_error)
            ep_key = req.endpoint.lower()
            make_err_for_bad_ep = lambda: _err(f"invalid endpoint: [{ep_key}]", status=404)
            if ep_key.startswith("_"): return make_err_for_bad_ep()
            if not hasattr(client, ep_key): return make_err_for_bad_ep()
            ep = getattr(client, ep_key)
            if not callable(ep): return make_err_for_bad_ep()
            Log.Info(f"[{channel._id}]: calling [{ep_key}]")
            status, data = ep(req.data)
            return IpcResponse(
                status=status, data=data
            )
        res = _handle()
        res.message_id = req.message_id
        channel.Send(res.Serialize())
        
    def new_connection(channel: PipeServer, raw: str):
        req = IpcRequest.Parse(raw)
        def _err(msg: str):
            return dict(error = msg)
        def _handle() -> IpcResponse:
            log(f"new connection")
            if not req.IsValid(): return IpcResponse(
                status=400, data=_err(req.parse_error)
            )
            if req.endpoint == "connect":
                client = None
                def _cb (channel: PipeServer, raw: str):
                    if client is None: return
                    _handle_connection(client, raw)
                new_channel = PipeServer(workspace, _cb, id = GenerateId())
                client = Client(new_channel._id, CurrentTimeMillis(), new_channel)
                log(f"new client assigned to: [{new_channel._id}]")
                connections[new_channel._id] = client
                return IpcResponse(
                    status=200, data=dict(path=str(new_channel._client_path.resolve().relative_to(workspace)))
                )
            else:
                return IpcResponse(
                    status=400, data=_err(f"invalid endpoint: [{req.endpoint}]")
                )
        res = _handle()
        res.message_id = req.message_id
        channel.Send(res.Serialize())

    main_channel = PipeServer(workspace, new_connection, overwrite=True, id=MAIN_ID)
    def run():
        log("ready")
        while main_channel.IsOpen() and running:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                break

    def check_reap():
        now = CurrentTimeMillis()
        for id, client in list(connections.items()):
            start = client.birthtime
            channel = client.channel
            if now - start < GRACE: continue
            if channel.IsOpen() and channel._client_path.exists(): continue
            client._dispose()
            del connections[id]
            log(f"[{id}]: reaped")

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
        for id, client in list(connections.items()):
            client._dispose()

def _connect_as_client(server_path: Path, silent=False):
    if not server_path.exists():
        Log.Error(f"relay server not started in [{server_path}]")
        return
    with PipeClient(server_path) as p:
        try:
            res = p.Transact(IpcRequest(endpoint="connect"), timeout=1)
        except TimeoutError:
            Log.Error("error timeout")
            return
    if res.status != 200:
        Log.Error(f"error{res.data.get('error')}")
        return

    channel_path = Path(res.data.get("path"))
    if not silent: Log.Info(f"connecting to relay as [{channel_path.stem}]")
    if channel_path is None:
        Log.Error("error no channel path")
        return
    return server_path.parent/channel_path

def StopServer(workspace: Path):
    channel_path = _connect_as_client(workspace/f"{MAIN_ID}.in", silent=True)
    if channel_path is None: return
    with PipeClient(channel_path) as p:
        try:
            res = p.Transact(IpcRequest(endpoint="shutdown"), timeout=2)
        except TimeoutError:
            Log.Error("error timeout")
            return
        if res.status != 200:
            Log.Error(f"error: {res.data.get('error')}")
            return
        res_msg = res.data.get('message')
        if res_msg == "shutting down":
            Log.Info("Relay is shutting down")
        else:
            Log.Error(f"Relay stop request got unexpected status [{res.data.get('message')}]")

def GetStatus(workspace: Path):
    channel_path = _connect_as_client(workspace/f"{MAIN_ID}.in")
    if channel_path is None: return
    with PipeClient(channel_path) as p:
        try:
            res = p.Transact(IpcRequest(endpoint="status"), timeout=2)
        except TimeoutError:
            Log.Error("error timeout")
            return
        _clients = res.data.get("clients", [])
        if res.status != 200:
            Log.Error(f"error: {res.data.get('error')}")
            return
        Log.Info(f"number of clients [{len(_clients)}]")
        for client in _clients:
            Log.Info(f"  {client}")
