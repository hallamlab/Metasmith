import os, sys
import signal
from pathlib import Path
import time
from typing import Any, Callable
from threading import Condition, Thread
from datetime import datetime as dt
from dataclasses import dataclass, field
from enum import Enum
import random

from .coms.ipc import CurrentTimeMillis, RemoteShell, RemoveTrailingNewline, GenerateId, \
    TerminalProcess, PipeServer, PipeClient, IpcRequest, IpcResponse, ConnectionError
from .logging import Log

WS = Path(os.curdir).resolve()
GRACE = 3*1000
MAIN_ID = "main"

def log(x, timestamp=True):
    Log.Info(x)

def _clean_stale(workspace: Path):
    for p in list(workspace.glob("*.in")) + list(workspace.glob("*.out")):
        if not p.is_dir(): p.unlink()

class SERVER_STATUS(Enum):
    DEAD = 0
    ALIVE = 1
    STALE = 2
def _check_status(workspace: Path, silent=False):
    server_path = workspace/f"{MAIN_ID}.in"
    timeout = 3
    res = None
    if server_path.exists():
        for i in range(5):
            try:
                with PipeClient(server_path, timeout=timeout) as p:
                    res = p.Transact(IpcRequest(endpoint="ping"), timeout=timeout)
                break
            except ConnectionError:
                a, b = 2**(i-1), 2**i
                dt = random.random()*(b-a) + a
                time.sleep(dt)
        if res and res.IsValid() and res.status == 204:
            return SERVER_STATUS.ALIVE
        else:
            return SERVER_STATUS.STALE
    return SERVER_STATUS.DEAD

def RunServer(workspace: Path):
    workspace = workspace.absolute()
    status = _check_status(workspace)
    if status == SERVER_STATUS.ALIVE:
        Log.Error(f"relay server already running in [{workspace}]")
        os._exit(1)
    Log.AddLogFile(workspace/"main.log")
    Log.SetStdout(False)
    Log.Info(f"MONITOR: pid [{os.getpid()}]")
    child_pid = None
    while True: # monitor
        try:
            status = _check_status(workspace, silent=True)
            if status == SERVER_STATUS.STALE:
                Log.Info(f"MONITOR: removing stale connections at [{workspace}]")
                _clean_stale(workspace)
                if child_pid is not None:
                    Log.Info(f"MONITOR: sending kill signal to [{child_pid}]")
                    os.kill(pid, signal.SIGTERM)
                    child_pid = None
            if status != SERVER_STATUS.ALIVE:
                Log.Info(f"MONITOR: starting new server process at [{workspace}]")
                _rp, _wp = os.pipe()
                _rc, _wc = os.pipe()
                pid = os.fork()
                if pid == 0:
                    os.close(_rc)
                    os.close(_wp)
                    break # child leaves monitor loop to start server
                else:
                    os.close(_rp)
                    os.close(_wc)
                child_pid = int(os.read(_rc, 16))
                Log.Info(f"MONITOR: child pid [{child_pid}]")
                os.close(_rc)
                os.write(_wp, f"{os.getpid()}".encode())
                os.close(_wp)
            time.sleep(2)
        # except OSError as e:
        #     Log.Error(f"fork failed: {e.errno} ({e.strerror})")
        #     continue
        except KeyboardInterrupt:
            return
        
    # forked child (instance of server)
    os.write(_wc, f"{os.getpid()}".encode())
    os.close(_wc)
    monitor_pid = int(os.read(_rp, 16))
    os.close(_rp)

    # cant use Client type here
    # bc fork, so cant do from __future__ import annotations
    connections: dict[str, Any] = {} 
    reaper_lock = Condition()
    running = True
    workspace = workspace.resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    
    _start_msg = f"starting relay server with pid [{os.getpid()}], monitor [{monitor_pid}]"
    Log.Info("="*len(_start_msg))
    Log.Info(_start_msg)

    @dataclass
    class Client:
        key: str
        birthtime: int
        channel: PipeServer
        terminal: TerminalProcess | None = None
        listeners: dict[Path, tuple[str, Callable, PipeClient]] = field(default_factory=dict)
        last_ping: int = field(default_factory=CurrentTimeMillis)

        def _err(self, msg: str):
            return dict(error = msg)

        def ping(self, data: dict):
            self.last_ping = CurrentTimeMillis()
            return 204, {}

        def shutdown(self, data: dict):
            with reaper_lock:
                nonlocal running
                running = False
            Log.Info(f"sending kill signal to monitor [{monitor_pid}]")
            os.kill(monitor_pid, signal.SIGTERM)
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

        def disconnect(self, data: dict):
            Log.Info(f"[{self.channel._id}]: disconnect")
            # action delayed to after sending response in _handle_connection()
            return 204, dict(disconnect=True)

        def _dispose(self):
            if self.terminal is not None:
                self.terminal.Dispose()
            for key in list(self.listeners.keys()):
                _, _, listener = self.listeners[key]
                listener.Dispose()
                del self.listeners[key]
            self.channel.Dispose()

    reaper_lock = Condition()
    reaper_queue: set[str] = set()
    def _handle_connection(client: Client, raw: str):
        channel = client.channel
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
        if "disconnect" in res.data:
            with reaper_lock:
                reaper_queue.add(client.channel._id)
        
    def new_connection(channel: PipeServer, raw: str):
        req = IpcRequest.Parse(raw)
        def _err(msg: str):
            return dict(error = msg)
        def _handle() -> IpcResponse:
            if not req.IsValid(): return IpcResponse(
                status=400, data=_err(req.parse_error)
            )
            if req.endpoint == "disconnect":
                return IpcResponse(204)
            if req.endpoint == "ping":
                return IpcResponse(204)
            Log.Info(f"new connection")
            if req.endpoint == "connect":
                if len(connections)>=64:
                    return IpcResponse(
                        429, data=_err(f"too many concurrent requests [{len(connections)}]")
                    )
                client = None
                def _cb (channel: PipeServer, raw: str):
                    if client is None: return
                    _handle_connection(client, raw)
                new_channel = PipeServer(workspace, _cb, id = GenerateId())
                client = Client(new_channel._id, CurrentTimeMillis(), new_channel)
                Log.Info(f"new client assigned to: [{new_channel._id}]")
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
        Log.Info("ready")
        while main_channel.IsOpen() and running:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                break

    def check_reap():
        if main_channel.reader.IsClosed():
            Log.Debug("main reader closed")

        now = CurrentTimeMillis()
        for id, client in list(connections.items()):
            client: Client = client
            start = client.birthtime
            channel = client.channel
            stale = now - client.last_ping > 3*1000
            if not stale and channel.IsOpen() and channel._client_path.exists():
                continue
            if not stale and id not in reaper_queue:
                if now - start < GRACE: continue
            client._dispose()
            del connections[id]
            if id in reaper_queue:
                msg = "disposed"
                reaper_queue.remove(id)
            else:
                msg = "reaped"
            Log.Info(f"[{id}]: {msg}")

    def reaper_process():
        while True:
            with reaper_lock:
                if not running: break
                check_reap()
                reaper_lock.wait(1)
    reaper = Thread(target=reaper_process, args=[])

    try:
        reaper.start()
        run()
    finally:
        Log.Info("closing main channel")
        main_channel.Dispose()

        try:
            Log.Info("stopping reaper")
            with reaper_lock:
                running = False
            reaper.join()
        except KeyboardInterrupt:
            pass

        Log.Info("closing client channels")
        for id, client in list(connections.items()):
            client._dispose()
        msg = f"stopped relay server with pid [{os.getpid()}]"
        Log.Info(msg)
        Log.Info("="*len(msg))

def _connect_as_client(server_path: Path, silent=False, timeout=3):
    if not server_path.exists():
        Log.Error(f"relay server not started in [{server_path}]")
        return
    try:
        with PipeClient(server_path, timeout=timeout) as p:
            res = p.Transact(IpcRequest(endpoint="connect"), timeout=timeout)
    except (TimeoutError, ConnectionError) as e:
        Log.Error(f"{e}")
        return
    if res.status != 200:
        Log.Error(f"error {res.data.get('error')}")
        return
    if res.status == 429:
        raise ConnectionError(res.data.get('error'))

    channel_path = Path(res.data.get("path"))
    if not silent: Log.Info(f"connecting to relay as [{channel_path.stem}]")
    if channel_path is None:
        Log.Error("error no channel path")
        return
    return server_path.parent/channel_path

def StopServer(workspace: Path):
    for i in range(6):
        channel_path = _connect_as_client(workspace/f"{MAIN_ID}.in", silent=True, timeout=1)
        if channel_path is None: return
        with PipeClient(channel_path) as p:
            try:
                res = p.Transact(IpcRequest(endpoint="shutdown"), timeout=1)
            except TimeoutError:
                time.sleep(0.5)
                continue
            if res.status != 200:
                Log.Error(f"error: {res.data.get('error')}")
                return
            res_msg = res.data.get('message')
            if res_msg == "shutting down":
                Log.Info("Relay is shutting down")
                return
            else:
                Log.Error(f"Relay stop request got unexpected status [{res.data.get('message')}]")
                return

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

def Bounce(workspace: Path, cmd: str):
    with RemoteShell(workspace/f"{MAIN_ID}.in") as shell:
        shell.RegisterOnOut(print)
        shell.RegisterOnErr(lambda x: print(x, file=sys.stderr))
        shell.Exec(cmd, timeout=None)
