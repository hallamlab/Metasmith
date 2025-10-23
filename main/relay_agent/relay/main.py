import os, sys
import signal
from signal import Signals
from pathlib import Path
import time
from typing import Any, Callable
from threading import Condition, Thread
from datetime import datetime as dt
from dataclasses import dataclass, field
from enum import Enum
import random

from .coms.ipc import CurrentTimeMillis, RemoteShell, RemoveTrailingNewline, GenerateId, ResetGenerator, \
    TerminalProcess, PipeServer, PipeClient, IpcRequest, IpcResponse, ConnectionError
from .logging import Log

WS = Path(os.curdir).resolve()
MAIN_ID = "main"

def log(x, timestamp=True):
    Log.Info(x)

class SERVER_STATUS(Enum):
    DEAD = 0
    ALIVE = 1
    STALE = 2
def _check_status(workspace: Path, full=False):
    def check_minimal() -> SERVER_STATUS:
        server_path = workspace/f"{MAIN_ID}.in"
        res = None
        if server_path.exists():
            for i in range(-2, 3, 1): # <8s
                try:
                    with PipeClient(server_path) as p:
                        res = p.Transact(IpcRequest(endpoint="ping"), timeout=1)
                    break
                except (ConnectionError, TimeoutError, OSError):
                    dt = random.random()*2**i
                    time.sleep(dt)
            if res and res.IsValid() and res.status == 204:
                return SERVER_STATUS.ALIVE
            else:
                return SERVER_STATUS.STALE
        return SERVER_STATUS.DEAD
    def check_full() -> SERVER_STATUS:
        res = _connect_as_client(workspace/f"{MAIN_ID}.in")
        if res is None: return SERVER_STATUS.STALE
        channel_path, key = res
        with PipeClient(channel_path, key) as p:
            try:
                res = p.Transact(IpcRequest(endpoint="status", data=dict(connection=key)), timeout=2)
            except TimeoutError:
                return SERVER_STATUS.STALE
            if res.status != 200:
                return SERVER_STATUS.STALE
            return SERVER_STATUS.ALIVE
    status = check_minimal()
    match status:
        case SERVER_STATUS.ALIVE:
            return check_full() if full else status
        case _:
            return status

def RunServer(workspace: Path, channels: int):
    workspace = workspace.absolute()
    workspace.mkdir(parents=True, exist_ok=True)
    status = _check_status(workspace)
    if status == SERVER_STATUS.ALIVE:
        Log.Error(f"relay server already running in [{workspace}]")
        sys.exit(0)
    for f in workspace.iterdir():
        if f.name.endswith("lock"): f.unlink()
    ResetGenerator()
    session_key = GenerateId(24)
    session_lock = workspace/f"session-{session_key}.lock"
    session_lock.touch()
    Log.AddLogFile(workspace/"main.log")
    Log.SetStdout(False)
    Log.Info("")
    Log.Info(f"MONITOR: pid [{os.getpid()}]")
    child_pid = None
    shutdown_callbacks: list[Callable] = []
    def shutdown(code: int):
        for f in shutdown_callbacks:
            f()
        session_lock.unlink(missing_ok=True)
        Log.Info(f"exit | pid: [{os.getpid()}]")
        sys.exit(code)
    def _on_shutdown(signum, frame):
        sig = Signals(signum).name
        Log.Info(f"shutdown signal | pid: [{os.getpid()}], sig: [{sig}/{signum}]")
        shutdown(0)
    signal.signal(signal.SIGCHLD, signal.SIG_IGN) # no zombie children
    signal.signal(signal.SIGINT, _on_shutdown)
    def _on_die(signum, frame):
        sig = {9:"KILL", 15:"TERM", 2:"INT"}.get(signum, "")
        if sig: sig = f" ({sig})"
        Log.Warn(f"killed | pid: [{os.getpid()}], sig: [{signum}{sig}]")
        sys.exit(1)
    signal.signal(signal.SIGTERM, _on_die)

    def _clean_stale(workspace: Path):
        todo = list(workspace.glob("*.in")) + list(workspace.glob("*.out"))
        if len(todo)==0: return
        Log.Info(f"MONITOR: removing stale connections at [{workspace}]")
        for p in todo:
            if not p.is_dir(): p.unlink(missing_ok=True)

    def _clean_fd(log_prefix="") -> int:
        to_del = []
        open_fd = list(Path(f"/proc/{os.getpid()}/fd").iterdir())
        for f in open_fd:
            if Path(os.path.realpath(f)).name.endswith("(deleted)"): to_del.append(f)
        if len(to_del) < 128: return len(open_fd)
        Log.Info(f"{log_prefix}attempting to remove [{len(to_del)}] stale fd of [{len(open_fd)}]")
        for f in to_del:
            try:
                fd = int(f.name)
                os.close(fd)
            except:
                pass
        # time.sleep(1) # not sure if this is needed
        open_fd = list(Path(f"/proc/{os.getpid()}/fd").iterdir())
        Log.Info(f"{log_prefix}[{len(open_fd)}] fd remain")
        return len(open_fd)

    while True: # monitor
        try:
            if not session_lock.exists():
                shutdown(0)
                return
            nfds = _clean_fd("MONITOR: ")
            if nfds==0: status = SERVER_STATUS.DEAD
            if status is None: status = _check_status(workspace, full=True)
            if status == SERVER_STATUS.STALE:
                if child_pid is not None:
                    Log.Info(f"MONITOR: sending kill signal to [{child_pid}]")
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except OSError as e:
                        Log.Info(f"MONITOR: [{e}]")
                    child_pid = None
                    for i in range(3):
                        if not (workspace/"main.in").exists(): break
                        time.sleep(0.5) # wait for child to exit
            if status != SERVER_STATUS.ALIVE:
                _clean_stale(workspace)
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
            status = None
            time.sleep(5)
        except OSError as e:
            Log.Error(f"fork failed: {e.errno} ({e.strerror})")
            time.sleep(15)
            _clean_fd("MONITOR: ")
            continue
        except KeyboardInterrupt:
            Log.Info(f"MONITOR: exit [{child_pid}]")
            return
        
    # forked child (instance of server)
    ResetGenerator()
    os.write(_wc, f"{os.getpid()}".encode())
    os.close(_wc)
    monitor_pid = int(os.read(_rp, 16))
    os.close(_rp)

    # cant use Client type with connections
    # bc fork, so cant do from __future__ import annotations
    connections: dict[str, Any] = {} 
    reaper_lock = Condition()
    running = True

    workspace = workspace.resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    _start_msg = f"starting relay server with pid [{os.getpid()}], monitor [{monitor_pid}], and [{channels}] channels"
    Log.Info("="*len(_start_msg))
    Log.Info(_start_msg)

    @dataclass
    class Client:
        key: str = ""
        last_used: int = 0
        _seen_messages: set[str] = field(default_factory=set)

        def __post_init__(self):
            def _cb (channel: PipeServer, raw: str):
                _handle_connection(self, raw)
            self.channel = PipeServer(workspace, _cb, id = GenerateId(3))
            self._terminal = None

        def _get_terminal(self):
            if self._terminal is None:
                self._terminal = TerminalProcess()
                def _make_listener(stream: str):
                    def _callback(x: bytes):
                        if self._terminal is None: return
                        msg = RemoveTrailingNewline(self._terminal.Decode(x))
                        self.channel.Send(IpcResponse(103, dict(stream=stream, content=msg)).Serialize())
                    return _callback
                self._terminal.RegisterOnOut(_make_listener("out"))
                self._terminal.RegisterOnErr(_make_listener("err"))
            return self._terminal

        def _close_terminal(self):
            if self._terminal is None: return
            self._terminal.Dispose()
            self._terminal = None

        def _err(self, msg: str):
            return dict(error = msg)

        def status(self, req: IpcRequest):
            return 200, dict(
                clients=[(c.channel._id, c.key) for c in server_channels],
                monitor_pid=monitor_pid,
                relay_pid=os.getpid(),
                workspace=str(workspace),
                open_file_discriptors=len(list(Path(f"/proc/{os.getpid()}/fd").iterdir())),
                lock=str(session_lock),
            )

        def ping(self, req: IpcRequest):
            # Log.Debug(f"[{self.key}]:[{self.channel._id}]")
            return 204, {}
        
        def echo(self, req: IpcRequest):
            data = req.data
            return 200, data

        def bash(self, req: IpcRequest):
            if req.message_id in self._seen_messages:
                return 204, {}
            data = req.data
            cmd = data.get("script")
            if cmd is None: return 400, self._err(f"missing required field: [script]")
            if len(cmd) == 0 or cmd[-1] != "\n": cmd += "\n"
            self._get_terminal().Write(cmd)
            return 204, {}

        def disconnect(self, req: IpcRequest):
            self._close_terminal()
            self._seen_messages.clear()
            Log.Info(f"[{self.channel._id} {self.key}] disconnected | {len(connections)-1}/{len(server_channels)}")
            return 204, dict(disconnect=True)

        def _dispose(self):
            self._close_terminal()
            self.channel.Dispose()

    def _handle_connection(client: Client, raw: str):
        with reaper_lock:
            client.last_used = CurrentTimeMillis()
        channel = client.channel
        req = IpcRequest.Parse(raw)
        def _err(msg: str|None, status=400, log=True):
            if log: Log.Warn(f"[{client.channel._id}]: {msg}")
            return IpcResponse(status=status, data=dict(error=msg))
        def _handle() -> IpcResponse:
            # Log.Debug(req)
            if not req.IsValid() or not isinstance(req, IpcRequest): return _err(req.parse_error)
            con_key = req.data.get("connection", "")
            if not con_key: return _err("no connection key", status=401)
            if con_key != client.key:
                safe_eps = {"ping", "disconnect"}
                return _err(f"wrong connection key. Used [{con_key}]. Expect [{client.key}]. [{req.endpoint}]", status=401, log=(req.endpoint not in safe_eps))
            ep_key = req.endpoint.lower()
            make_err_for_bad_ep = lambda: _err(f"invalid endpoint: [{ep_key}]", status=404)
            if ep_key.startswith("_"): return make_err_for_bad_ep()
            if not hasattr(client, ep_key): return make_err_for_bad_ep()
            # if ep_key != "ping": Log.Debug(f"[{client.key}] >>> {req}")
            ep: Callable = getattr(client, ep_key)
            if not callable(ep): return make_err_for_bad_ep()
            # Log.Debug(f"[{client.key}]: calling [{ep_key}]")
            status, data = ep(req)
            client._seen_messages.add(req.message_id)
            # if ep_key != "ping": Log.Debug(f"[{client.key}] <<< {status} {data}")
            return IpcResponse(
                status=status, data=data
            )
        res = _handle()
        res.message_id = req.message_id
        res.data["connection"] = client.key
        channel.Send(res.Serialize())
        if "disconnect" in res.data:
            with reaper_lock:
                del connections[client.key]
                client.key = ""

    server_channels: list[Client] = []
    for _ in range(channels):
        try:
            server_channels.append(Client())
        except OSError as e:
            Log.Error(e)
            if "out of pty devices" not in e.args:
                raise e
            shutdown(1)
    def _client_shutdown():
        Log.Info("closing client channels")
        l = len(server_channels)
        for i, client in enumerate(server_channels[::-1]):
            Log.Info(f"  {i+1}/{l}: [{client.channel._id}]")
            client._dispose()
    shutdown_callbacks.append(_client_shutdown)

    connection_history: dict[str, tuple[int, IpcResponse]] = {}
    def new_connection(channel: PipeServer, raw: str):
        req = IpcRequest.Parse(raw)
        def _err(msg: str|None):
            return dict(error = msg)
        def _handle() -> IpcResponse:
            if not req.IsValid() or not isinstance(req, IpcRequest): return IpcResponse(
                status=400, data=_err(req.parse_error)
            )
            if req.endpoint == "disconnect":
                return IpcResponse(204) # not applicable
            if req.endpoint == "ping":
                return IpcResponse(204)
            if req.endpoint == "get_monitor":
                return IpcResponse(200, data=dict(pid=monitor_pid))
            if req.endpoint == "shutdown":
                with reaper_lock:
                    nonlocal running
                    running = False
                    reaper_lock.notify_all()
                return IpcResponse(204)
            if req.endpoint == "connect":
                with reaper_lock:
                    connection_key = req.message_id
                    if connection_key in connection_history: # repeat message
                        _, r = connection_history[connection_key]
                        return r

                    if len(connections)>=len(server_channels):
                        return IpcResponse(
                            429, data=_err(f"too many concurrent requests [{len(connections)}]")
                        )
                    active_clients = list(connections.values())
                    available_client = next(iter(c for c in server_channels if c not in active_clients))
                    available_client.last_used = CurrentTimeMillis()
                    available_client.key = connection_key
                    # Log.Debug(f"{available_client.channel._id}/{connection_key} {connections.keys()}")
                    connections[connection_key] = available_client
                    Log.Info(f"[{available_client.channel._id} {connection_key}] connected | {len(connections)}/{len(server_channels)}")
                    res = IpcResponse(
                        status=200, data=dict(connection=connection_key, path=str(available_client.channel._client_path.resolve().relative_to(workspace)))
                    )
                    connection_history[connection_key] = available_client.last_used, res
                return res
            else:
                Log.Error(f"invalid call to [{req.endpoint}]")
                return IpcResponse(
                    status=400, data=_err(f"invalid endpoint: [{req.endpoint}]")
                )
        res = _handle()
        res.message_id = req.message_id
        channel.Send(res.Serialize())

    GRACE = 5
    HIST_TIME = 60
    main_channel = PipeServer(io_dir=workspace, callback=new_connection, overwrite=True, id=MAIN_ID)
    def run():
        Log.Info("ready")
        nonlocal running
        while main_channel.IsOpen():
            now = CurrentTimeMillis()
            with reaper_lock:
                # Log.Debug("="*12)
                # Log.Debug(len(connections))
                to_del = []
                for k, (ts, c) in connection_history.items():
                    if (now-ts)>HIST_TIME*1000: to_del.append(k)
                for k in to_del: del connection_history[k]
                # Log.Debug("."*12)
                connected_channels = {c.channel._id:k for k, c in connections.items()}
                to_del.clear()
                for client in server_channels:
                    # active = client.channel._id in connected_channels
                    # same = client.key == connected_channels[client.channel._id] if active else True
                    # Log.Debug(f"[{client.channel._id}] [{client.key if active else ' '*12}] [{' '*12 if same else connected_channels[client.channel._id]}] {(now - client.last_used)/1000:.2f}")
                    if client.channel._id not in connected_channels: continue
                    if client.key == connected_channels[client.channel._id]:
                        _grace = GRACE*1000
                        if now - client.last_used<_grace: continue
                    to_del.append(client)
                for client in to_del:
                    client._close_terminal()
                    del connections[client.key]
                    Log.Info(f"[{client.channel._id} {client.key}] reaped | {len(connections)}/{len(server_channels)}")
                    client.key = ""
                # Log.Debug("="*12)
                if not running: break
                if not session_lock.exists(): break
                try:
                    reaper_lock.wait(0.1 if len(connections)==len(server_channels) else GRACE)
                except KeyboardInterrupt:
                    running = False
                    break

    def _main_shutdown():
        Log.Info("closing main channel")
        main_channel.Dispose()
    shutdown_callbacks.append(_main_shutdown)
    try:
        run()
    finally:
        shutdown(0)

def _connect_as_client(server_path: Path, silent=False, timeout=3):
    if not server_path.exists():
        Log.Error(f"relay server not started in [{server_path}]")
        return
    try:
        with PipeClient(server_path) as p:
            res = p.Transact(IpcRequest(endpoint="connect"), timeout=timeout)
    except (TimeoutError, ConnectionError) as e:
        Log.Error(f"{e}")
        return
    if res.status != 200:
        Log.Error(f"error {res.data.get('error')}")
        return
    if res.status == 429:
        raise ConnectionError(res.data.get('error'))

    channel_path = Path(res.data["path"])
    con: str = res.data["connection"]
    if not silent: Log.Info(f"connecting to relay on [{channel_path.stem}] as [{con}]")
    if channel_path is None:
        Log.Error("error no channel path")
        return
    return server_path.parent/channel_path, con

def StopServer(workspace: Path):
    delays = [2**i for i in range(-2, 4, 1)] # <32s
    server_path = workspace/f"{MAIN_ID}.in"
    if server_path.exists():
        client = None
        monitor_pid = None
        try:
            for dt in delays:
                try:
                    if not client:
                        client = PipeClient(server_path)
                        if not client: continue

                    if not monitor_pid:
                        res = client.Transact(IpcRequest(endpoint="get_monitor"), timeout=1)
                        if res.status != 200:
                            Log.Error(f"get_monitor: [{res.status}:{res.data}]")
                            continue
                        monitor_pid = res.data.get("pid")
                        if monitor_pid:
                            Log.Info(f"sending kill signal to monitor [{monitor_pid}]")
                            os.kill(monitor_pid, signal.SIGINT)
                        else:
                            continue

                    res = client.Transact(IpcRequest(endpoint="shutdown"), timeout=1)
                    if res.status != 204:
                        Log.Error(f"shutdown: [{res.status}:{res.data}]")
                    else:
                        Log.Info(f"shutdown request sent to relay")
                        break
                except (ConnectionError, TimeoutError, OSError):
                    dt = random.random()*dt
                    time.sleep(dt)
        finally:
            if client: client.Dispose()
    else:
        Log.Info(f"relay not running at [{workspace}]")

def GetStatus(workspace: Path):
    status = _check_status(workspace)
    Log.Info(f"status: [{status.name}]")

    res = _connect_as_client(workspace/f"{MAIN_ID}.in")
    if res is None: return
    channel_path, key = res
    with PipeClient(channel_path, key) as p:
        try:
            res = p.Transact(IpcRequest(endpoint="status", data=dict(connection=key)), timeout=2)
        except TimeoutError:
            Log.Error("error timeout")
            return
        if res.status != 200:
            Log.Error(f"error: {res.data.get('error')}")
            return
        for k, v in res.data.items():
            if k == "connection": continue
            if k == "clients":
                Log.Info(f"number of connections [{len([x for _, x in v if x])}]")
                for channel, connection  in v:
                    Log.Info(f"  {channel} : {connection}")
            else:
                Log.Info(f"{k}: [{v}]")

def Bounce(workspace: Path, cmd: str):
    with RemoteShell(workspace/f"{MAIN_ID}.in") as shell:
        shell.RegisterOnOut(print)
        shell.RegisterOnErr(lambda x: print(x, file=sys.stderr))
        shell.Exec(cmd, timeout=None)
