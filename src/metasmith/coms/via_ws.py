from __future__ import annotations
import re
# from gevent import monkey
# monkey.patch_all()
from gevent.lock import Semaphore as Condition
from gevent.pywsgi import WSGIServer
from gevent.pool import Pool
import gevent
from flask import Flask
from flask_socketio import SocketIO, send, emit
import socketio
from enum import Enum
from typing import Callable
from pathlib import Path
from dataclasses import dataclass, field, fields
import os
import signal
import time
import numpy as np
import json
import hashlib

from ..logging import logging
from .ipc import CurrentTimeMillis, GenerateId, ResetGenerator
from .ipc import RemoveLeadingIndent, RemoveTrailingNewline

@dataclass
class WsMessage:
    message_id: str = field(default_factory=GenerateId, kw_only=True)
    parse_error: str | None = field(default=None, kw_only=True)
    timestamp: int = field(default_factory=CurrentTimeMillis, kw_only=True)

    @classmethod
    def Parse(cls, raw: dict):
        def _err(m):
            return WsMessage(message_id="", parse_error=m)
        try:
            return cls(**raw)
        except TypeError as e:
            emsg = str(e)
            emsg = emsg.replace("IpcModel.__init__()", "").strip()
            to_replace = {
                "missing 1 required positional argument:": "missing field",
                "got an unexpected keyword argument": "unknown field",
                "'": "[",
                "'": "]",
            }
            for k, v in to_replace.items():
                emsg = emsg.replace(k, v, 1)
            return _err(emsg)
        except json.JSONDecodeError as e:
            return _err(f"invalid json [{raw}]")

    def IsValid(self):
        return self.parse_error is None

    def Pack(self):
        bl = {"parse_error"}
        should_serialize = lambda k, v: not k.startswith("_") and not callable(v) and k not in bl
        d = {k:v for k, v in self.__dict__.items() if should_serialize(k, v)}
        return d

    def Serialize(self):
        d = self.Pack()
        return json.dumps(d)

@dataclass
class WsRequest(WsMessage):
    endpoint: str
    data: dict = field(default_factory=dict)

@dataclass
class WsResponse(WsMessage):
    status: int
    data: dict = field(default_factory=dict)

def _exponential_fallback(do: Callable, should_break: Callable[[], bool], timeout: float=5):
    start = CurrentTimeMillis()
    dt = 1/64
    next_try = 0
    while True:
        if should_break(): return True
        now = CurrentTimeMillis()
        if now >= next_try:
            do()
            dt = min(dt*2, 60)
            next_try = now + (dt*1000)
        remain = (timeout*1000)-(now-start)
        if remain <= 0:
            # raise TimeoutError("failed to send message")
            return False
        if should_break(): return True
        gevent.sleep(1/1000)

class Sender:
    def __init__(self, on_send: Callable[[dict]], is_service_live: Callable[[], bool]) -> None:
        self._send = on_send
        self._is_service_live = is_service_live
        self._pending: set[str] = set()
        self._lock = Condition()

    def RobustSend(self, message: WsRequest, timeout: float=5):
        raw = message.Pack()
        k = message.message_id
        with self._lock:
            self._pending.add(k)
        def send():
            self._send(raw)
        def should_break():
            if not self._is_service_live(): return True
            with self._lock:
                return k not in self._pending
        success = _exponential_fallback(do=send, should_break=should_break, timeout=timeout)
        if not self._is_service_live(): return False
        return success
                
    def Acknowledge(self, data: dict):
        req = WsResponse.Parse(data)
        if not isinstance(req, WsResponse): return
        k = req.message_id
        with self._lock:
            if k in self._pending: self._pending.remove(k)
RESPONSE_ENPOINT = "response"
class Reciever:
    def __init__(self, on_ack: Callable[[dict]], sender: Sender) -> None:
        self._sender = sender
        self._seen_message_ids: tuple[set[str], set[str], set[str]] = (set(), set(), set())
        self._last_rotate = CurrentTimeMillis()
        self._lock = Condition()
        self._handlers: dict[str, list[Callable[[WsRequest], WsResponse|None]]] = {}
        self._ack = on_ack

    def _has_seen(self, k: str):
        with self._lock:
            curr, last, new = self._seen_message_ids
            is_seen = k in curr or k in last
            now = CurrentTimeMillis()
            if now - self._last_rotate >= 10 * 60 * 1000: # 10 minutes
                self._seen_message_ids = new, curr, last
                last.clear()
                self._last_rotate = CurrentTimeMillis()
            return is_seen
    
    def _add_seen(self, k:str):
        with self._lock:
            curr, last, new = self._seen_message_ids
            curr.add(k)

    def AddHandler(self, endpoint: str, handler: Callable[[WsRequest], WsResponse|None]):
        self._handlers[endpoint] = self._handlers.get(endpoint, [])+[handler]
    
    def RemoveHandler(self, endpoint: str, handler: Callable[[WsRequest], WsResponse|None]):
        if endpoint not in self._handlers: return
        arr = self._handlers[endpoint]
        arr = [f for f in arr if f != handler]
        self._handlers[endpoint] = arr

    def NewRequest(self, data: dict):
        req = WsRequest.Parse(data)
        if not isinstance(req, WsRequest): return
        k = req.message_id
        self._ack(WsResponse(
            status=202,
            message_id=req.message_id,
        ).Pack())
        if self._has_seen(k): return
        self._add_seen(k)
        for handler in self._handlers.get(req.endpoint, []):
            raw_res = handler(req)
            if raw_res is None: continue
            d = raw_res.Pack()
            del d["message_id"]
            del d["timestamp"]
            res = WsRequest(
                message_id=req.message_id,
                endpoint=RESPONSE_ENPOINT,
                data=d,
            )
            self._sender.RobustSend(res)

class CON_STATE(Enum):
    IDLE = 0
    STARTING = 1
    ACTIVE = 2
    DISPOSED = 3

SERVER_TO_CLIENT = "s2c"
CLIENT_TO_SERVER = "c2s"
PORTF_PRE, PORTF_EXT = "ws_port", "lock"
class WsServer:
    def __init__(self, workspace: Path):
        self._lock = Condition()
        self._state = CON_STATE.IDLE
        self._worker = None
        self._port_file = None
        self._sio = None
        self.workspace = workspace
        def is_active():
            with self._lock:
                return self._state == CON_STATE.ACTIVE
        def send(channel: str, raw):
            if not is_active(): return
            if self._sio is None: return
            self._sio.emit(channel, raw) # do not use include_self=False
        self.outbound = Sender(on_send=lambda x: send(SERVER_TO_CLIENT, x), is_service_live=is_active)
        self.inbound = Reciever(on_ack=lambda x: send(CLIENT_TO_SERVER, x), sender=self.outbound)

    def Start(self):
        with self._lock:
            if self._state != CON_STATE.IDLE: return
            self._state = CON_STATE.STARTING
        def _start():
            workspace = self.workspace
            def make_logger(name):
                logger = logging.getLogger(__file__+name)
                logger.handlers.clear()
                log_path = workspace/name
                file_handler = logging.FileHandler(log_path)
                logger.addHandler(file_handler)
                logger.propagate = False
                return logger
            log_out = make_logger("main.log")
            log_err = make_logger("main.err")

            app = Flask(f"{WsServer}")
            def getSecret():
                secret_path = workspace/'secrets'
                secret_path.mkdir(exist_ok=True, parents=True)
                secret_path = secret_path/'secret'
                try:
                    with open(secret_path, 'r') as s:
                        return s.readlines()[0][:-1]
                except FileNotFoundError:
                    import secrets
                    with open(secret_path, 'w') as s:
                        tok = secrets.token_urlsafe(64)
                        s.write(tok)
                        s.flush()
                        return tok
            app.config['SECRET_KEY'] = getSecret()
            socketio = SocketIO(app, async_mode='gevent')
            self._sio = socketio

            @app.route('/')
            def home():
                return f"{type(self)}"

            @socketio.on(CLIENT_TO_SERVER)
            def handle_req(data):
                self.inbound.NewRequest(data)

            @socketio.on(SERVER_TO_CLIENT)
            def handle_res(data):
                self.outbound.Acknowledge(data)
                
            pool = Pool(1000)
            for _try in range(100):
                port = np.random.randint(49152, 65535)
                try:
                    self._worker = WSGIServer(
                        ('localhost', port),
                        app,
                        log=log_out,
                        error_log=log_err,
                        spawn=pool,
                    )
                    self._worker.start()
                    log_out.info(f"port [{port}]")
                    port_file = workspace/f"{PORTF_PRE}.{port}.{PORTF_EXT}"
                    port_file.touch(0o600)
                    self._port_file = port_file
                    break
                except OSError as e:
                    s = str(e)
                    if s.startswith("Address already in use:"):
                        log_err.warning(f"port occupied [{port}]")
                        continue
                    else:
                        raise e

        with self._lock:
            success = False
            try:
                _start()
                success = True
            finally:
                if success:
                    self._state = CON_STATE.ACTIVE
                else:
                    self._state = CON_STATE.IDLE
                    if isinstance(self._port_file, Path): self._port_file.unlink()
    
    def Endpoint(self, endpoint: str):
        """decorator"""
        def _register_function(handler: Callable[[WsRequest], WsResponse|None]):
            self.inbound.AddHandler(endpoint, handler)
        return _register_function

    def Dispose(self):
        with self._lock:
            self._state = CON_STATE.DISPOSED
        if self._worker is not None:
            self._sio = None
            self._worker.stop()
            self._worker.close()
        if isinstance(self._port_file, Path):
            self._port_file.unlink()
    
class WsClient:
    def __init__(self, workspace: Path, timeout: float=15) -> None:
        self._workspace = workspace
        self._lock = Condition()
        self._state = CON_STATE.IDLE
        self._recieved: dict[str, WsResponse] = {}

        def send(channel: str, raw):
            if not self.IsAlive(): return
            self._con.emit(event=channel, data=raw)
        self.outbound = Sender(on_send=lambda x: send(CLIENT_TO_SERVER, x), is_service_live=lambda: self.IsAlive())
        self.inbound = Reciever(on_ack=lambda x: send(SERVER_TO_CLIENT, x), sender=self.outbound)
        def on_response(req: WsRequest):
            def try_add():
                with self._lock:
                    k = req.message_id
                    if k in self._recieved: return
                    raw = req.data
                    req_d = req.__dict__
                    for field in fields(WsResponse):
                        if field.name in raw: continue
                        raw[field.name] = req_d[field.name]
                    res = WsResponse.Parse(raw)
                    if not isinstance(res, WsResponse): return
                    res.timestamp = CurrentTimeMillis()
                    self._recieved[k] = res
            try_add()
            now = CurrentTimeMillis()
            MAX_LIFE = 60*1000
            to_del = [k for k, v in self._recieved.items() if (now-v.timestamp)>=MAX_LIFE]
            if len(to_del)==0: return
            for k in to_del:
                del self._recieved[k] 
        self.inbound.AddHandler(
            endpoint=RESPONSE_ENPOINT,
            handler=on_response
        )

        sio = socketio.Client(reconnection_attempts=3)
        sio.on(CLIENT_TO_SERVER, lambda x: self.outbound.Acknowledge(x))
        sio.on(SERVER_TO_CLIENT, lambda x: self.inbound.NewRequest(x))
        self._con = sio
        self._reset(timeout=timeout)

    def _reset(self, timeout: float=5):
        def _try_connect():
            port = None
            for f in self._workspace.iterdir():
                if not (f.name.startswith(PORTF_PRE) and f.name.endswith(PORTF_EXT)): continue
                toks = f.name.split(".")
                port = int(toks[1]) # the middle of 3
            if port is None:
                return False
            try:
                self._con.connect(f'ws://localhost:{port}', transports=['websocket'])
            except ConnectionError:
                return False
            return True
        connected = False
        def do():
            nonlocal connected
            connected = _try_connect()
        def is_disposed():
            with self._lock:
                return self._state == CON_STATE.DISPOSED
        def should_break():
            if is_disposed(): return True
            if connected: return True
            return False
        success = False
        try:
            self._state = CON_STATE.STARTING
            success = _exponential_fallback(do=do, should_break=should_break, timeout=timeout)
            if is_disposed(): return
        finally:
            if success:
                self._state = CON_STATE.ACTIVE
            else:
                self._state = CON_STATE.IDLE

    def IsAlive(self):
        with self._lock:
            return self._state == CON_STATE.ACTIVE

    def Dispose(self):
        with self._lock:
            self._disposed = True
            if self._state == CON_STATE.ACTIVE:
                self._con.disconnect()

    def Endpoint(self, endpoint: str):
        """decorator"""
        def _register_function(handler: Callable[[WsRequest], WsResponse|None]):
            self.inbound.AddHandler(endpoint, handler)
        return _register_function

    def Transact(self, req: WsRequest, timeout: float=5):
        k = req.message_id
        self.outbound.RobustSend(req, timeout=timeout)
        start = CurrentTimeMillis()
        while True:
            with self._lock:
                if k in self._recieved:
                    return self._recieved[k]
            now = CurrentTimeMillis()
            if (now-start)>timeout*1000:
                raise TimeoutError
            gevent.sleep(1/1000)
