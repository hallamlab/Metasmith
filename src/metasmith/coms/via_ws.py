from __future__ import annotations
import asyncio
import re
# from gevent import monkey
# monkey.patch_all()
# from gevent.lock import Semaphore as Condition
# from gevent.pywsgi import WSGIServer
# from gevent.pool import Pool
# import gevent
# from flask import Flask
# from flask_socketio import SocketIO, send, emit
import socketio
from enum import Enum
from typing import Callable, Any, TypeVar, TypeAlias
from types import CoroutineType
from pathlib import Path
from dataclasses import dataclass, field, fields
import os
import signal
import time
import numpy as np
import json
import socket
import hashlib
from concurrent.futures import ThreadPoolExecutor
from threading import Condition, Thread
from queue import Queue

import logging as py_logging
py_logging.getLogger('asyncio').setLevel(py_logging.WARNING) # avoid printing "Using selector: EpollSelector"

from ..logging import logging
from .ipc import CurrentTimeMillis, GenerateId, ResetGenerator
from .ipc import RemoveLeadingIndent, RemoveTrailingNewline
ResetGenerator()

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

type P[T: Any] = CoroutineType[Any, Any, T]
async def _exponential_fallback(do: Callable[[], P], should_break: Callable[[], P[bool]], timeout: float=5):
    start = CurrentTimeMillis()
    dt = 1/64
    next_try = 0
    while True:
        if await should_break(): return True
        now = CurrentTimeMillis()
        # print(now, next_try, next_try-now)
        if now >= next_try:
            await do()
            dt = min(dt*2, 60)
            next_try = now + (dt*1000)
        remain = (timeout*1000)-(now-start)
        if remain <= 0:
            # raise TimeoutError("failed to send message")
            return False
        if await should_break(): return True
        await asyncio.sleep(0.01)

# may need to allow sending of multiple messages while waiting for ack
class Sender:
    def __init__(self, on_send: Callable[[str, dict], P], outbound_channel: str, inbound_channel: str, is_service_live: Callable[[], bool]) -> None:
        self._send = on_send
        self._is_service_live = is_service_live
        self._pending: set[str] = set()
        self._channels = outbound_channel, inbound_channel

    async def RobustSend(self, message: WsRequest, timeout: float=5, channel: str|None=None):
        raw = message.Pack()
        k = message.message_id
        self._pending.add(k)
        async def send():
            if channel is None:
                c, _ = self._channels
            else:
                c = channel
            await self._send(c, raw)
        async def should_break():
            if not self._is_service_live(): return True
            return k not in self._pending
        await _exponential_fallback(do=send, should_break=should_break, timeout=timeout)
        if not self._is_service_live(): return False
        if k in self._pending:
            self._pending.remove(k)
            return False
        else:
            return True
                
    async def Acknowledge(self, data: dict):
        req = WsResponse.Parse(data)
        if not isinstance(req, WsResponse): return
        k = req.message_id
        if k in self._pending: self._pending.remove(k)
        
RESPONSE_ENPOINT = "response"
class Reciever:
    def __init__(self, sender: Sender) -> None:
        self._sender = sender
        self._seen_message_ids: tuple[set[str], set[str], set[str]] = (set(), set(), set())
        self._last_rotate = CurrentTimeMillis()
        self._handlers: dict[str, list[Callable[[WsRequest], P[WsResponse|None]]]] = {}

    def _has_seen(self, k: str):
        curr, last, new = self._seen_message_ids
        is_seen = k in curr or k in last
        now = CurrentTimeMillis()
        if now - self._last_rotate >= 10 * 60 * 1000: # 10 minutes
            self._seen_message_ids = new, curr, last
            last.clear()
            self._last_rotate = CurrentTimeMillis()
        return is_seen
    
    def _add_seen(self, k:str):
        curr, last, new = self._seen_message_ids
        curr.add(k)

    def AddHandler(self, endpoint: str, handler: Callable[[WsRequest], P[WsResponse|None]]):
        self._handlers[endpoint] = self._handlers.get(endpoint, [])+[handler]
    
    def RemoveHandler(self, endpoint: str, handler: Callable[[WsRequest], P[WsResponse|None]]):
        if endpoint not in self._handlers: return
        arr = self._handlers[endpoint]
        arr = [f for f in arr if f != handler]
        self._handlers[endpoint] = arr

    async def NewRequest(self, data: dict):
        req = WsRequest.Parse(data)
        if not isinstance(req, WsRequest): return
        k = req.message_id
        _, c = self._sender._channels
        await self._sender._send(c, WsResponse(
            status=202,
            message_id=req.message_id,
        ).Pack())
        if self._has_seen(k): return
        self._add_seen(k)
        for handler in self._handlers.get(req.endpoint, []):
            raw_res = await handler(req)
            if raw_res is None: continue
            d = raw_res.Pack()
            del d["message_id"]
            del d["timestamp"]
            res = WsRequest(
                message_id=req.message_id,
                endpoint=RESPONSE_ENPOINT,
                data=d,
            )
            await self._sender.RobustSend(res)

class CON_STATE(Enum):
    IDLE = 0
    STARTING = 1
    ACTIVE = 2
    DISPOSED = 3

SERVER_TO_CLIENT = "s2c"
CLIENT_TO_SERVER = "c2s"
PORTF_PRE, PORTF_EXT = "ws_port", "lock"

class LockFile:
    def __init__(self, workspace: Path, port: int) -> None:
        candidates = self._get_candidates(workspace)
        if len(candidates)>0:
            raise FileExistsError(candidates[0])
        workspace.mkdir(exist_ok=True, parents=True)
        host = socket.gethostname()
        self._file = workspace/f"{PORTF_PRE}.{host}.{port}.{PORTF_EXT}"
        self._file.touch(0o644)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.Dispose()

    def Dispose(self):
        if self._file.exists(): self._file.unlink()

    @classmethod
    def _get_candidates(cls, workspace: Path):
        candiates: list[Path] = []
        if not workspace.exists(): return candiates
        for f in workspace.iterdir():
            if f.name.startswith(PORTF_PRE):
                candiates.append(f)
        return candiates

    @classmethod
    def GetPort(cls, workspace: Path):
        candidates = cls._get_candidates(workspace)
        pre, host, port, ext = candidates[0].name.split(".")
        return int(port)

class WsClient:
    def __init__(self, workspace: Path, timeout: float=15) -> None:
        self._workspace = workspace
        self._lock = Condition()
        self._state = CON_STATE.IDLE
        self._recieved: dict[str, WsResponse] = {}
        self._to_send: Queue[tuple[str, dict]] = Queue()
        self._key = GenerateId(3)
        self._thread_pool = ThreadPoolExecutor(3)
        
        async def queue_send(channel: str, raw: dict):
            if not self.IsAlive(): return
            self._to_send.put_nowait((channel, raw))
            # self._con.emit(event=channel, data=raw)
        self.outbound = Sender(queue_send, CLIENT_TO_SERVER, SERVER_TO_CLIENT, is_service_live=lambda: self.IsAlive())
        self.inbound = Reciever(sender=self.outbound)
        async def on_response(req: WsRequest):
            def try_add_response():
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
            try_add_response()
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

        self._buf_out: list[str] = []
        self._buf_err: list[str] = []
        async def on_stream(req: WsRequest):
            c = req.data.get("channel")
            if c is None: return
            buf = req.data.get("buf")
            if buf is None: return
            match(c):
                case "out":
                    self._buf_out.append(buf)
                case "err":
                    self._buf_err.append(buf)
        self.inbound.AddHandler(
            endpoint="stream",
            handler=on_stream
        )
        
        self._worker: Thread|None = None
        self._reset(timeout=timeout)

    def _reset(self, timeout: float=5):
        async def main(sio: socketio.AsyncClient):
            async def inbound(raw: dict):
                with self._lock:
                    if self._state != CON_STATE.ACTIVE: return
                await self.inbound.NewRequest(raw)
            sio.on(SERVER_TO_CLIENT, inbound)
            sio.on(self._key, inbound)
            async def ack(raw: dict):
                with self._lock:
                    if self._state != CON_STATE.ACTIVE: return
                await self.outbound.Acknowledge(raw)
            sio.on(CLIENT_TO_SERVER, ack)

            async def _try_connect(port: int):
                try:
                    await sio.connect(f'ws://localhost:{port}', transports=['websocket'])
                except ConnectionError:
                    return False
                return True
            connected = False
            async def do():
                nonlocal connected
                connected = await _try_connect(LockFile.GetPort(self._workspace))
            def is_disposed():
                with self._lock:
                    return self._state == CON_STATE.DISPOSED
            async def should_break():
                if is_disposed(): return True
                if connected: return True
                return False
            async def connect():
                success = False
                try:
                    success = await _exponential_fallback(do=do, should_break=should_break, timeout=timeout)
                finally:
                    with self._lock:
                        if success:
                            self._state = CON_STATE.ACTIVE
                        else:
                            self._state = CON_STATE.IDLE
                    return success
            success = await connect()
            if not success: return

            last_ping = 0
            while True:
                with self._lock:
                    if self._state != CON_STATE.ACTIVE:
                        await sio.disconnect()
                        return
                while not self._to_send.empty():
                    c, raw = self._to_send.get()
                    await sio.emit(c, raw)
                now = CurrentTimeMillis()
                if now-last_ping>=1000:
                    await sio.emit(CLIENT_TO_SERVER, data=WsRequest("ping", dict(client=self._key)).Pack())
                    last_ping = now
                await asyncio.sleep(0.1)

        with self._lock:
            self._state = CON_STATE.STARTING
        sio = socketio.AsyncClient(reconnection_attempts=3)
        def _run():
            asyncio.run(main(sio))
        worker = Thread(target=_run)
        worker.daemon = True
        worker.start()
        self._worker = worker
    
        start = CurrentTimeMillis()
        while True:
            now = CurrentTimeMillis()
            with self._lock:
                match(self._state):
                    case CON_STATE.ACTIVE | CON_STATE.DISPOSED:
                        return
                    case CON_STATE.IDLE:
                        # failed
                        raise ConnectionError()
            if now-start>=timeout*1000:
                raise TimeoutError()
            time.sleep(0.1)

    def GetKey(self):
        return self._key

    def IsAlive(self):
        with self._lock:
            return self._state == CON_STATE.ACTIVE

    def Dispose(self):
        with self._lock:
            if self._state == CON_STATE.ACTIVE:
                self._state = CON_STATE.DISPOSED
        if self._worker:
            self._worker.join()
        self._thread_pool.shutdown()

    def Endpoint(self, endpoint: str):
        """decorator"""
        def _register_function(handler: Callable[[WsRequest], WsResponse|None]):
            async def async_wrapper(req: WsRequest):
                return handler(req)
            self.inbound.AddHandler(endpoint, async_wrapper)
        return _register_function

    def Transact(self, req: WsRequest, timeout: float=5):
        k = req.message_id
        def send(req, timeout):
            success = False
            async def f():
                nonlocal success
                success = await self.outbound.RobustSend(req, timeout=timeout)
            asyncio.run(f())
            return success
        future = self._thread_pool.submit(send, req, timeout)
        future.result() # await send
        start = CurrentTimeMillis()
        while True:
            with self._lock:
                if self._state != CON_STATE.ACTIVE: raise ConnectionError()
                if k in self._recieved:
                    return self._recieved[k]
            now = CurrentTimeMillis()
            if (now-start)>timeout*1000:
                raise TimeoutError()
            time.sleep(0.001)
