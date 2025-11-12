from __future__ import annotations
import os
from pathlib import Path
from typing import IO, Callable, Any
from threading import Condition, Thread
from dataclasses import dataclass, field
import json
from collections import deque
import hashlib
import time
import random

from .ipc import NonBlockingReader, GenerateId, CurrentTimeMillis, RemoveTrailingNewline, RemoveLeadingIndent
from .terminals import ShellResult

IPC_HASH_LEN = 16
@dataclass
class PipeMessage:
    # message_id: IpcMessageId = field(default_factory=IpcMessageId.Generate, kw_only=True)
    message_id: str = field(default_factory=GenerateId, kw_only=True)
    parse_error: str | None = field(default=None, kw_only=True)

    @classmethod
    def Parse(cls, raw: str):
        if len(raw)<IPC_HASH_LEN: return PipeMessage(parse_error="no hash")
        try:
            s = raw[:-IPC_HASH_LEN]
            h1 = hashlib.md5(s.encode("latin1")).hexdigest()[:IPC_HASH_LEN]
            h2 = raw[-IPC_HASH_LEN:]
            if h1 != h2: return PipeMessage(parse_error=f"corrupted [{h1} != {h2}] [{s}]")
            d = json.loads(s)
            return cls(**d)
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
            return PipeMessage(parse_error=emsg)
        except json.JSONDecodeError as e:
            return PipeMessage(parse_error=f"invalid json [{raw}]")

    def IsValid(self):
        return self.parse_error is None

    def Serialize(self):
        bl = {"parse_error"}
        should_serialize = lambda k, v: not k.startswith("_") and not callable(v) and k not in bl
        d = {k:v for k, v in self.__dict__.items() if should_serialize(k, v)}

        s = json.dumps(d)
        h = hashlib.md5(s.encode("latin1")).hexdigest()[:IPC_HASH_LEN]
        return s+h

@dataclass
class PipeRequest(PipeMessage):
    endpoint: str
    data: dict = field(default_factory=dict)

@dataclass
class PipeResponse(PipeMessage):
    status: int
    data: dict = field(default_factory=dict)

# should pre-create fifo for client
class PipeServer:
    def __init__(self, io_dir: Path, callback: Callable[[PipeServer, str], None], overwrite: bool=False, id: str|None = None) -> None:
        success = False
        try:
            self._id = "main" if id is None else id
            self._server_path = io_dir/f"{self._id}.in"
            self._client_path = io_dir/f"{self._id}.out"
            self._is_closing = False
            self._lock = Condition()
            
            for p in [self._server_path, self._client_path]:
                if p.exists():
                    if overwrite:
                        os.remove(p)
                        os.mkfifo(p)
                else:
                    os.mkfifo(p)

            self._server_channel = os.open(self._server_path, os.O_RDONLY|os.O_NONBLOCK)
            def _on_close(x):
                with self._lock:
                    self._is_closing=True
            self.reader = NonBlockingReader(self._server_channel, on_close=_on_close)
            self.reader.RegisterCallback(lambda x: callback(self, RemoveTrailingNewline(x.decode())))

            self._client_channel: int|None = None
            self._buffer = deque()
            def try_send():
                while len(self._buffer)>0:
                    with self._lock:
                        if self._is_closing: return
                    msg = self._buffer[0]
                    try:
                        if self._client_channel is None:
                            self._client_channel = os.open(self._client_path, os.O_WRONLY|os.O_NONBLOCK)
                        os.write(self._client_channel, (msg+"\n").encode())
                        self._buffer.popleft()
                    except OSError:
                        self._client_channel = None
                        with self._lock:
                            if self._is_closing: return
                            self._lock.wait(0.1)
                        return
            def sender_process():
                while True:
                    with self._lock:
                        if self._is_closing: break
                        if len(self._buffer) == 0:
                            self._lock.wait(10)
                        else:
                            try_send()
            self._sender = Thread(target=sender_process)
            self._sender.start()
            success = True
        finally:
            if not success: self.Dispose()

    def Send(self, msg: str):
        with self._lock:
            self._buffer.append(msg)
            self._lock.notify_all()

    def IsOpen(self):
        return self._server_path.exists()

    def Dispose(self):
        def _try_close(fd):
            if fd is None: return
            try:
                os.close(fd)
            except (OSError, TypeError):
                pass
        try:
            with self._lock:
                self._is_closing = True
                self._lock.notify_all()
            self.reader.Dispose()
            _try_close(self._server_channel)
            _try_close(self._client_channel)
            self._sender.join()
        except AttributeError:
            pass
        finally:
            for p in [self._client_path, self._server_path]:
                if not p.exists(): continue
                try:
                    os.remove(p)
                except FileNotFoundError:
                    pass # race conditioned between check and os.remove probably

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.Dispose()

class PipeClient:
    def __init__(self, server_pipe: Path, connection_key: str|None=None, on_push: Callable[[PipeClient, PipeResponse], None]|None = None) -> None:
        success = False
        try:
            self._id = server_pipe.stem
            io_dir = server_pipe.parent
            self._server_path = io_dir/f"{self._id}.in"
            self._client_path = io_dir/f"{self._id}.out"
            self._server_channel=-1
            self._lock = Condition()
            self._closed = False
            self._connection_key = connection_key

            delays = [2**i for i in range(-4, 0, 1)] # < 1sec
            success = False
            for dt in delays:
                if self._server_path.exists() and self._client_path.exists():
                    self._server_channel = os.open(self._server_path, os.O_WRONLY|os.O_NONBLOCK)
                    self._client_channel = os.open(self._client_path, os.O_RDONLY|os.O_NONBLOCK)
                    success = True
                    break
                with self._lock:
                    if self._closed: return
                    self._lock.wait(dt)
            if not success: raise ConnectionError("failed to open channels")
            # start = CurrentTimeMillis()
            # i = -6
            # while self._client_path.exists() and self._server_path.exists():
            #     try:
            #         break
            #     except FileNotFoundError:
            #         pass # race condition
            #     now = CurrentTimeMillis()
            #     delay = 2**i
            #     time.sleep(max(min(delay*1000, timeout*1000-(now-start)), 0))
            #     i = min(0, i+1)
            #     if CurrentTimeMillis() - start > timeout*1000:
            #         raise TimeoutError("Failed to connect to server")

            def _on_close(reader: NonBlockingReader):
                with self._lock:
                    self._closed = True
            self._reader = NonBlockingReader(self._client_channel, on_close=_on_close)
            # self._reader = NonBlockingReader(self._client_channel)
            
            self._last_message_id: str|None = None
            self._last_response: PipeResponse|None = None
            def _on_response(x):
                res = PipeResponse.Parse(x.decode())
                if not isinstance(res, PipeResponse): return
                # the http code analogy breaks here, but close enough
                # https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/103
                if res.status == 103 and on_push is not None: on_push(self, res)
                if res.message_id != self._last_message_id: return
                # print(res)
                self._last_response = res
                with self._lock:
                    self._lock.notify_all()
            self._reader.RegisterCallback(_on_response)
            success = True
            
            self._last_sent = CurrentTimeMillis()
            self._sending = False
            def _keep_alive():
                while True:
                    with self._lock:
                        if self._closed: return
                        sending = self._sending
                    now = CurrentTimeMillis()
                    if not sending and now - self._last_sent > 1000:
                        success = self.Send(PipeRequest(endpoint="ping", data=dict(connection=self._connection_key)).Serialize())
                    else:
                        success = False
                    with self._lock:
                        self._lock.wait(1 if success else 0.1)

            self._keep_alive_worker = Thread(target=_keep_alive)
            self._keep_alive_worker.start()
        finally:
            if not success: self.Dispose()

    def Send(self, msg: str):
        try:
            with self._lock:
                self._sending = True
                os.write(self._server_channel, (msg+"\n").encode())
                self._last_sent = CurrentTimeMillis()
                self._sending = False
        except BrokenPipeError:
            return False
        return True

    def Transact(self, req: PipeRequest, timeout: int|float|None = 20) -> PipeResponse:
        closed_res = PipeResponse(500, dict(error="connection closed"))
        self._last_response = None
        self._last_message_id = req.message_id
        if self._connection_key: req.data["connection"] = self._connection_key
        msg = req.Serialize()
        start = CurrentTimeMillis()
        i, max_i = -3, 4 # 0.125 - 8
        while self._last_response is None:
            with self._lock:
                if self._closed:
                    return closed_res
            if not self.Send(msg): return closed_res
            delay = 2**i
            i = min(i+1, max_i)
            with self._lock:
                self._lock.wait(delay)
            if timeout and CurrentTimeMillis() - start > timeout*1000:
                raise TimeoutError("Failed to receive response")
        return self._last_response

    def Dispose(self):
        with self._lock:
            self._closed = True
            d = dict(connection=self._connection_key) if self._connection_key else {}
            try:
                self.Send(PipeRequest(endpoint="disconnect", data=d).Serialize())
            except OSError:
                pass
            self._lock.notify_all()

        try:
            if hasattr(self, "_keep_alive_worker"):
                self._keep_alive_worker.join(3)
        except:
            pass # doesn't really matter

        def _safe_do(f):
            try:
                f()
            except (OSError, AttributeError):
                pass
        for f in [
            lambda: self._reader.Dispose(),
            lambda: os.close(self._server_channel),
            lambda: os.close(self._server_channel),
        ]:
            _safe_do(f)
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.Dispose()

class RemoteShell:
    def __init__(self, server_path: Path, timeout=20) -> None:
        self._out_callbacks=[] # care to not reassign these
        self._err_callbacks=[] # care to not reassign these
        self._MARK=f"done_{GenerateId()}"
        self._done_stack = set()
        self._server_path, self._connect_timeout = server_path, timeout
        self._channel = self._reset()

    def _reset(self):
        def _on_push(con: PipeClient, msg: PipeResponse):
            stream = msg.data.get("stream")
            if stream not in {"out", "err"}: return
            match stream:
                case "out":
                    cb_list = self._out_callbacks
                case "err":
                    cb_list = self._err_callbacks
            content = msg.data.get("content", "")
            if content.startswith(self._MARK):
                # was echo ping from transaction
                _, k = content.split(".")
                if k in self._done_stack: self._done_stack.remove(k)
                return
            else:
                # was output from terminal
                for f in cb_list:
                    f(content)

        ws = self._server_path.parent
        start = CurrentTimeMillis()
        req_con = PipeRequest(endpoint="connect") # send the same message id
        with PipeClient(self._server_path) as p:
            while True:
                remaining_time = max(1, self._connect_timeout-(CurrentTimeMillis()-start))
                try:
                    res = p.Transact(req_con, timeout=remaining_time)
                except (ConnectionError, TimeoutError):
                    dt = (random.random()*0.3)+0.2
                    time.sleep(dt)
                    continue
                if res.status==429: # too many requests
                    dt = random.random()*1
                    time.sleep(dt)
                    if (CurrentTimeMillis()-start)*1000>self._connect_timeout:
                        raise ConnectionError(f"timeout while waiting due to 429: too many requests")
                    continue
                if res.status != 200:
                    raise ConnectionError(f"server connect error: [{res.data.get('error')}]")
                _channel_path = res.data.get("path")
                # Log.Info(f"connecting as [{channel_path.stem}]")
                if _channel_path is None:
                    raise ConnectionError("server didn't give channel path")
                channel_path = Path(ws/_channel_path)
                key = res.data.get("connection")
                if key is None:
                    raise ConnectionError("server didn't give connection key")
                # attempt connect before disposing initial starter client
                _channel = PipeClient(channel_path, key, _on_push)
                break # sucess
        return _channel

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.Dispose()
    
    def RegisterOnOut(self, callback: Callable[[str], None]):
        self._out_callbacks.append(callback)
    
    def RegisterOnErr(self, callback: Callable[[str], None]):
        self._err_callbacks.append(callback)

    def RemoveOnOut(self, callback: Callable[[str], None]):
        if callback in self._out_callbacks: self._out_callbacks.remove(callback)

    def RemoveOnErr(self, callback: Callable[[str], None]):
        if callback in self._err_callbacks: self._err_callbacks.remove(callback)

    def _send(self, cmd):
        while True:
            res = self._channel.Transact(PipeRequest(endpoint="bash", data={"script": cmd}))
            if res.status in {401}:
                self._channel.Dispose()
                self._channel = self._reset()
                continue
            if res.status in {204, 200}:
                return 
            else:
                return  res.data.get("error")

    def ExecAsync(self, cmd: str):
        err = self._send(RemoveLeadingIndent(cmd))
        _hash = GenerateId()
        self._done_stack.add(_hash)
        if err: raise ConnectionError(err)
        return _hash

    def AwaitDone(self, timeout: int|float|None=15, _hash: str|None=None):
        def _await_done(await_timeout, delta):
            start = CurrentTimeMillis()
            while True:
                if len(self._done_stack)==0: break
                if _hash is not None and _hash not in self._done_stack: break
                if CurrentTimeMillis() - start > await_timeout*1000: return False
                time.sleep(delta)
            return True

        start = CurrentTimeMillis()
        _d = 0.5
        while True:
            if len(self._done_stack)==0: break
            if _hash is not None:
                if _hash not in self._done_stack: break
                _mark = _hash
            else:
                _mark = next(iter(self._done_stack))
            err = self._send(f'echo "{self._MARK}.{_mark}"')
            if err: 
                time.sleep(0.5)
                continue
                # raise ConnectionError(err)
            else:
                if _await_done(await_timeout=_d, delta=min(_d/5, 1)): break
            # _d = min(_d*2, 864000) # 10 days
            if timeout is not None and CurrentTimeMillis() - start > timeout*1000: break
        if len(self._done_stack) > 0:
            _mark = next(iter(self._done_stack))
            self._send(f'echo "{self._MARK}.{_mark}"')

    def Exec(self, cmd: str, timeout: int|float|None=None, history: bool=False) -> ShellResult:
        _out, _err = [], []
        def _on_out(msg):
            _out.append(msg)
        def _on_err(msg):
            _err.append(msg)
        if history:
            self.RegisterOnOut(_on_out)
            self.RegisterOnErr(_on_err)
        _hash = self.ExecAsync(cmd)
        self.AwaitDone(timeout=timeout, _hash=_hash)
        if history:
            self.RemoveOnOut(_on_out)
            self.RemoveOnErr(_on_err)
        return ShellResult(out=_out, err=_err)

    def Dispose(self):
        self._channel.Dispose()
