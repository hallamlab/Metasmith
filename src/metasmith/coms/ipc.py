from __future__ import annotations
import os
import re
from pathlib import Path
from typing import IO, Callable
from threading import Condition, Thread
from dataclasses import dataclass, field
import json
import subprocess
import select
import pty
import time
import random

from ..hashing import KeyGenerator
from ..serialization import StdTime
from ..logging import Log

def CurrentTimeMillis():
    return StdTime.CurrentTimeMillis()

# removes: colors, escape, control sequences
# https://stackoverflow.com/questions/14693701/how-can-i-remove-the-ansi-escape-sequences-from-a-string-in-python 
def StripANSI(s: str):
    return re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])').sub('', s)

def RemoveTrailingNewline(s):
    while len(s) > 0 and s[-1] in {"\n", "\r"}:
        s = s[:-1]
    return s

def RemoveLeadingIndent(s: str):
    lines = s.split("\n")
    if len(lines) == 0: return s
    indent = 0
    for line in lines:
        if line == "": continue
        for c in line:
            if c not in {" ", "\t"}: break
            indent += 1
        break
    cleaned = "\n".join([l[indent:] for l in lines])
    cleaned = cleaned.strip()
    if lines[-1][indent:] == "": cleaned += "\n"
    return cleaned

_kg = KeyGenerator()
def GenerateId():
    return _kg.GenerateUID(12)

class ConnectionError(Exception):
    pass

@dataclass
class IpcModel:
    message_id: str = field(default_factory=GenerateId, kw_only=True)
    parse_error: str | None = field(default=None, kw_only=True)

    @classmethod
    def Parse(cls, raw: str):
        try:
            d = json.loads(raw)
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
            return IpcModel(parse_error=emsg)
        except json.JSONDecodeError as e:
            return IpcModel(parse_error="invalid json")

    def IsValid(self):
        return self.parse_error is None

    def Serialize(self):
        bl = {"parse_error"}
        should_serialize = lambda k, v: not k.startswith("_") and not callable(v) and k not in bl
        d = {k:v for k, v in self.__dict__.items() if should_serialize(k, v)}
        return json.dumps(d)

@dataclass
class IpcRequest(IpcModel):
    endpoint: str
    data: dict = field(default_factory=dict)

@dataclass
class IpcResponse(IpcModel):
    status: int
    data: dict = field(default_factory=dict)

class PipeServer:
    def __init__(self, io_dir: Path, callback: Callable[[PipeServer, str], None], overwrite: bool = False, id: str = None) -> None:
        success = False
        try:
            self._id = "main" if id is None else id
            self._server_path = io_dir/f"{self._id}.in"
            self._client_path = io_dir/f"{self._id}.out"
            if overwrite and self._server_path.exists(): os.remove(self._server_path)
            self._is_closing = False
            self._lock = Condition()
            
            os.mkfifo(self._server_path)
            def _open():
                return os.open(self._server_path, os.O_RDONLY|os.O_NONBLOCK)
            self._server_channel = _open()
            # def _reset():
            #     if self._is_closing: return
            #     self._server_channel = _open()
            #     # reader.Reset(self._server_channel)
            # _reset()
            def _on_close(x):
                with self._lock:
                    self._is_closing=True
            self.reader = NonBlockingReader(self._server_channel, on_close=_on_close)
            self.reader.RegisterCallback(lambda x: callback(self, RemoveTrailingNewline(x.decode())))
            
            self._client_channel: int|None = None
            self._buffer: list[str] = []
            def prep_channel():
                if not self._client_path.exists(): 
                    if self._client_channel is not None:
                        os.close(self._client_channel)
                        self._client_channel = None
                    return False
                if self._client_channel is None:
                    try:
                        self._client_channel = os.open(self._client_path, os.O_WRONLY)
                    except FileNotFoundError: # race conditioned
                        return False
                return True

            def try_send():
                if not prep_channel(): return
                retries = 2
                while len(self._buffer) > 0 and retries > 0:
                    msg = self._buffer.pop(0)
                    try:
                        if self._client_channel is None: return
                        os.write(self._client_channel, (msg+"\n").encode())
                    except OSError:
                        self._buffer.insert(0, msg)
                        # the channel is likely pointing to the previous client
                        # which is now closed, so we need to get a new fd for pipe
                        self._client_channel = None
                        prep_channel()
                        retries -= 1
            
            def sender_process():
                while True:
                    with self._lock:
                        if self._is_closing: break
                        if len(self._buffer) == 0:
                            self._lock.wait(1)
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
            try:
                os.close(fd)
            except OSError:
                pass
        try:
            with self._lock:
                self._is_closing = True
                self._lock.notify_all()
            self.reader.Dispose()
            _try_close(self._server_channel)
            self._sender.join()
        except AttributeError:
            pass
        finally:
            if self._server_path.exists(): os.remove(self._server_path)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.Dispose()

class PipeClient:
    def __init__(self, server_pipe: Path, timeout: int|float = 60) -> None:
        success = False
        try:
            self._id = server_pipe.stem
            io_dir = server_pipe.parent
            self._server_path = io_dir/f"{self._id}.in"
            self._client_path = io_dir/f"{self._id}.out"
            self._server_channel=-1
            self._lock = Condition()
            self._closed = False

            start = CurrentTimeMillis()
            def _on_close(reader: NonBlockingReader):
                with self._lock:
                    self._closed = True
            while True:
                if not self._client_path.exists():
                    try:
                        os.mkfifo(self._client_path)
                        break
                    except FileExistsError:
                        pass # race condition between processes possible
                delay = random.random()*0.1
                time.sleep(delay)
                if CurrentTimeMillis() - start > timeout*1000:
                    raise TimeoutError("Failed to connect to server")

            try:
                self._server_channel = os.open(self._server_path, os.O_WRONLY|os.O_NONBLOCK)
            except (FileNotFoundError, OSError):
                raise ConnectionError("server not found")
            self._client_channel = os.open(self._client_path, os.O_RDONLY|os.O_NONBLOCK)
            self._reader = NonBlockingReader(self._client_channel, on_close=_on_close)
            # self._reader = NonBlockingReader(self._client_channel)
            
            self._last_message_id: str = None
            self._last_response: IpcResponse|None = None
            def _on_response(x):
                res = IpcResponse.Parse(x.decode())
                if res.message_id != self._last_message_id: return
                self._last_response = res
            self._reader.RegisterCallback(_on_response)
            success = True

            def _keep_alive():
                while True:
                    time.sleep(1)
                    with self._lock:
                        if self._closed: return
                        try:
                            self.Send(IpcRequest("ping").Serialize())
                        except:
                            pass
            self._keep_alive_worker = Thread(target=_keep_alive)
            self._keep_alive_worker.start()
        finally:
            if not success: self.Dispose()

    def Send(self, msg: str):
        if self._closed: return
        os.write(self._server_channel, (msg+"\n").encode())

    def Transact(self, req: IpcRequest, timeout: int|float|None = 15) -> IpcResponse:
        self._last_response = None
        self._last_message_id = req.message_id
        msg = req.Serialize()
        self.Send(msg)
        start = CurrentTimeMillis()
        while self._last_response is None:
            if timeout and CurrentTimeMillis() - start > timeout*1000:
                raise TimeoutError("Failed to receive response")
            with self._lock:
                self._lock.wait(0.1)
                if self._closed:
                    return IpcResponse(500, dict(error="connection closed"))
        return self._last_response

    def IsOpen(self):
        return self._client_path.exists()

    def Dispose(self):
        try:
            self.Send(IpcRequest("disconnect").Serialize())
        except OSError:
            pass
        def _try_close(fd):
            try:
                os.close(fd)
            except OSError:
                pass
        with self._lock:
            self._closed = True
        try:
            self._keep_alive_worker.join(3)
        except:
            pass # doesn't really matter
        try:
            self._reader.Dispose()
            if self._server_channel>=0: _try_close(self._server_channel)
            _try_close(self._client_channel)
        except AttributeError:
            pass
        finally:
            if self._client_path.exists():
                try:
                    os.remove(self._client_path)
                except FileNotFoundError:
                    pass # race conditioned between check and os.remove probably

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.Dispose()

MAX_READERS = 256
_readers = set()
class NonBlockingReader:
    def __init__(self, io_handle: int, on_close: Callable[[NonBlockingReader], None] = None, sep: bytes = b"\n") -> None:
        self._callbacks = []
        self._lock = Condition()
        self._notify_out, self._notify_in = os.pipe() # https://stackoverflow.com/a/57341500/13690762
        self._on_close = on_close
        self._sep = sep
        self._worker = None
        self._is_closed = False
        self._io_handle = io_handle
        self._start(io_handle)

    def _start(self, io_handle: int):
        if len(_readers)>MAX_READERS:
            raise ConnectionError("too many readers")
        _readers.add(self)

        def reader(fd: int, callbacks: list[Callable[[bytes], None]]):
            _buffer = []
            def _try_read():
                nonlocal _buffer
                changed = False
                while True:
                    # https://stackoverflow.com/a/21429655/13690762
                    r, _, _ = select.select([ fd, self._notify_out ], [], [], 60) # allows unblock with notify_out
                    # Log.Debug(f"r* [{id(self)}] [closed: {self.IsClosed()}] [notified: {self._notify_out in r}] ")
                    if self.IsClosed():
                        return []
                    chunk = os.read(fd, 4096)
                    if len(chunk) == 0: continue
                    _buffer.append(chunk)
                    changed = True
                    if self._sep in chunk: break # line complete
                if not changed: return []

                complete_segments = []
                remainder = []
                lines = b''.join(_buffer).split(self._sep)
                for i, line in enumerate(lines):
                    if i < len(lines)-1:
                        complete_segments.append(line)
                    else: # last chunk
                        if len(line) > 0: remainder.append(line) # save for later if incomplete
                _buffer.clear()
                _buffer.extend(remainder)
                return complete_segments

            initial_wait, max_wait = 0.1, 600
            clear = True
            wait = 0.1
            def reset_wait():
                nonlocal wait, clear
                wait = initial_wait
                clear = True
            
            def scaling_wait():
                nonlocal wait, clear
                with self._lock:
                    self._lock.wait(wait)
                if not clear:
                    wait = min(wait*2, max_wait)
                clear = False

            while not self.IsClosed():
                try:
                    lines = list(_try_read())
                    for line in lines:
                        # Log.Debug(f"--- {line}")
                        for cb in callbacks: cb(line)
                    reset_wait()
                except OSError as e: # fd closed
                    if e.errno == 9: # Bad file descriptor
                        break
                    else: # likely a race condition
                        scaling_wait()
            if callable(self._on_close): self._on_close(self)
            # Log.Debug(f"> close! [{self._is_closed}]")

        # with self._lock:
            # if not self._is_closed: return # already stopped
        self._worker = Thread(target=reader, args=[io_handle, self._callbacks])
        self._worker.start()

    def RegisterCallback(self, callback: Callable[[bytes], None]):
        self._callbacks.append(callback)

    def RemoveCallback(self, callback: Callable[[bytes], None]):
        self._callbacks.remove(callback)

    def IsClosed(self):
        with self._lock:
            return self._is_closed

    def Dispose(self):
        try:
            while True:
                with self._lock:
                    self._is_closed = True

                if self._worker is None: break
                if not self._worker.is_alive(): break
                try:
                    os.write(self._notify_in, b"dispose") # unblock reader
                except OSError:
                    pass
                
                try:
                    self._worker.join(1)
                except RuntimeError as e:
                    Log.Error(f"NonBlockingReader.Dispose() [{e}]")

            for fd in [self._notify_in, self._notify_out]:
                try:
                    os.close(fd)
                except OSError as e:
                    pass
                #     Log.Error(f"NonBlockingReader.Dispose() fd:{fd} [{e}]")
        finally:
            _readers.remove(self)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.Dispose()

class TerminalProcess:
    class Pipe:
        def __init__(self, io:IO[bytes], lock: Condition = None) -> None:
            self.IO = io
            if lock is None: lock = Condition()
            self.Lock = lock

        def __enter__(self):
            self.Lock.acquire()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.Lock.release()

    def __init__(self) -> None:
        # https://stackoverflow.com/questions/41542960/run-interactive-bash-with-popen-and-a-dedicated-tty-python
        out_master, out_slave = pty.openpty()
        err_master, err_slave = pty.openpty()
        self._fds = [out_master, err_master, out_slave, err_slave]

        console = subprocess.Popen(
            ["bash"],
            stdin=subprocess.PIPE,
            stdout=out_slave,
            stderr=err_slave,
            close_fds=True,
            start_new_session=True, # nextflow needs this
        )

        self.ENCODING = "utf-8"
        self._console = console
        self._in = TerminalProcess.Pipe(console.stdin)
        self._onCloseLock = Condition()
        self._closed = False
        self.pid = console.pid
        self._err_reader = NonBlockingReader(err_master)
        self._out_reader = NonBlockingReader(out_master)

    def Send(self, payload: bytes):
        stdin = self._in
        with self._in:
            stdin.IO.write(payload)
            stdin.IO.flush()
    
    def Decode(self, payload: bytes):
        return payload.decode(encoding=self.ENCODING)

    def Write(self, msg: str):
        self.Send(bytes('%s\n' % (msg), encoding=self.ENCODING))

    def RegisterOnOut(self, callback: Callable[[bytes], None]):
        self._out_reader.RegisterCallback(callback)

    def RegisterOnErr(self, callback: Callable[[bytes], None]):
        self._err_reader.RegisterCallback(callback)

    def RemoveOnOut(self, callback: Callable[[bytes], None]):
        self._out_reader.RemoveCallback(callback)

    def RemoveOnErr(self, callback: Callable[[bytes], None]):
        self._err_reader.RemoveCallback(callback)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.Dispose()
        return

    def Dispose(self):
        self._err_reader.Dispose()
        self._out_reader.Dispose()
        self._console.terminate()
        for i, fd in enumerate(self._fds):
            try:
                os.close(fd)
            except OSError as e:
                Log.Error(f"TerminalProcess.Dispose() fd:{i} [{e}]")

@dataclass
class ShellResult:
    out: list[str]
    err: list[str]

class LiveShell:
    def __init__(self) -> None:
        self._MARK = f"done_{GenerateId()}"
        self._done_stack = set()
        self._err_callbacks = []
        self._out_callbacks = []

        self._shell = TerminalProcess()
        def _tee(cb_lst: list[Callable[[str], None]], check=False):
            def _cb(x):
                msg = RemoveTrailingNewline(self._shell.Decode(x))
                if len(msg) == 0: return
                if msg.startswith(self._MARK):
                    _, k = msg.split(".")
                    if k in self._done_stack: self._done_stack.remove(k)
                    return
                for f in cb_lst: f(msg)
            return _cb
        self._shell.RegisterOnErr(_tee(self._err_callbacks))
        self._shell.RegisterOnOut(_tee(self._out_callbacks, check=True))
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.Dispose()

    def Dispose(self):
        if self._shell is None: return
        self._shell.Dispose()
        self._shell = None
        self._err_callbacks.clear()
        self._out_callbacks.clear()

    def RegisterOnOut(self, callback: Callable[[str], None]):
        self._out_callbacks.append(callback)
    
    def RegisterOnErr(self, callback: Callable[[str], None]):
        self._err_callbacks.append(callback)

    def RemoveOnOut(self, callback: Callable[[str], None]):
        if callback in self._out_callbacks: self._out_callbacks.remove(callback)

    def RemoveOnErr(self, callback: Callable[[str], None]):
        if callback in self._err_callbacks: self._err_callbacks.remove(callback)

    def ExecAsync(self, cmd: str):
        _hash = GenerateId()
        self._done_stack.add(_hash)
        self._shell.Write(RemoveLeadingIndent(cmd))
        return _hash

    def AwaitDone(self, timeout: int|float = 15, _hash: str=None):
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
            try:
                if len(self._done_stack)==0: break
                if _hash is not None:
                    if _hash not in self._done_stack: break
                    _mark = _hash
                else:
                    _mark = next(iter(self._done_stack))
                self._shell.Write(f'echo "{self._MARK}.{_mark}"')
            except BrokenPipeError: break
            if _await_done(await_timeout=_d, delta=min(_d/5, 1)): break
            # _d = min(_d*2, 864000) # 10 days
            if timeout is not None and CurrentTimeMillis() - start > timeout*1000: break
        if len(self._done_stack) > 0:
                _mark = next(iter(self._done_stack))
                self._shell.Write(f'echo "{self._MARK}.{_mark}"')

    def Exec(self, cmd: str, timeout: int|None = 15, history: bool=False) -> ShellResult:
        _out, _err = [], []
        def _log_err(msg):
            _err.append(msg)
        def _log_out(msg):
            _out.append(msg)
        if history:
            self.RegisterOnOut(_log_out)
            self.RegisterOnErr(_log_err)

        _hash = self.ExecAsync(cmd)
        self.AwaitDone(timeout=timeout, _hash=_hash)
        if history:
            self.RemoveOnOut(_log_out)
            self.RemoveOnErr(_log_err)
        return ShellResult(out=_out, err=_err)

class RemoteShell:
    def __init__(self, server_path: Path, timeout=60) -> None:
        ws = server_path.parent
        start = CurrentTimeMillis()
        while True:
            with PipeClient(server_path) as p:
                remaining_time = max(1, timeout-(CurrentTimeMillis()-start))
                res = p.Transact(IpcRequest(endpoint="connect"), timeout=remaining_time)
            if res.status==429: # too many requests
                time.sleep(2)
                if (CurrentTimeMillis()-start)*1000>timeout:
                    raise ConnectionError(f"timeout while waiting due to 429: too many requests")
            else:
                break
        if res.status != 200:
            raise ConnectionError(f"server connect error: [{res.data.get('error')}]")

        channel_path = Path(res.data.get("path"))
        # Log.Info(f"connecting as [{channel_path.stem}]")
        if channel_path is None:
            raise ConnectionError("server didn't give channel path")
        channel_path = ws/channel_path
        out_cb, err_cb = [], []
        MARK = f"done_{GenerateId()}"
        self._done_stack = set()
        def _make_callback(callback_list: list):
            def _handler(channel: PipeServer, raw: str):
                if raw.startswith(MARK):
                    _, k = raw.split(".")
                    if k in self._done_stack: self._done_stack.remove(k)
                    return
                for f in callback_list:
                    f(raw)
            return _handler

        k = channel_path.stem
        live_out = PipeServer(ws, _make_callback(out_cb), id=k+".bash_out")
        live_err = PipeServer(ws, _make_callback(err_cb), id=k+".bash_err")
        channel = PipeClient(channel_path)

        err = None
        for stream, path in [
            ("out", live_out._server_path.name),
            ("err", live_err._server_path.name),
        ]:
            res = channel.Transact(IpcRequest(endpoint="register_bash_listener", data=dict(
                stream=stream,
                channel=str(path),
            )))
            if res.status != 200:
                err = f"failed to connect [{stream}] listener for shell [{res.data.get('error')}]"
                break
        if err:
            live_out.Dispose()
            live_err.Dispose()
            channel.Dispose()
            raise ConnectionError(err)
        
        self._err_pipe=live_err
        self._out_pipe=live_out
        self._channel=channel
        self._out_callbacks=out_cb # care to not reassign these
        self._err_callbacks=err_cb # care to not reassign these
        self._MARK=MARK

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
        res = self._channel.Transact(IpcRequest(endpoint="bash", data={"script": cmd}))
        if res.status not in {204, 200}:
            return res.data.get("error")
        else:
            return

    def ExecAsync(self, cmd: str):
        err = self._send(RemoveLeadingIndent(cmd))
        _hash = GenerateId()
        self._done_stack.add(_hash)
        if err: raise ConnectionError(err)
        return _hash

    def AwaitDone(self, timeout: int|float=15, _hash: str=None):
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
            if err: raise ConnectionError(err)
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
        self._err_pipe.Dispose()
        self._out_pipe.Dispose()
        self._channel.Dispose()
