from gevent import monkey
monkey.patch_all()
from gevent.pywsgi import WSGIServer
# import gunicorn.app.base
from flask import Flask
import socketio
from pathlib import Path
from threading import Condition, Thread
from dataclasses import dataclass
import os
import signal
import time
import numpy as np

from ..logging import logging

@dataclass
class WsServerApp:
    app: Flask

class WsServer:
    def __init__(self, app: WsServerApp, workspace: Path):
        def make_logger(name):
            logger = logging.getLogger(__file__+name)
            logger.handlers.clear()
            log_path = workspace/name
            file_handler = logging.FileHandler(log_path)
            logger.addHandler(file_handler)
            logger.propagate = False
            return logger
        self._lock = Condition()
        self._disposed = False
        self._worker = None
        def graceful_shutdown(signum, frame):
            with self._lock:
                self._disposed = True
            if self._worker is None: return
            self._worker.stop(timeout=1)
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, graceful_shutdown)
        signal.signal(signal.SIGINT, graceful_shutdown)  # For Ctrl+C
        
        def _run():
            # https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml
            seen = set()
            port_file = None
            while True:
                while True:
                    port = np.random.randint(49152, 65535)
                    if port in seen: continue
                    seen.add(port)
                    port_file = workspace/f"ws_port.{port}.lock"
                    port_file.touch(0o066)
                    break
                with self._lock:
                    if self._disposed: break
                try:
                    self._worker = WSGIServer(
                        ('localhost', port),
                        app.app,
                        log=make_logger("main.log"),
                        error_log=make_logger("main.err"),
                    )
                    self._worker.serve_forever()
                    break
                except OSError as e:
                    s = str(e)
                    if s.startswith("Address already in use:"):
                        continue
                    else:
                        raise e
                finally:
                    if port_file is not None: os.remove(port_file)
        self._worker_thread = Thread(target=_run)
        self._worker_thread.start()

        def _monitor():
            while True:
                with self._lock:
                    if self._disposed: break
                    self._lock.wait(1)
        self._monitor_thread = Thread(target=_monitor)
        self._monitor_thread.start()

    def Dispose(self):
        with self._lock:
            self._disposed = True
            self._lock.notify_all()
        if self._worker is None:
            return
        self._worker.stop()
        self._worker.close()
        self._worker_thread.join(timeout=5)
        self._monitor_thread.join(timeout=1)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.Dispose()
    

# class WsClient:
#     def __init__(self, port: int=DEFAULT_PORT) -> None:
#         pass