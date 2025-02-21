import sys
from .serialization import StdTime
from pathlib import Path

_DEBUG = True
_log_path: Path = None
def _log(*args, **kwargs):
    if _log_path is not None:
        if kwargs.get("file") == sys.stderr:
            log_file = _log_path.with_suffix(".err")
        else:
            log_file = _log_path.with_suffix(".out")
        if "file" in kwargs: del kwargs["file"]
        with open(log_file, "a") as f:
            print(*args, **kwargs, file=f, flush=True)
    else:
        print(*args, **kwargs, flush=True)

# todo: use logging module
class Log:
    @classmethod
    def SetLogFile(cls, file_path: Path):
        global _log_path
        _log_path = file_path

    @classmethod
    def Info(cls, message):
        line = f"{StdTime.Timestamp()}  | {message}"
        _log(line)

    @classmethod
    def Debug(cls, message):
        line = f"{StdTime.Timestamp()} D| {message}"
        if _DEBUG: _log(line)

    @classmethod
    def Warn(cls, message):
        line = f"{StdTime.Timestamp()} W| {message}"
        _log(line, file=sys.stderr)

    @classmethod
    def Error(cls, message):
        line = f"{StdTime.Timestamp()} E| {message}"
        _log(line, file=sys.stderr)
