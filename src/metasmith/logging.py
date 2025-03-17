import sys
from pathlib import Path
import logging

from .serialization import StdTime

DEFAULT_CONFIG = dict(
    datefmt=StdTime.FORMAT,
    level=logging.DEBUG,
    encoding="latin1",
)
logging.basicConfig(**DEFAULT_CONFIG)

for level, name in [
    (logging.INFO,      " "),
    (logging.DEBUG,     "D"),
    (logging.WARNING,   "W"),
    (logging.ERROR,     "E"),
    (logging.CRITICAL,  "!"),
]:
    logging.addLevelName(level, name)

class ConditionalFormatter(logging.Formatter):
    def __init__(self, fmt_with_timestamp, fmt_without_timestamp):
        super().__init__()
        self.fmt_with_timestamp = fmt_with_timestamp
        self.fmt_without_timestamp = fmt_without_timestamp
        self.datefmt = StdTime.FORMAT

    def format(self, record):
        if getattr(record, 'include_timestamp', False):
            self._style._fmt = self.fmt_with_timestamp
        else:
            self._style._fmt = self.fmt_without_timestamp
        return super().format(record)
    
class InfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno < logging.ERROR

_formatter = ConditionalFormatter(
    "%(asctime)s %(levelname)s| %(message)s",
    "%(levelname)s| %(message)s",
)
_handler = logging.StreamHandler(stream=sys.stdout)
_handler.setFormatter(_formatter)
_handler.addFilter(InfoFilter())
_handler_err = logging.StreamHandler(stream=sys.stderr)
_handler_err.setLevel(logging.ERROR)
_handler_err.setFormatter(_formatter)
_logger = logging.getLogger()
_logger.setLevel(logging.DEBUG)
_logger.handlers.clear()
_logger.addHandler(_handler)
_logger.addHandler(_handler_err)

_file_handlers: dict[Path, logging.FileHandler] = {}
class Log:
    @classmethod
    def AddLogFile(cls, file_path: Path):
        _file_handler = logging.FileHandler(file_path)
        _file_handlers[file_path] = _file_handler
        _file_handler.setFormatter(_formatter)
        _logger.addHandler(_file_handler)
    
    @classmethod
    def RemoveLogFile(cls, file_path: Path):
        if file_path not in _file_handlers:
            return
        _file_handler = _file_handlers[file_path]
        _logger.removeHandler(_file_handler)
        _file_handler.close()

    @classmethod
    def Info(cls, message, timestamp=True):
        _logger.info(message, extra=dict(include_timestamp=timestamp))

    @classmethod
    def Debug(cls, message):
        _logger.debug(message)

    @classmethod
    def Warn(cls, message):
        _logger.warning(message)

    @classmethod
    def Error(cls, message, timestamp=True):
        _logger.error(message, extra=dict(include_timestamp=timestamp))
