import sys
from .serialization import StdTime

_DEBUG = True

# todo: use logging module
class Log:
    @classmethod
    def Info(cls, message):
        line = f"{StdTime.Timestamp()}  |{message}"
        print(line, flush=True)

    @classmethod
    def Debug(cls, message):
        line = f"{StdTime.Timestamp()} D|{message}"
        if _DEBUG: print(line, flush=True)

    @classmethod
    def Warn(cls, message):
        line = f"{StdTime.Timestamp()} W|{message}"
        print(line, file=sys.stderr, flush=True)

    @classmethod
    def Error(cls, message):
        line = f"{StdTime.Timestamp()} E|{message}"
        print(line, file=sys.stderr, flush=True)
