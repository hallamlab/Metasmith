import sys

_DEBUG = True

# todo: use logging module
class Log:
    @classmethod
    def Info(cls, message):
        print(message)

    @classmethod
    def Debug(cls, message):
        if _DEBUG: print(message)

    @classmethod
    def Warn(cls, message):
        print(message, file=sys.stderr)

    @classmethod
    def Error(cls, message):
        print(message, file=sys.stderr)
