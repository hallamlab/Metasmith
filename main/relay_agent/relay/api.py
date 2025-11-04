from pathlib import Path
from .server import ServerStatus, SERVER_HEALTH

def RunServer(workspace: Path):
    pass

def CheckStatus(workspace: Path):
    return ServerStatus()

def StopServer(workspace: Path):
    pass

def Bounce(workspace: Path, command: str):
    pass
