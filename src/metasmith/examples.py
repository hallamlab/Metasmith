from pathlib import Path
import shutil

from .logging import Log

def _prep(path: Path|str, on_exist: str = "skip"):
    path = Path(path)
    assert on_exist in {"skip", "error", "clear", "update"}
    if path.exists():
        if on_exist == "skip":
            Log.Warn(f'destination exists: [{path}]')
            return False
        elif on_exist == "error":
            raise FileExistsError(f'destination exists: [{path}]')
        elif on_exist == "clear":
            Log.Warn(f'deleting previous [{path}]')
            shutil.rmtree(path)
        elif on_exist == "update":
            Log.Warn(f'updating [{path}]')
    return path

def GenerateContigs(path: Path|str, on_exist: str = "skip"):
    path = _prep(path, on_exist)
    if not path: return # skip
    

def GenerateReferences(path: Path|str):
    path = Path(path)
    for file in path.glob('*.fasta'):
        print(file)
