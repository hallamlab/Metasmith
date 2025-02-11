from pathlib import Path
from .constants import WORKSPACE_ROOT

def LinkifyPath(path: Path):
    if not path.is_absolute():
        print(f"./{path}")
        return
    
    cwd = Path("./").resolve()
    root_relative = path.relative_to(WORKSPACE_ROOT)
    backs = len(cwd.relative_to(WORKSPACE_ROOT).parents)
    print(f"./{'../'*backs}{root_relative}")
