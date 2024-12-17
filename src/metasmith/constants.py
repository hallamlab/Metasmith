from pathlib import Path

NAME = Path(__file__).parent.name
GIT_URL = "https://github.com/Tony-xy-Liu/Metasmith"

with open(Path(__file__).parent/"version.txt") as f:
    VERSION = f.read().strip()
