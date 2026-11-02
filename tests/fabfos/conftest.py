from __future__ import annotations

import sys
from pathlib import Path

SRC = str(Path(__file__).resolve().parents[2] / "src")
if SRC not in sys.path:
    sys.path.append(SRC)
