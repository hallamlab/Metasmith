from enum import Enum
from pathlib import Path
import os

EXECUTION_DIR = Path(os.getcwd())
WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
