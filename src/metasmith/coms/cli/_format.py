"""Output formatting helpers for the CLI."""
from __future__ import annotations

import json
import sys
from typing import Any

from ...logging import Log


def emit(args, value: Any) -> None:
    """Print a result, honoring --json / --quiet."""
    if getattr(args, "quiet", False):
        return
    if getattr(args, "json", False):
        print(json.dumps(value, indent=2, default=str))
        return
    _pretty(value)


def _pretty(value: Any, indent: int = 0) -> None:
    pad = "  " * indent
    if value is None:
        print(f"{pad}(none)")
    elif isinstance(value, (str, int, float, bool)):
        print(f"{pad}{value}")
    elif isinstance(value, list):
        if not value:
            print(f"{pad}(empty)")
            return
        # list of dicts → table-ish; otherwise one per line
        if all(isinstance(x, dict) for x in value):
            for i, x in enumerate(value):
                if i > 0:
                    print()
                _pretty(x, indent)
        else:
            for x in value:
                print(f"{pad}- {x}")
    elif isinstance(value, dict):
        for k, v in value.items():
            if isinstance(v, (dict, list)) and v:
                print(f"{pad}{k}:")
                _pretty(v, indent + 1)
            else:
                pv = v if not isinstance(v, (dict, list)) else "(empty)"
                print(f"{pad}{k}: {pv}")
    else:
        print(f"{pad}{value}")


def die(message: str, code: int = 1) -> None:
    """Print an error to stderr and exit non-zero."""
    Log.Error(message)
    sys.exit(code)
