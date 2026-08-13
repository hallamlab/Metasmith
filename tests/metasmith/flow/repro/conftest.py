"""Allow pytest to collect `repro_*.py` files in this directory.

The global `pyproject.toml` restricts `python_files` to `test_*.py`, but
the flow-catalog convention names trap-case pins `repro_<id>_<slug>.py`
(see `tests/flow/AGENTS.md`). We extend collection locally so the pins
that contain `test_*` functions are picked up while keeping the global
collection pattern unchanged.
"""

from __future__ import annotations

import pytest


def pytest_collect_file(parent, file_path):
    if file_path.suffix != ".py":
        return None
    name = file_path.name
    if not name.startswith("repro_"):
        return None
    # Defer to the default Python module collector with the file path —
    # pytest's Module.from_parent picks it up the same way it would a
    # `test_*.py` match.
    return pytest.Module.from_parent(parent, path=file_path)
