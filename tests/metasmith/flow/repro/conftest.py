from __future__ import annotations

import pytest


def pytest_collect_file(parent, file_path):
    if file_path.suffix != ".py":
        return None
    name = file_path.name
    if not name.startswith("repro_"):
        return None
    return pytest.Module.from_parent(parent, path=file_path)
