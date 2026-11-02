from __future__ import annotations

import sys
import threading
from pathlib import Path

import pytest

from metasmith.models.libraries import TransformInstanceLibrary

pytestmark = pytest.mark.fast

EXAMPLES = Path(__file__).resolve().parents[3] / "src" / "metasmith" / "examples"


def _load_all():
    lib = TransformInstanceLibrary.Load(EXAMPLES)
    for _ in range(3):
        for key, _dtype_name, _dtype in lib.Iterate():
            lib.GetTransform(key, reload=True)


def test_sequential_load_restores_sys_path():
    before = list(sys.path)
    _load_all()
    assert sys.path == before


def test_concurrent_loads_do_not_grow_sys_path():
    before = len(sys.path)
    errors: list[BaseException] = []

    def work():
        try:
            _load_all()
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=work) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    assert len(sys.path) == before
