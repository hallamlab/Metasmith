"""Importing a transform must leave `sys.path` exactly as it found it.

`TransformInstance.Load` puts the transform's own directory at the front of
`sys.path` so a bare `__import__` finds it, and takes it off again on the way
out. It used to do that by snapshotting the whole list and rebinding it, which
is correct for one thread and silently wrong for two: the second snapshot
already holds the first's entry, so restoring it puts that entry back forever.

Nothing fails when it happens. Every later import just scans more directories,
so the symptom is a process that gets slower at *planning* and at nothing else
-- measured at 0.4s to 9s per solve on a GUI server that had raced once. That
is why this is pinned by the length of a list rather than by an exception.
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path

import pytest

from metasmith.models.libraries import TransformInstanceLibrary

pytestmark = pytest.mark.fast

EXAMPLES = Path(__file__).resolve().parents[3] / "src" / "metasmith" / "examples"


def _load_all():
    # `reload=True` on every pass: the cache is what a second look would hit,
    # and it is the import underneath it that touches `sys.path`
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
        except BaseException as e:  # a raise here would hide the leak below
            errors.append(e)

    threads = [threading.Thread(target=work) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    assert len(sys.path) == before
