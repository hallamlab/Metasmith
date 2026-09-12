#!/usr/bin/env python3
"""The standard library's authoring API, re-rooted on this library.

`src/metasmith_libraries/_authoring.py` anchors every path on its own directory,
so importing it gives helpers pointed at the standard library. Every one of them
reads `MLIB` at call time, which is what lets this module rebind the root instead
of copying sixty lines that would then drift.

Import this rather than the original from anything authoring a KBase template.
"""

from __future__ import annotations

import sys
from pathlib import Path

from metasmith_libraries import _authoring as _std

MLIB = Path(__file__).resolve().parent
TYPES = MLIB / "data_types"

_std.MLIB = MLIB
_std.TYPES = TYPES

transforms = _std.transforms
envs = _std.envs
deferred_inputs = _std.deferred_inputs
author = _std.author
cli = _std.cli
