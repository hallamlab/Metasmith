"""Make ``fabfos`` importable for the test suite without shadowing metasmith.

``fabfos`` is not pip-installed (src-layout, no editable install in the test
envs), so ``src/`` must be on ``sys.path``. It must be *appended*, not
prepended: ``src/metasmith`` and ``src/metasmith_libraries`` are sibling
packages under the same ``src/``, and prepending puts their source copy ahead
of the real installed ``metasmith`` package, which several msm envs pin newer
than what's checked out here (see README's "ambient PYTHONPATH" gotcha).
Appending lets the installed package resolve first while still making
``fabfos`` (which has no installed copy at all) importable.
"""
from __future__ import annotations

import sys
from pathlib import Path

SRC = str(Path(__file__).resolve().parents[2] / "src")
if SRC not in sys.path:
    sys.path.append(SRC)
