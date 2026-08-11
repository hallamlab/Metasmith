"""Names importable from the three largest modules must stay importable.

`models/libraries.py`, `models/workflow.py` and `agents.py` are being split
into packages. `metasmith/__init__.py` is entirely commented out, so those
dotted paths *are* the public API: `python_api.py` imports `Endpoint` and
`Transform` from `models.libraries` rather than from `models.solver`, and
`coms/api.py` imports the agent-side free functions by bare name from
`agents`. Every transform file in the standard library -- a separate repo --
reaches the same paths through `python_api`.

A re-exporting `__init__.py` is supposed to make the split invisible. With
2600 lines moving there is no reviewing your way to "the re-export is
complete", so this pins the answer instead: a snapshot of every name the
modules exported before the split, checked as a **subset** of what they
export now.

Subset, not equality, because the split legitimately *adds* names -- a
package exposes its submodules as attributes. What it must never do is drop
one.

The snapshot is deliberately generous. It holds incidental imports (`shlex`,
`Callable`, `md5`) alongside the real API, because deciding which names are
"real" is exactly the judgment call that loses `ContextPath` -- or
`ResourceOverrides`, a type alias whose `__module__` is `builtins`, so every
"defined in this module" heuristic misses it.

Regenerate deliberately, never to make a red test green:

    PYTHONPATH=src python tests/unit/test_module_surface.py
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SNAPSHOT = REPO_ROOT / "tests" / "fixtures" / "module_surface.json"

# The modules being split. Recorded here rather than derived, because the
# point is to freeze these three specifically.
MODULES = (
    "metasmith.models.libraries",
    "metasmith.models.workflow",
    "metasmith.agents",
)


def _surface(module_name: str) -> set[str]:
    """Every non-dunder attribute -- see the module docstring on generosity."""
    module = importlib.import_module(module_name)
    return {n for n in dir(module) if not n.startswith("__")}


def _load_snapshot() -> dict[str, list[str]]:
    if not SNAPSHOT.is_file():
        pytest.fail(f"missing surface snapshot at {SNAPSHOT}; regenerate it")
    return json.loads(SNAPSHOT.read_text(encoding="utf-8"))["modules"]


def test_snapshot_covers_every_split_module():
    """A module dropped from the snapshot is a gate that stopped gating."""
    assert set(_load_snapshot()) == set(MODULES)


@pytest.mark.parametrize("module_name", MODULES)
def test_exported_names_survive(module_name):
    expected = set(_load_snapshot()[module_name])
    actual = _surface(module_name)
    missing = sorted(expected - actual)
    assert not missing, (
        f"{module_name} no longer exports {len(missing)} name(s) it exported "
        f"before the split:\n  " + "\n  ".join(missing)
        + "\n\nRe-export them from the package __init__. If a name was removed "
        "on purpose, say so in the commit and regenerate the snapshot."
    )


def _regenerate() -> None:
    payload = {
        "_note": (
            "Public surface of the three modules split in the 2026-07 god-file "
            "refactor, captured at f0378bf (pre-split). Checked as a subset by "
            "tests/unit/test_module_surface.py -- see that file's docstring."
        ),
        "modules": {name: sorted(_surface(name)) for name in MODULES},
    }
    SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    total = sum(len(v) for v in payload["modules"].values())
    print(f"wrote {SNAPSHOT} ({total} names across {len(MODULES)} modules)")


if __name__ == "__main__":
    _regenerate()
