from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
SNAPSHOT = REPO_ROOT / "tests" / "metasmith" / "fixtures" / "module_surface.json"

MODULES = (
    "metasmith.models.libraries",
    "metasmith.models.workflow",
    "metasmith.agents",
)


def _surface(module_name: str) -> set[str]:
    module = importlib.import_module(module_name)
    return {n for n in dir(module) if not n.startswith("__")}


def _load_snapshot() -> dict[str, list[str]]:
    if not SNAPSHOT.is_file():
        pytest.fail(f"missing surface snapshot at {SNAPSHOT}; regenerate it")
    return json.loads(SNAPSHOT.read_text(encoding="utf-8"))["modules"]


def test_snapshot_covers_every_split_module():
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
            "tests/metasmith/unit/test_module_surface.py -- see that file's docstring."
        ),
        "modules": {name: sorted(_surface(name)) for name in MODULES},
    }
    SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    total = sum(len(v) for v in payload["modules"].values())
    print(f"wrote {SNAPSHOT} ({total} names across {len(MODULES)} modules)")


if __name__ == "__main__":
    _regenerate()
