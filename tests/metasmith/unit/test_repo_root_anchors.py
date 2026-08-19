from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]

ROOT_NAMES = {"REPO_ROOT", "_REPO_ROOT", "ROOT", "_ROOT"}

_ASSIGN = re.compile(
    r"^\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*"
    r"Path\(__file__\)\.resolve\(\)\.parents\[(?P<n>\d+)\]",
    re.M,
)


def _candidates():
    listed = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "ls-files", "-z", "tests"],
        capture_output=True, text=True, check=True,
    ).stdout.split("\0")
    for name in listed:
        if not name or not name.endswith(".py"):
            continue
        p = REPO_ROOT / name
        try:
            text = p.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for m in _ASSIGN.finditer(text):
            if m.group("name") in ROOT_NAMES:
                yield Path(name), int(m.group("n"))


def test_repo_root_anchors_resolve_to_the_repo_root():
    checked = 0
    wrong = []
    for rel, n in _candidates():
        checked += 1
        resolved = (REPO_ROOT / rel).resolve().parents[n]
        if resolved != REPO_ROOT:
            correct = len(rel.parts) - 1
            wrong.append(f"{rel}: parents[{n}] -> {resolved} (should be parents[{correct}])")
    assert checked, "found no repo-root anchors to check -- has the pattern changed?"
    assert not wrong, "these anchors no longer reach the repo root:\n  " + "\n  ".join(wrong)
