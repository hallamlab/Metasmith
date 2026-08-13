"""`parents[N]` anchors must still land on the repo root after a file moves.

`Path(__file__).resolve().parents[N]` encodes the file's depth in the tree
as a number. Move the file one directory deeper and the constant is
silently wrong -- it now points at `tests/`, every path built from it is
missing, and the failure surfaces as a `FileNotFoundError` on a fixture
rather than as anything about layout. Two files broke exactly this way in
the 2026-07 reshape, and neither is in a suite that runs without a docker
daemon, so both were green until the daemon was there.

This is a static check: find the `parents[N]` expressions that are meant
to name the repo root, and assert each one still does from where its file
now lives.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]

# The name a repo-root anchor is bound to. An expression assigned to
# something else (SRC_ROOT, a scenarios dir, a template dir) is anchoring on
# something other than the root and is none of this test's business.
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
