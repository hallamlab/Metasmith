from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]

RENAMED = {"ContainerRuntime": "Runtime"}

SEARCH_ROOTS = ("src", "docs", "research", "tests")
SUFFIXES = {".py", ".ipynb", ".rst", ".md", ".yml", ".yaml"}
ALLOWED = {
    Path("src/metasmith/env/environment.py"),
    Path("tests/metasmith/unit/test_renamed_symbols_are_gone.py"),
}


def _candidate_files():
    try:
        listed = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "ls-files", "-z", *SEARCH_ROOTS],
            capture_output=True, text=True, check=True,
        ).stdout.split("\0")
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("not a git checkout; nothing to define 'shipped'")
    for name in listed:
        if not name:
            continue
        rel = Path(name)
        if rel.suffix not in SUFFIXES or rel in ALLOWED:
            continue
        p = REPO_ROOT / rel
        if p.is_file():
            yield rel, p


@pytest.mark.parametrize("old,new", sorted(RENAMED.items()))
def test_renamed_symbol_is_gone_from_shipped_text(old, new):
    offenders = []
    for rel, p in _candidate_files():
        try:
            text = p.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if old in text:
            offenders.append(str(rel))
    assert not offenders, (
        f"[{old}] was renamed to [{new}] but still appears in:\n  "
        + "\n  ".join(sorted(offenders))
    )
