from __future__ import annotations

import ast
import fnmatch
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
PKG_REL = Path("src/metasmith")

EXEMPT_DIRS = {
    "example_resources/tutorials/.ipynb_checkpoints",
    "frontend",
    "examples",
    "scratch",
}
EXEMPT_SUFFIXES = {".pyc", ".pyi"}
EXEMPT_FILES = {
    ".gitignore",
}


def _tracked_data_files() -> list[Path]:
    out = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "ls-files", "-z", str(PKG_REL)],
        capture_output=True, text=True, check=True,
    ).stdout.split("\0")
    files = []
    for name in out:
        if not name:
            continue
        rel = Path(name).relative_to(PKG_REL)
        if rel.suffix == ".py" or rel.suffix in EXEMPT_SUFFIXES:
            continue
        if any(str(rel).startswith(d) for d in EXEMPT_DIRS):
            continue
        if rel.as_posix() in EXEMPT_FILES:
            continue
        files.append(rel)
    return files


def _declared_patterns() -> list[str]:
    tree = ast.parse((REPO_ROOT / "setup.py").read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.keyword) or node.arg != "package_data":
            continue
        for key, value in zip(node.value.keys, node.value.values):
            if isinstance(key, ast.Constant) and key.value == "":
                return [e.value for e in value.elts if isinstance(e, ast.Constant)]
    pytest.fail("could not find package_data's all-packages entry in setup.py")


def _covered(rel: Path, patterns: list[str]) -> bool:
    s = rel.as_posix()
    for pat in patterns:
        if fnmatch.fnmatch(s, pat):
            return True
        if pat.endswith("/**") and s.startswith(pat[:-2]):
            return True
    return False


def test_every_tracked_data_file_is_covered_by_package_data():
    patterns = _declared_patterns()
    missed = sorted(
        {str(f) for f in _tracked_data_files() if not _covered(f, patterns)}
    )
    assert not missed, (
        "these files live inside the package but no package_data glob ships "
        "them, so an installed metasmith will not have them:\n  "
        + "\n  ".join(missed)
        + f"\n\ndeclared globs: {patterns}"
    )
