"""The marker vocabulary and the directory map must describe one suite.

Two halves of the tree arrived with different taxonomies: one keyed by
axis directory with markers applied automatically, one hand-decorated. The
merge that joined them could have kept either marker list, and the wrong
choice loses a whole gate silently -- a marker nobody applies selects
nothing, and a directory nobody maps stamps nothing.

So: every marker `pyproject.toml` declares is either reachable from
`_DIR_MARKERS` or explicitly listed here as applied by hand, and every
axis directory that exists is mapped.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.conftest import _DIR_MARKERS


REPO_ROOT = Path(__file__).resolve().parents[2]
TESTS_ROOT = REPO_ROOT / "tests"

# Markers no directory grants, with the reason each is applied by hand.
MANUAL_ONLY = {
    # capability gates -- a test asks for these itself, per test, because the
    # capability is a property of the host, not of the axis
    "requires_ssh_localhost",
    "requires_docker",
    "requires_apptainer",
    "requires_docker_dev_image",
    "network",
    # legacy selectors kept for `-m "not docker"` style invocations
    "docker",
    "nextflow",
}

# Directories under tests/ that hold no tests and so need no axis row.
NON_AXIS_DIRS = {"fixtures", "__pycache__", "repro"}


def _declared_markers() -> set[str]:
    body = (REPO_ROOT / "pyproject.toml").read_text()
    block = re.search(r"^markers\s*=\s*\[(.*?)^\]", body, re.S | re.M)
    assert block, "could not find the markers list in pyproject.toml"
    return {
        m.group(1)
        for m in re.finditer(r'"\s*([A-Za-z_][A-Za-z0-9_]*)\s*:', block.group(1))
    }


def test_every_declared_marker_is_reachable():
    granted = {m for _dir, markers in _DIR_MARKERS for m in markers}
    unreachable = _declared_markers() - granted - MANUAL_ONLY
    assert not unreachable, (
        "these markers are declared but no directory grants them and they are "
        f"not listed as manual-only: {sorted(unreachable)}"
    )


def test_every_granted_marker_is_declared():
    granted = {m for _dir, markers in _DIR_MARKERS for m in markers}
    undeclared = granted - _declared_markers()
    assert not undeclared, (
        "tests/conftest.py grants markers pyproject.toml does not declare, so "
        f"pytest warns and `-m` on them is a typo away from silence: {sorted(undeclared)}"
    )


def test_every_axis_directory_is_mapped():
    mapped = {d.split("/")[0] for d, _ in _DIR_MARKERS}
    on_disk = {
        p.name
        for p in TESTS_ROOT.iterdir()
        if p.is_dir() and p.name not in NON_AXIS_DIRS and not p.name.startswith(".")
    }
    unmapped = {
        d for d in on_disk - mapped
        if any((TESTS_ROOT / d).rglob("test_*.py"))
    }
    assert not unmapped, (
        f"these test directories sit under no axis in _DIR_MARKERS: {sorted(unmapped)}"
    )


def test_no_test_files_at_the_tests_root():
    """The root is not an axis, so a file there would carry no marker.

    `tests/conftest.py` already fails collection on one; this says the same
    thing at the file level so the message names the layout rule rather than
    the symptom.
    """
    strays = sorted(p.name for p in TESTS_ROOT.glob("test_*.py"))
    assert not strays, (
        f"test files at tests/ root belong under an axis directory: {strays}"
    )
