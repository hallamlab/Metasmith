from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.metasmith.conftest import _DIR_MARKERS


REPO_ROOT = Path(__file__).resolve().parents[3]
TESTS_ROOT = REPO_ROOT / "tests" / "metasmith"

MANUAL_ONLY = {
    "requires_ssh_localhost",
    "requires_docker",
    "requires_apptainer",
    "requires_docker_dev_image",
    "requires_relay",
    "network",
    "docker",
    "nextflow",
    "python_solver",
}

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
    strays = sorted(p.name for p in TESTS_ROOT.glob("test_*.py"))
    assert not strays, (
        f"test files at tests/ root belong under an axis directory: {strays}"
    )
