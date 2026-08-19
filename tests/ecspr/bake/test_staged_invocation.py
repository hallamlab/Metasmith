from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
BUILDLIB = REPO / "src" / "fabfos" / "build_references" / "resources" / "buildlib"
STAGED = BUILDLIB / "ecspr"

INVOCATIONS = [
    "ecspr.bake.aam.worklist",
    "ecspr.bake.aam.redox",
    "ecspr.bake.direction.drive",
    "ecspr.bake.atom_pairs",
    "ecspr.bake.evidence",
]


@pytest.fixture(scope="module")
def staged() -> Path:
    if not (STAGED / "bake" / "__init__.py").exists():
        pytest.skip(f"no vendored package at {STAGED}; "
                    f"run src/fabfos/build_references/build.sh")
    return STAGED


def test_the_vendored_tree_excludes_what_would_move_its_hash(staged):
    junk = [p for p in staged.rglob("*")
            if p.name in ("build_hash.txt",) or p.suffix == ".pyc"
            or "__pycache__" in p.parts or ".egg-info" in p.name]
    assert junk == [], f"vendored tree carries non-source: {[str(p) for p in junk[:5]]}"


@pytest.mark.parametrize("module", INVOCATIONS)
def test_a_transform_shaped_invocation_resolves(staged, module):
    if module in ("ecspr.bake.atom_pairs", "ecspr.bake.aam.redox"):
        pytest.importorskip("rdkit", reason="the extractor imports it at module scope")
    env = dict(os.environ, PYTHONPATH=str(staged.parent), OMP_NUM_THREADS="1",
               PYTHONDONTWRITEBYTECODE="1")
    r = subprocess.run([sys.executable, "-m", module, "--help"],
                       capture_output=True, text=True, env=env, cwd=str(REPO))
    assert r.returncode == 0, f"{module}: {r.stderr[-2000:]}"
    assert "usage:" in r.stdout


def test_the_staged_directory_is_a_copy_not_a_link(staged):
    links = [p for p in staged.rglob("*") if p.is_symlink()]
    assert links == [], f"vendored tree contains symlinks: {[str(p) for p in links[:5]]}"
    assert not staged.is_symlink()
