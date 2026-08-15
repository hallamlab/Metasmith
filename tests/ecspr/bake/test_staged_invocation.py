"""The staged package resolves the way a transform invokes it.

Everything else in this suite imports the package the way a developer does.
A transform does not: it stages a directory named `ecspr` beside the surviving
flat buildlib files, sets `PYTHONPATH` to the directory ABOVE it, and runs
`python3 -m ecspr.bake.<lane>.<module>`. Every part of that is load-bearing --
the directory's NAME is the package name, `libdir` is its parent, and `-m` needs
the whole package importable from a cold interpreter with nothing else on the
path.

So this shells out. It is the one subprocess tier in the suite and it is worth
its second: the failure it catches (a vendor step that stopped copying, a
requirement that stopped staging, a module path that does not resolve) is
otherwise discovered six hours into a queued job.

The vendored copy is a build product of `build_references/build.sh`. When it is
absent this skips rather than fails -- a fresh checkout legitimately has none,
which is the same reason the library's `_metadata` has to be compiled before use.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
BUILDLIB = REPO / "src" / "fabfos" / "build_references" / "resources" / "buildlib"
STAGED = BUILDLIB / "ecspr"

# One per lane plus the two shared modules, rather than all fourteen: what is
# under test is that the staging idiom resolves, and a fourth subprocess proves
# nothing a third did not.
INVOCATIONS = [
    "ecspr.bake.aam.worklist",
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
    """`buildlib::ecspr`'s instance_id IS the tree digest, so junk in the tree is
    a cache miss with no cause anyone can see.

    Three things change without the code changing -- __pycache__, build_hash.txt
    and .egg-info -- and all three are excluded at vendor time. The tree can
    still be dirtied AFTER vendoring, by anything that runs from it (this file's
    own subprocesses would, which is why they set PYTHONDONTWRITEBYTECODE). That
    is safe in the sanctioned flow, because build.sh removes and re-copies the
    tree immediately before compiling the index that records its id -- but a
    failure here still means the id on disk describes something other than the
    source, so re-run build.sh rather than deleting the offending files by hand.
    """
    junk = [p for p in staged.rglob("*")
            if p.name in ("build_hash.txt",) or p.suffix == ".pyc"
            or "__pycache__" in p.parts or ".egg-info" in p.name]
    assert junk == [], f"vendored tree carries non-source: {[str(p) for p in junk[:5]]}"


@pytest.mark.parametrize("module", INVOCATIONS)
def test_a_transform_shaped_invocation_resolves(staged, module):
    """`PYTHONPATH={libdir} python3 -m {module} --help`, exactly as a transform
    writes it -- from a cold interpreter with the staging directory as the ONLY
    path entry, so a module that silently resolved from the source tree instead
    would not pass here."""
    if module == "ecspr.bake.atom_pairs":
        pytest.importorskip("rdkit", reason="the extractor imports it at module scope")
    # DONTWRITEBYTECODE because the test above asserts this tree is clean, and a
    # test that dirties the artifact it checks is a test that fails on its
    # second run for a reason that has nothing to do with the code.
    env = dict(os.environ, PYTHONPATH=str(staged.parent), OMP_NUM_THREADS="1",
               PYTHONDONTWRITEBYTECODE="1")
    r = subprocess.run([sys.executable, "-m", module, "--help"],
                       capture_output=True, text=True, env=env, cwd=str(REPO))
    assert r.returncode == 0, f"{module}: {r.stderr[-2000:]}"
    assert "usage:" in r.stdout


def test_the_staged_directory_is_a_copy_not_a_link(staged):
    """`Logistics` copies symlinks AS symlinks, so a vendor step that linked
    would stage a dangling path on every host that is not this one."""
    links = [p for p in staged.rglob("*") if p.is_symlink()]
    assert links == [], f"vendored tree contains symlinks: {[str(p) for p in links[:5]]}"
    assert not staged.is_symlink()
