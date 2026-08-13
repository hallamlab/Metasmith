"""Regression test: dev.sh refuses to build the image / conda package from a
stale dist/.

`-bd` re-tags the image from the *live* source tree but installs whatever sdist
sits in dist/ (`COPY ./dist/*.tar.gz`); `-bc` packages that same sdist. If the
source is edited after `-bp` without rebuilding, the artifact's embedded build
hash no longer matches the tag the image is given (and the conda build). The
`_assert_dist_matches_source` guard recomputes the live source hash and requires
a matching `dist/<name>-<ver>+<hash>.tar.gz`, blocking the build otherwise.
"""

import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DEV_SH = REPO_ROOT / "dev" / "metasmith.sh"


def _run_guard(tmp_path: Path, *, stub_hash: str, dist_files: list[str]):
    """Source dev.sh, point HERE at a fake repo whose source hash is `stub_hash`
    (via a `python` stub on PATH), populate dist/ with `dist_files`, and call
    `_assert_dist_matches_source`. Returns the CompletedProcess."""
    fake_here = tmp_path / "repo"
    (fake_here / "dist").mkdir(parents=True)
    (fake_here / "src").mkdir()
    for f in dist_files:
        (fake_here / "dist" / f).write_text("x")

    # stub `python` so `python -m metasmith._build_hash` prints a fixed hash
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    py_stub = stub_dir / "python"
    py_stub.write_text(f"#!/bin/sh\necho {stub_hash}\n")
    py_stub.chmod(0o755)

    script = textwrap.dedent(f"""
        set -e
        source {DEV_SH} >/dev/null 2>&1 || true
        # override the repo-derived vars to point at the fake layout
        HERE={fake_here}
        NAME=metasmith
        VER=9.9.9
        export PATH={stub_dir}:$PATH
        _assert_dist_matches_source
    """)
    return subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, timeout=20
    )


@pytest.mark.skipif(not DEV_SH.exists(), reason="dev.sh missing")
@pytest.mark.skipif(shutil.which("bash") is None, reason="bash required")
def test_guard_passes_when_dist_matches(tmp_path):
    proc = _run_guard(
        tmp_path,
        stub_hash="abc1234",
        dist_files=["metasmith-9.9.9+abc1234.tar.gz"],
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "matches source" in proc.stdout


@pytest.mark.skipif(not DEV_SH.exists(), reason="dev.sh missing")
@pytest.mark.skipif(shutil.which("bash") is None, reason="bash required")
def test_guard_blocks_on_stale_dist(tmp_path):
    # dist holds an sdist from a *different* (old) source hash
    proc = _run_guard(
        tmp_path,
        stub_hash="abc1234",
        dist_files=["metasmith-9.9.9+0000000.tar.gz"],
    )
    assert proc.returncode != 0
    assert "does not match the current source tree" in proc.stdout
    assert "metasmith-9.9.9+abc1234.tar.gz" in proc.stdout


@pytest.mark.skipif(not DEV_SH.exists(), reason="dev.sh missing")
@pytest.mark.skipif(shutil.which("bash") is None, reason="bash required")
def test_guard_blocks_on_empty_dist(tmp_path):
    proc = _run_guard(tmp_path, stub_hash="abc1234", dist_files=[])
    assert proc.returncode != 0
    assert "does not match the current source tree" in proc.stdout


@pytest.mark.skipif(not DEV_SH.exists(), reason="dev.sh missing")
@pytest.mark.skipif(shutil.which("bash") is None, reason="bash required")
def test_guard_override_skips_check(tmp_path):
    fake_here = tmp_path / "repo"
    (fake_here / "dist").mkdir(parents=True)
    script = textwrap.dedent(f"""
        source {DEV_SH} >/dev/null 2>&1 || true
        HERE={fake_here}
        NAME=metasmith
        VER=9.9.9
        export MSM_SKIP_DIST_CHECK=1
        _assert_dist_matches_source
    """)
    proc = subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, timeout=20
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "skipping dist/source hash check" in proc.stdout
