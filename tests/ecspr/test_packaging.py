from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

import ecspr
from ecspr._build_hash import BUILD_HASH_FILE, compute_build_hash

REPO = Path(ecspr.__file__).resolve().parents[2]
COMPILE = REPO / "conda_recipe" / "ecspr" / "compile_recipe.py"

# These are repo-side packaging guards, and they write the build stamp to say what
# an unstamped tree does. Inside the image `ecspr` is installed with no repo beside
# it, and mutating an installed package's stamp there is not a thing a test may do.
pytestmark = pytest.mark.skipif(not COMPILE.exists(),
                                reason="installed package, no repo tree alongside")


def _compile(out_dir: Path):
    return subprocess.run([sys.executable, str(COMPILE), "--out-dir", str(out_dir)],
                          capture_output=True, text=True)


@pytest.fixture
def stamp():
    was = BUILD_HASH_FILE.read_text() if BUILD_HASH_FILE.exists() else None

    def set_to(value: str | None):
        if value is None:
            BUILD_HASH_FILE.unlink(missing_ok=True)
        else:
            BUILD_HASH_FILE.write_text(value)
    yield set_to
    set_to(was)


def test_the_conda_recipe_still_compiles(stamp, tmp_path):
    stamp(compute_build_hash())
    r = _compile(tmp_path)
    assert r.returncode == 0, r.stderr

    meta = (tmp_path / "meta.yaml").read_text()
    assert not re.findall(r"<[A-Z_]+>", meta), f"unsubstituted placeholder:\n{meta}"
    # conda renders the recipe through jinja first; yaml alone chokes on `{{ ... }}`.
    spec = yaml.safe_load(re.sub(r"\{\{.*?\}\}", "jinja", meta))
    assert spec["package"]["version"] == ecspr.VERSION
    assert spec["build"]["string"] == f"py_{compute_build_hash()}"
    assert f"{ecspr.NAME} selftest" in spec["test"]["commands"]
    assert (tmp_path / "call_build.sh").exists()


def test_an_unstamped_tree_will_not_compile_a_recipe(stamp, tmp_path):
    stamp(None)
    r = _compile(tmp_path)
    assert r.returncode != 0
    assert "ecspr._build_hash --write" in r.stderr
    assert not (tmp_path / "meta.yaml").exists()


def test_an_unstamped_tree_does_not_answer_like_a_release(stamp):
    stamp(None)
    env = {**os.environ, "PYTHONPATH": str(REPO / "src")}
    out = subprocess.run([sys.executable, "-c",
                          "import ecspr; print(ecspr.FULL_VERSION, ecspr.CONTAINER_TAG)"],
                         capture_output=True, text=True, env=env)
    assert out.returncode == 0, out.stderr
    full, tag = out.stdout.split()
    assert full == f"{ecspr.VERSION}+unstamped"
    assert tag == f"{ecspr.VERSION}-unstamped"
