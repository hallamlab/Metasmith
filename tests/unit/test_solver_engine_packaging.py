"""The three ways the solver binary silently fails to ship.

None of these has a symptom. A wheel built without `engine/**` in
`package_data`, an image built over a stale stage, a resolver pointed somewhere
the package data does not go -- all three install cleanly, plan correctly, and
run the python solver about fifteen times slower than the one that was supposed
to be there. So they are pinned here rather than discovered by someone noticing
their planning got slow.

Same shape as `test_container_tag.py` and `test_dev_sh_tag.py`: read the build
files as text and assert about them. `setup.py` in particular is *read*, never
imported -- importing it runs a `setup()` call.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import metasmith
from metasmith.models.solver_engine import ENGINE_DIR, ENGINE_NAME, packaged_engine_path

REPO_ROOT = Path(__file__).resolve().parents[2]
SETUP_PY = REPO_ROOT/"setup.py"
DEV_SH = REPO_ROOT/"dev.sh"


def test_the_engine_resolves_inside_the_installed_package():
    """One lookup, and it is the package's own directory.

    This is what makes source / container / conda identical: `PYTHONPATH=src`
    and an installed wheel both make `<metasmith>/engine/` the same relative
    place. A resolver that reached for PATH, a repo-relative path, or an
    environment variable would work in exactly one of the three.
    """
    package_root = Path(metasmith.__file__).resolve().parent
    assert ENGINE_DIR.resolve() == package_root/"engine"
    found = packaged_engine_path()
    if found is not None:
        assert found.parent.resolve() == package_root/"engine"
        assert found.name.startswith(f"{ENGINE_NAME}.")


def test_no_metasmith_code_reads_an_environment_variable_to_pick_a_solver():
    """Selection is a class, and only a class.

    `METASMITH_SOLVER_ENGINE` used to decide this, which meant the choice was
    invisible at the call site and unscoped. `MSM_SOLVER_TRACE` is excluded on
    purpose: the Rust binary reads it, metasmith never does.
    """
    src = REPO_ROOT/"src"/"metasmith"
    offenders = [
        p.relative_to(REPO_ROOT).as_posix()
        for p in src.rglob("*.py")
        if "METASMITH_SOLVER_ENGINE" in p.read_text(encoding="utf-8")
    ]
    assert offenders == [], f"solver selection read from the environment in: {offenders}"


def test_setup_py_still_ships_the_engine_directory():
    """`engine/**`, recursive, in `package_data`.

    Recursive on purpose, the same way `gui/static/**` and `std/**` are: the
    single-star form matches nothing setuptools then copies, and the wheel
    builds green either way.
    """
    text = SETUP_PY.read_text(encoding="utf-8")
    assert '"engine/**"' in text, (
        "setup.py no longer lists engine/** in package_data; the wheel and the"
        " conda package would ship no solver binaries and nothing would fail"
    )


@pytest.mark.parametrize("verb", ["-bp", "-bc", "-bd"])
def test_every_shipping_build_checks_the_engine_stage(verb):
    """pip, conda and docker all install the sdist, so all three can ship a
    stage that is empty or host-linked. The guard has to be on each."""
    text = DEV_SH.read_text(encoding="utf-8")
    # From this verb's `case` arm to the next one.
    arm = re.search(
        rf"^    \{verb}\).*?(?=^    -|\A\Z)", text, re.S | re.M,
    )
    assert arm is not None, f"dev.sh has no [{verb}] arm any more"
    assert "_assert_solver_engine" in arm.group(0), (
        f"dev.sh {verb} no longer runs _assert_solver_engine; it would build"
        " a shippable artifact with no usable solver engine in it, silently"
    )
