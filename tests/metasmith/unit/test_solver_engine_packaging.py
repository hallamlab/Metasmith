"""The four ways the solver binary silently fails to ship.

None of these has a symptom. A wheel built without `engine/**` in
`package_data`, an image built over a stale stage, a resolver pointed somewhere
the package data does not go, a binary that arrives without its executable bit
-- all four install cleanly, plan correctly, and run the python solver about
fifteen times slower than the one that was supposed to be there. So they are
pinned here rather than discovered by someone noticing their planning got slow.

The fourth is the newest and the one this file used to miss entirely, because
every assertion below it is static text analysis that passes against an empty
`engine/`. `engine/` was DVC-tracked for a while; DVC materialises its outputs
as read-only hardlinks and does not carry the exec bit, so all four binaries
checked out mode 444 and every plan in the repo reverted to the python search
on a permission error. `test_an_engine_is_staged_*` are the two that go red for
that, and they are deliberately loud rather than skipping: `engine/` is a build
artifact now, so a fresh scope is red until someone builds it.

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

REPO_ROOT = Path(__file__).resolve().parents[3]
SETUP_PY = REPO_ROOT/"setup.py"
DEV_SH = REPO_ROOT/"dev"/"metasmith.sh"


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


@pytest.mark.parametrize("verb", ["-ud", "-bs"])
def test_every_publishing_step_checks_the_engine_inside_the_image(verb):
    """The staging guard runs before pip, and the damage happens during pip.

    A file staged mode 444 passes `_assert_solver_engine` -- it did not, until
    that guard learned the exec bit, and it still would if the mode were lost
    anywhere downstream: an sdist carries 444 through unchanged and a wheel
    normalises it to 644. Neither runs. So the last gate before something
    leaves this machine asks the *installed* package which backend it will use,
    which is the only check downstream of every step that can mangle a mode.
    """
    text = DEV_SH.read_text(encoding="utf-8")
    arm = re.search(rf"^    \{verb}\).*?(?=^    -|\A\Z)", text, re.S | re.M)
    assert arm is not None, f"dev.sh has no [{verb}] arm any more"
    assert "_assert_engine_in_image" in arm.group(0), (
        f"dev.sh {verb} no longer runs _assert_engine_in_image; a published"
        " image whose engine cannot execute is indistinguishable from a good"
        " one until someone times a plan"
    )


def test_the_stage_guard_checks_the_executable_bit():
    """Size and magic bytes say nothing about whether the file can run.

    This is the assertion the 444 checkout walked straight past: four valid
    ELF/Mach-O binaries of the right size, none of them executable.
    """
    text = DEV_SH.read_text(encoding="utf-8")
    arm = re.search(r"^_assert_solver_engine\(\).*?^\}", text, re.S | re.M)
    assert arm is not None, "dev.sh has no _assert_solver_engine any more"
    assert '[ -x "$f" ]' in arm.group(0), (
        "_assert_solver_engine no longer tests the executable bit; a stage"
        " that lost its mode passes every other check it makes"
    )


def test_an_engine_is_staged_for_this_platform():
    """`engine/` is generated, not committed, and nothing else notices its absence.

    Every other engine test in the suite skips when there is no binary, by
    design -- they are about behaviour, and there is none to test. This one is
    about the build, so it fails.
    """
    found = packaged_engine_path()
    assert found is not None, (
        f"no {ENGINE_NAME} staged for this platform in {ENGINE_DIR}."
        " It is a per-scope build artifact -- build it with:\n"
        "    ./dev/metasmith.sh -bec   # one time: pull the cross-compile container\n"
        "    ./dev/metasmith.sh -be    # build all 4 targets and stage them\n"
        "  or ./dev/metasmith.sh -bel  # host-only, seconds, not shippable"
    )


def test_the_staged_engine_is_executable():
    """The mode, on the file that is actually there.

    Separate from the test above so the failure names which of the two
    happened: nothing staged is a build that was not run, a staged file that
    cannot run is a mode lost between the build and here.
    """
    import os

    found = packaged_engine_path()
    if found is None:
        pytest.skip("nothing staged; test_an_engine_is_staged_for_this_platform reports that")
    assert os.access(found, os.X_OK), (
        f"{found} is mode {oct(found.stat().st_mode & 0o777)} and cannot be"
        " executed, so every solve in this checkout falls back to the python"
        " search. Restage it with ./dev/metasmith.sh -be (or chmod +x it)."
    )
