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
    package_root = Path(metasmith.__file__).resolve().parent
    assert ENGINE_DIR.resolve() == package_root/"engine"
    found = packaged_engine_path()
    if found is not None:
        assert found.parent.resolve() == package_root/"engine"
        assert found.name.startswith(f"{ENGINE_NAME}.")


def test_no_metasmith_code_reads_an_environment_variable_to_pick_a_solver():
    src = REPO_ROOT/"src"/"metasmith"
    offenders = [
        p.relative_to(REPO_ROOT).as_posix()
        for p in src.rglob("*.py")
        if "METASMITH_SOLVER_ENGINE" in p.read_text(encoding="utf-8")
    ]
    assert offenders == [], f"solver selection read from the environment in: {offenders}"


def test_setup_py_still_ships_the_engine_directory():
    text = SETUP_PY.read_text(encoding="utf-8")
    assert '"engine/**"' in text, (
        "setup.py no longer lists engine/** in package_data; the wheel and the"
        " conda package would ship no solver binaries and nothing would fail"
    )


@pytest.mark.parametrize("verb", ["-bp", "-bc", "-bd"])
def test_every_shipping_build_checks_the_engine_stage(verb):
    text = DEV_SH.read_text(encoding="utf-8")
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
