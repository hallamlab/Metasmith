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
