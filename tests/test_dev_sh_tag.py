"""Regression test: dev.sh derives the same Docker tag as Agent.container.

If dev.sh's tag composition drifts from `constants.CONTAINER_TAG`, the
maintainer's `dev.sh -bd && dev.sh -ud` pushes one tag while fresh deploys
pull a different one. That was the root cause of issue 1.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

from metasmith.constants import CONTAINER_TAG

REPO_ROOT = Path(__file__).resolve().parents[1]
DEV_SH = REPO_ROOT / "dev.sh"


@pytest.mark.skipif(not DEV_SH.exists(), reason="dev.sh missing")
@pytest.mark.skipif(shutil.which("bash") is None, reason="bash required")
def test_dev_sh_emits_container_tag():
    """Source dev.sh's variable assignments in a clean subshell and verify
    DOCKER_TAG matches what Python computes from version.txt."""
    # `source` evaluates dev.sh top-of-file (the var-assignment block) without
    # taking a case-target branch (no $1 supplied -> falls into '*) bad option').
    # That's fine: VER and DOCKER_TAG are assigned before the case.
    proc = subprocess.run(
        ["bash", "-c", f"set -e; cd {REPO_ROOT}; source {DEV_SH} >/dev/null 2>&1; echo $DOCKER_TAG"],
        capture_output=True, text=True, timeout=10,
    )
    emitted = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else ""
    assert emitted == CONTAINER_TAG, (
        f"dev.sh DOCKER_TAG=[{emitted}] vs constants.CONTAINER_TAG=[{CONTAINER_TAG}]; "
        f"stderr=[{proc.stderr.strip()}]"
    )
    # And the tag must be Docker-legal (no '+').
    assert "+" not in emitted
