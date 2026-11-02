import shutil
import subprocess
from pathlib import Path

import pytest

from metasmith.constants import CONTAINER_TAG

REPO_ROOT = Path(__file__).resolve().parents[3]
DEV_SH = REPO_ROOT / "dev" / "metasmith.sh"


@pytest.mark.skipif(not DEV_SH.exists(), reason="dev.sh missing")
@pytest.mark.skipif(shutil.which("bash") is None, reason="bash required")
def test_dev_sh_emits_container_tag():
    proc = subprocess.run(
        ["bash", "-c", f"set -e; cd {REPO_ROOT}; source {DEV_SH} >/dev/null 2>&1; echo $DOCKER_TAG"],
        capture_output=True, text=True, timeout=10,
    )
    emitted = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else ""
    assert emitted == CONTAINER_TAG, (
        f"dev.sh DOCKER_TAG=[{emitted}] vs constants.CONTAINER_TAG=[{CONTAINER_TAG}]; "
        f"stderr=[{proc.stderr.strip()}]"
    )
    assert "+" not in emitted
