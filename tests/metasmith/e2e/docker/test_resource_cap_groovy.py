"""The retry-ladder cap metasmith generates is Groovy nextflow accepts.

`cap_lines` writes a closure into every local run's config. A closure that does
not parse would break every local run at config time, which is worse than the
unbounded ladder it replaces -- and no amount of string assertion in the unit
lane can tell whether nextflow accepts it. So run it.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from metasmith.agents.ceiling import cap_lines


PROCESS = '''
process hello {
    output:
    stdout
    script:
    """
    echo "memory=${task.memory}"
    """
    stub:
    """
    echo "memory=${task.memory}"
    """
}
workflow { hello() | view }
'''


def _run(tmp_path: Path, image: str, request_gb: float, ceiling_gb: float) -> str:
    (tmp_path / "main.nf").write_text(PROCESS)
    (tmp_path / "nextflow.config").write_text(
        "process { executor = 'local' }\n"
        + "\n".join(cap_lines({"hello": (1, request_gb)}, ceiling_gb))
        + "\n"
    )
    res = subprocess.run(
        ["docker", "run", "--rm", "-v", f"{tmp_path}:{tmp_path}", "-w", str(tmp_path),
         image, "nextflow", "run", "main.nf", "-stub", "-ansi-log", "false"],
        capture_output=True, text=True, timeout=600,
    )
    assert res.returncode == 0, res.stdout[-3000:] + res.stderr[-3000:]
    return res.stdout


def test_a_request_under_the_ceiling_is_left_alone(tmp_path, docker_image):
    assert "memory=8 GB" in _run(tmp_path, docker_image, 8.0, 50.0)


def test_a_request_over_the_ceiling_is_capped_to_it(tmp_path, docker_image):
    # Attempt 1 already exceeds it here, which is the same arithmetic attempt 2
    # of a 30 GB step does -- and the reason no retry ever recovered.
    assert "memory=50 GB" in _run(tmp_path, docker_image, 60.0, 50.0)
