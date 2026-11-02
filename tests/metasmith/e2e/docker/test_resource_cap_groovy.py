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


LADDER_PROCESS = '''
process p01__step {
    input:
    val codes
    output:
    val codes
    script:
    def c = codes.split(',')
    def code = task.attempt <= c.size() ? c[task.attempt-1] : '0'
    """
    echo "${codes}|${task.attempt}|${task.memory}|${task.time}" >> ${projectDir}/rungs.txt
    exit ${code}
    """
}
workflow { p01__step(channel.of('137,137,0', '140,0', '143,0', '1,0')) }
'''


def _run_ladder(tmp_path: Path, image: str) -> list[str]:
    from metasmith.models.libraries import Duration, Resources, Size
    from metasmith.models.libraries.resources import RetryStrategy

    res = Resources(memory=Size.GB(1), duration=Duration(hours=1))
    (tmp_path / "main.nf").write_text(LADDER_PROCESS)
    (tmp_path / "nextflow.config").write_text("\n".join([
        "params { process { tries = 3; maxMemory = '1.5 GB'; maxDuration = '90m' } }",
        "executor { cpus = 2; memory = '8 GB' }",
        "workflow { failOnIgnore = false }",
        "process {",
        "    executor = 'local'",
        "    maxRetries = 99",
        f"    errorStrategy = {RetryStrategy()}",
        "    withName: 'p01__step' {",
        *[f"        {l}" for l in res.AsNextflowFormat(is_config=True)],
        "    }",
        "}",
    ]) + "\n")
    subprocess.run(
        ["docker", "run", "--rm", "-v", f"{tmp_path}:{tmp_path}", "-w", str(tmp_path),
         image, "nextflow", "run", "main.nf", "-ansi-log", "false"],
        capture_output=True, text=True, timeout=600,
    )
    rungs = tmp_path / "rungs.txt"
    return rungs.read_text().splitlines() if rungs.exists() else []


def test_the_ladder_escalates_only_what_the_exit_code_names(tmp_path, docker_image):
    rungs = set(_run_ladder(tmp_path, docker_image))
    # An OOM doubles memory, clamped to maxMemory, and leaves time alone.
    assert "137,137,0|2|1.5 GB|1h" in rungs, rungs
    assert "137,137,0|3|1.5 GB|1h" in rungs, rungs
    # A Slurm timeout doubles time, clamped to maxDuration, and leaves memory alone.
    assert "140,0|2|1 GB|1h 30m" in rungs, rungs
    # A preemption retries as it was.
    assert "143,0|2|1 GB|1h" in rungs, rungs
    # The tool's own failure is not retried.
    assert not any(r.startswith("1,0|2|") for r in rungs), rungs
