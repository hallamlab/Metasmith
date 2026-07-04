"""Regression test for the hardened dev-overlay staging LOGIC used by the
bootstrap heredoc in `agents.py` (`run_container`).

The bootstrap stages the dev overlay per-node-once under `flock`, verifies
completeness (key submodule + file count) before binding, retries transient
errno-108 (ESHUTDOWN), and fails-open to the shared Lustre bind on any error.
That shell lives inside a generated heredoc and cannot be imported, so this
runs the same primitives against a simulated flaky source (a partial readdir
that a copy reports as success) and asserts:

  * naive staging binds a silently-incomplete copy -> import fails (the
    reproduced exit-127 mechanism),
  * hardened staging retries past the partial read and binds a verified copy,
  * hardened staging fails-open (never leaves a partial dest) on a broken source,
  * per-node-once flock makes concurrent callers read the source exactly once.

Faithful HPC evidence for the same fix: RCA plan 02-errno108-overlay-fanout-rca.md
(naive 558 errno-108 / 93-of-97 incomplete; per-node-once 0 / 0).
"""
from pathlib import Path
import subprocess

_SCRIPT = Path(__file__).parent / "overlay_staging_logic.sh"


def test_hardened_overlay_staging_logic() -> None:
    assert _SCRIPT.exists(), f"missing staging-logic script at {_SCRIPT}"
    proc = subprocess.run(
        ["/bin/bash", str(_SCRIPT)], capture_output=True, text=True, timeout=120
    )
    out = proc.stdout + proc.stderr
    assert "ALL GREEN" in out, f"staging logic regression failed:\n{out}"
    assert proc.returncode == 0, f"non-zero exit {proc.returncode}:\n{out}"
