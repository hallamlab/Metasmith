"""Regression test for the TARBALL dev-overlay staging LOGIC used by the
bootstrap heredoc in `agents.py` (`run_container`).

The bootstrap delivers the overlay as a single tarball; each node natively
`cp`s it to local scratch, `tar -x` extracts it, verifies completeness (key
submodule + file count) before binding, does this per-node-once under `flock`,
keys the cache by the tarball's own `stat` (mtime+size, so no dependence on
SLURM_ARRAY_JOB_ID), retries transient errno-108 (ESHUTDOWN), and fails-open to
the shared Lustre bind on any error. That shell lives inside a generated heredoc
and cannot be imported, so this runs the same primitives and asserts:

  * a truncated tarball fails `tar -x` loudly (no silently-incomplete bind — the
    old exit-0-on-partial-readdir -> exit-127 trap is gone),
  * a good tarball is cp+extracted, verified, and importable,
  * staging fails-open (never leaves a partial dest) on a broken tarball,
  * per-node-once flock makes concurrent callers cp+extract exactly once,
  * the `stat` cache key reuses an identical tarball but re-extracts a changed
    one to a fresh dir (a reused node never serves a stale overlay).

Faithful HPC evidence for the same fix: RCA plan 02-errno108-overlay-fanout-rca.md
(naive rsync fan-out 558 errno-108 / 93-of-97 incomplete; tarball 0 / 0).
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
