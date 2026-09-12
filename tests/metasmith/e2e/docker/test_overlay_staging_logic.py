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
