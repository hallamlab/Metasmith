from __future__ import annotations

import os
import subprocess
from pathlib import Path

_DEFAULT_CACHE = Path.home() / ".cache" / "msm-e2e"


def bootstrap_env_path(cache_root: Path | None = None) -> Path:
    return (cache_root or _DEFAULT_CACHE) / "bootstrap-env"


def _env_has_tools(env_path: Path) -> bool:
    bin_dir = env_path / "bin"
    return all((bin_dir / x).exists() for x in ("python", "mamba", "apptainer"))


def ensure_bootstrap_env(cache_root: Path | None = None) -> Path:
    env_path = bootstrap_env_path(cache_root)
    if _env_has_tools(env_path):
        return env_path
    env_path.parent.mkdir(parents=True, exist_ok=True)

    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    cmd = [
        "mamba", "create", "-y",
        "-p", str(env_path),
        "-c", "conda-forge",
        "python=3.12", "mamba=2.5.0", "apptainer",
    ]
    r = subprocess.run(cmd, env=env)
    if r.returncode != 0:
        raise RuntimeError(
            f"failed to create bootstrap env at {env_path} (exit {r.returncode})"
        )
    if not _env_has_tools(env_path):
        raise RuntimeError(
            f"bootstrap env created at {env_path} but missing expected binaries"
        )
    return env_path
