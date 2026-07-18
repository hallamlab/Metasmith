"""Host-shared bootstrap conda env for agent shell-tool calls.

Real users have `mamba` already installed on PATH before they run the
metasmith install command. This module provides that minimal precondition
in our test harness: a conda env containing just `python`, `mamba`, and
`apptainer`. The agent inherits this env's `bin/` on PATH; everything
metasmith-related happens via `.condarc` redirection into a sandbox-local
env that the *agent* creates.

The bootstrap env is shared across all tests (it's not per-test state) and
lives at ``~/.cache/msm-e2e/bootstrap-env/``. First call materializes it
(~30 s); subsequent calls are instant.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

_DEFAULT_CACHE = Path.home() / ".cache" / "msm-e2e"


def bootstrap_env_path(cache_root: Path | None = None) -> Path:
    return (cache_root or _DEFAULT_CACHE) / "bootstrap-env"


def _env_has_tools(env_path: Path) -> bool:
    """Crude liveness check: bin/ exists with mamba and apptainer."""
    bin_dir = env_path / "bin"
    return all((bin_dir / x).exists() for x in ("python", "mamba", "apptainer"))


def ensure_bootstrap_env(cache_root: Path | None = None) -> Path:
    """Create the host bootstrap env if missing; return its absolute path.

    The env carries the prerequisites a real user is expected to have
    pre-installed: mamba (to run the install command from the docs) and
    apptainer (the conda recipe doesn't require apptainer; it's part of the
    container-runtime setup a user does separately). Python is along for
    the ride because mamba's executable wraps it.
    """
    env_path = bootstrap_env_path(cache_root)
    if _env_has_tools(env_path):
        return env_path
    env_path.parent.mkdir(parents=True, exist_ok=True)

    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    # mamba is pinned to 2.5.0: mamba/micromamba >=2.6 default to CEP-16
    # "sharded repodata", whose on-demand per-package shard fetch silently
    # drops transitive deps across channels (observed on micb0: a bioconda
    # `nextflow` pulling a conda-forge `openjdk` shard came back empty, so the
    # metasmith solve failed with "openjdk ... does not exist"). 2.5.0 uses
    # classic full repodata.json and solves the same specs cleanly. This binary
    # is what the agent's install path uses too, so the pin fixes both.
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
