"""Container-registry spoof for the e2e suite.

`metasmith agent deploy` gates pulls on ``[ -e <local_sif> ] || pull`` for
APPTAINER (and on `docker run`'s own daemon-local lookup for DOCKER). To
keep the agent from ever reaching quay.io we pre-place the locally-built
sif at the exact path `Agent.Deploy` computes.

For DOCKER there is no per-test setup: `dev.sh -bd` tags the host daemon
with ``quay.io/hallamlab/metasmith:<VER>`` and `docker run` finds it.
"""
from __future__ import annotations

import os
from pathlib import Path


# The CONTAINER_CACHE subdir name from src/metasmith/constants.py:AgentPaths.
# Hardcoded here so the harness does not import metasmith (the agent's
# environment is the system-under-test; we keep the host harness loose).
_CONTAINER_CACHE_SUBDIR = "container_images"


def sif_basename(image: str) -> str:
    """Mirror Container.GetLocalPath()'s sanitizer (containers.py:35).

    For image ``docker://quay.io/hallamlab/metasmith:0.18.1`` this yields
    ``docker..quay.io_hallamlab_metasmith..0.18.1.sif``.
    """
    name = image.replace("://", "..").replace(":", "..").replace("/", "_")
    return f"{name}.sif"


def store_root(agent_home: Path, apptainer_cachedir: str | os.PathLike | None = None) -> Path:
    """Mirror Container._store_root() (containers.py).

    The image store is ``APPTAINER_CACHEDIR`` when set on the execution host,
    else ``<agent_home>/container_images``. The harness sets APPTAINER_CACHEDIR
    in the agent's env (sandbox.py), so the spoof must pre-place the sif where
    deploy will actually look — pass that value through here.
    """
    if apptainer_cachedir:
        return Path(apptainer_cachedir)
    return agent_home / _CONTAINER_CACHE_SUBDIR


def expected_sif_path(agent_home: Path, image: str,
                      apptainer_cachedir: str | os.PathLike | None = None) -> Path:
    """Path Agent.Deploy will check before pulling."""
    return store_root(agent_home, apptainer_cachedir) / sif_basename(image)


def preplace_sif(agent_home: Path, image: str, source_sif: Path,
                 apptainer_cachedir: str | os.PathLike | None = None) -> Path:
    """Hardlink ``source_sif`` to the path Agent.Deploy expects.

    Idempotent: if the destination already points at the same inode it's a
    no-op. Falls back to ``copy`` across filesystems where hardlink fails.
    """
    if not source_sif.exists():
        raise FileNotFoundError(f"source sif not found: {source_sif}")
    dest = expected_sif_path(agent_home, image, apptainer_cachedir)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        try:
            if dest.samefile(source_sif):
                return dest
        except OSError:
            pass
        dest.unlink()
    try:
        os.link(source_sif, dest)
    except OSError:
        import shutil
        shutil.copy2(source_sif, dest)
    return dest
