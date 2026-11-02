from __future__ import annotations

import os
from pathlib import Path


_CONTAINER_CACHE_SUBDIR = "container_images"


def sif_basename(image: str) -> str:
    name = image.replace("://", "..").replace(":", "..").replace("/", "_")
    return f"{name}.sif"


def store_root(agent_home: Path, apptainer_cachedir: str | os.PathLike | None = None) -> Path:
    if apptainer_cachedir:
        return Path(apptainer_cachedir)
    return agent_home / _CONTAINER_CACHE_SUBDIR


def expected_sif_path(agent_home: Path, image: str,
                      apptainer_cachedir: str | os.PathLike | None = None) -> Path:
    return store_root(agent_home, apptainer_cachedir) / sif_basename(image)


def preplace_sif(agent_home: Path, image: str, source_sif: Path,
                 apptainer_cachedir: str | os.PathLike | None = None) -> Path:
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
