"""The standard library clone, and what a project bootstrap consists of.

`msm lab` and `msm gui` open the same working directory and need the same things
in it: the bundled example resources, and a clone of the standard library. That
bootstrap lives here so the two front ends share it rather than drifting apart.

The library is pulled from main with no configuration -- there is deliberately no
UI for it and no per-project pin. Everything it contains is offered to the
planner, so adding a transform library to the repository is enough to make it
available.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

from ..constants import MODULE_PATH, STDLIB_NAME, STDLIB_URL
from ..logging import Log

DATA_TYPES_DIRNAME = "data_types"
TRANSFORMS_DIRNAME = "transforms"
RESOURCES_DIRNAME = "resources"


def clone_stdlib(root: Path, url: str = STDLIB_URL) -> dict:
    """Clone the standard library into `root` if it is not already there.

    A failed clone is reported, not raised: a user without network access should
    still get a notebook or a page, with the absence stated plainly rather than a
    traceback at startup. Callers surface `error` in the UI.
    """
    dest = Path(root) / STDLIB_NAME
    if dest.exists():
        return {"path": str(dest), "cloned": False}
    Log.Info(f"downloading standard library from [{url}]...")
    res = subprocess.run(
        ["git", "clone", "--depth", "1", url, str(dest)],
        text=True, capture_output=True,
    )
    if res.returncode != 0:
        err = res.stderr.strip() or f"git clone exited {res.returncode}"
        Log.Error(f"failed to clone [{url}]: {err}")
        return {"path": str(dest), "cloned": False, "error": err}
    return {"path": str(dest), "cloned": True}


def stdlib_commit(root: Path) -> str | None:
    """The commit the clone is on, recorded with each plan so a result is traceable."""
    dest = Path(root) / STDLIB_NAME
    if not (dest / ".git").exists():
        return None
    res = subprocess.run(
        ["git", "-C", str(dest), "rev-parse", "HEAD"], text=True, capture_output=True,
    )
    return res.stdout.strip() or None if res.returncode == 0 else None


def copy_example_resources(root: Path) -> dict:
    """Copy the bundled tutorials/transforms into the project, once."""
    dest = Path(root) / "example_resources"
    if dest.exists():
        return {"path": str(dest), "copied": False}
    src = MODULE_PATH / "example_resources"
    if not src.exists():
        return {"path": str(dest), "copied": False}
    Log.Info("loading tutorials...")
    subprocess.run(["rsync", "-auP", f"{src}/", f"{dest}"], text=True)
    return {"path": str(dest), "copied": True}


def bootstrap_project(root: Path, with_examples: bool = True, url: str = STDLIB_URL) -> dict:
    """Everything a fresh working directory needs before either front end opens."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    out: dict = {"root": str(root)}
    if with_examples:
        out["examples"] = copy_example_resources(root)
    out["stdlib"] = clone_stdlib(root, url)
    return out


# -- discovery ---------------------------------------------------------------
#
# The repository is a plain directory tree; these are the three shapes in it.


def _dirs(path: Path) -> list[Path]:
    if not path.is_dir():
        return []
    return sorted(p for p in path.iterdir() if p.is_dir())


def discover(root: Path) -> dict:
    """List the type, transform, and resource libraries in the clone."""
    lib = Path(root) / STDLIB_NAME
    types = []
    tdir = lib / DATA_TYPES_DIRNAME
    if tdir.is_dir():
        types = sorted(str(p) for p in tdir.glob("*.yml"))
    return {
        "path": str(lib),
        "present": lib.is_dir(),
        "commit": stdlib_commit(root),
        "data_types": types,
        "transform_libraries": [str(p) for p in _dirs(lib / TRANSFORMS_DIRNAME)],
        "resource_libraries": [str(p) for p in _dirs(lib / RESOURCES_DIRNAME)],
    }


def available_types(root: Path) -> list[dict]:
    """Every data type in the standard library, namespaced by its file stem."""
    from ..models.libraries import DataTypeLibrary

    out: list[dict] = []
    for p in discover(root)["data_types"]:
        path = Path(p)
        namespace = path.stem
        try:
            lib = DataTypeLibrary.Load(path)
        except Exception as exc:
            out.append({"namespace": namespace, "path": p, "error": str(exc)})
            continue
        for name, endpoint in lib.types.items():
            out.append({
                "namespace": namespace,
                "name": name,
                "full_name": f"{namespace}::{name}",
                "path": p,
                "properties": sorted(endpoint.properties),
            })
    return out
