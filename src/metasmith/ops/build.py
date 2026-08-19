from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

import yaml

from ..models.build_libraries import (
    LoadTypeLibraries,
    CompileUniqueLibrary,
    CompileTransformLibrary,
    Build,
)
from .._build_hash import compute_build_hash

def _parse_vendor_srcs(srcs: list[str]) -> dict[str, Path]:
    mapping: dict[str, Path] = {}
    for item in srcs:
        if "=" not in item:
            raise ValueError(f"--src must be NAME=PATH, got [{item}]")
        name, _, path = item.partition("=")
        name = name.strip()
        if not name:
            raise ValueError(f"--src [{item}] has an empty NAME")
        if name in mapping:
            raise ValueError(f"--src NAME [{name}] given more than once")
        p = Path(path).resolve()
        if not p.is_dir():
            raise FileNotFoundError(f"--src {name}=[{p}] is not a directory")
        mapping[name] = p
    return mapping


def _library_content_hash(mapping: dict[str, Path]) -> str:
    parts = [f"{name}:{compute_build_hash(path)}" for name, path in sorted(mapping.items())]
    return hashlib.md5("\0".join(parts).encode()).hexdigest()[:7]


def load_types(type_dirs: list[str]) -> dict:
    types = LoadTypeLibraries([Path(p) for p in type_dirs])
    return {ns: len(lib.types) for ns, lib in types.items()}


def compile_uniques(unique_dirs: list[str], type_dirs: list[str]) -> dict:
    types = LoadTypeLibraries([Path(p) for p in type_dirs])
    results = [CompileUniqueLibrary(Path(d), types) for d in unique_dirs]
    return {"types": {ns: len(lib.types) for ns, lib in types.items()}, "uniques": results}


def compile_transforms(transform_dirs: list[str], type_dirs: list[str]) -> dict:
    types = LoadTypeLibraries([Path(p) for p in type_dirs])
    results = [CompileTransformLibrary(Path(d), types) for d in transform_dirs]
    return {"types": {ns: len(lib.types) for ns, lib in types.items()}, "transforms": results}


def build_all(
    type_dirs: list[str],
    transform_dirs: list[str],
    unique_dirs: list[str] | None = None,
) -> dict:
    return Build(
        data_type_dirs=[Path(p) for p in type_dirs],
        transform_dirs=[Path(p) for p in transform_dirs],
        unique_dirs=[Path(p) for p in (unique_dirs or [])],
    )


def _assert_metadata_present(dst_path: Path) -> None:
    found = list(dst_path.rglob("_metadata"))
    if not found:
        raise ValueError(
            f"vendored bundle at [{dst_path}] carries no _metadata/ at all -- "
            f"it was built from an uncompiled source tree. Compile first "
            f"(`dev/libraries.sh -bm` or `metasmith build all ...`), then re-vendor."
        )
    empty = [d for d in found if not any(d.rglob("*"))]
    if empty:
        rel = ", ".join(str(d.relative_to(dst_path)) for d in sorted(empty))
        raise ValueError(
            f"vendored bundle at [{dst_path}] carries empty _metadata/ dirs: {rel}. "
            f"A library with no metadata loads and resolves nothing, silently -- "
            f"this is refused rather than shipped. Compile first, then re-vendor."
        )

    hollow = []
    for d in sorted(found):
        index = d / "index.yml"
        if not index.is_file():
            hollow.append((d, "no index.yml"))
            continue
        try:
            doc = yaml.safe_load(index.read_text()) or {}
        except yaml.YAMLError as e:
            hollow.append((d, f"unreadable index.yml ({e.__class__.__name__})"))
            continue
        if not (doc.get("manifest") or {}):
            hollow.append((d, "index.yml lists nothing"))
    if hollow:
        rel = ", ".join(f"{d.relative_to(dst_path)} ({why})" for d, why in hollow)
        raise ValueError(
            f"vendored bundle at [{dst_path}] carries unusable metadata: {rel}. "
            f"A library whose manifest is empty resolves nothing, silently -- "
            f"this is refused rather than shipped. Compile first, then re-vendor."
        )


def vendor_library(srcs: list[str], dst: str) -> dict:
    mapping = _parse_vendor_srcs(srcs)
    dst_path = Path(dst).resolve()
    content_hash = _library_content_hash(mapping)

    if dst_path.exists():
        shutil.rmtree(dst_path)
    dst_path.mkdir(parents=True)
    for name, path in mapping.items():
        # AGENTS.md is an authoring brief that resolves by directory proximity, so a
        # copy of it beside the vendored code is a second one that drifts from the
        # source without anything noticing.
        shutil.copytree(path, dst_path / name, ignore=shutil.ignore_patterns("AGENTS.md"))
    _assert_metadata_present(dst_path)
    (dst_path / "VENDOR_HASH").write_text(content_hash)
    return {"dst": str(dst_path), "vendored": sorted(mapping), "content_hash": content_hash}


def check_vendor_library(srcs: list[str], dst: str) -> dict:
    mapping = _parse_vendor_srcs(srcs)
    dst_path = Path(dst).resolve()
    stamp_file = dst_path / "VENDOR_HASH"
    if not stamp_file.exists():
        raise FileNotFoundError(
            f"no vendored bundle at [{dst_path}] (missing VENDOR_HASH) -- run "
            f"vendor-library first"
        )
    live_hash = _library_content_hash(mapping)
    stamped_hash = stamp_file.read_text().strip()
    if stamped_hash != live_hash:
        raise ValueError(
            f"vendored bundle at [{dst_path}] is stale: stamped [{stamped_hash}], "
            f"live source [{live_hash}]. Re-run vendor-library."
        )
    _assert_metadata_present(dst_path)
    return {"dst": str(dst_path), "content_hash": live_hash, "ok": True}
