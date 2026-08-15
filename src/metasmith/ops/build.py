"""Library compilation — thin wrapper over models.build_libraries decomposed steps."""
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
    """Parse `NAME=PATH` pairs into a name -> source-dir mapping. NAME becomes
    the destination subdir it is copied to -- explicit rather than sniffed,
    since a library's shippable pieces live under unrelated top-level
    directories in this repo (`src/<lib>/data_types`, but `envs/<lib>` as a
    whole becomes `envs`), so there is no single naming convention to infer."""
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
    """Hash of each named source dir, reusing `_build_hash.compute_build_hash`
    per dir rather than a second walker."""
    parts = [f"{name}:{compute_build_hash(path)}" for name, path in sorted(mapping.items())]
    return hashlib.md5("\0".join(parts).encode()).hexdigest()[:7]


def load_types(type_dirs: list[str]) -> dict:
    """Load every *.yml/*.yaml from each type dir; report counts per namespace."""
    types = LoadTypeLibraries([Path(p) for p in type_dirs])
    return {ns: len(lib.types) for ns, lib in types.items()}


def compile_uniques(unique_dirs: list[str], type_dirs: list[str]) -> dict:
    """Compile each unique-resource dir into a DataInstanceLibrary."""
    types = LoadTypeLibraries([Path(p) for p in type_dirs])
    results = [CompileUniqueLibrary(Path(d), types) for d in unique_dirs]
    return {"types": {ns: len(lib.types) for ns, lib in types.items()}, "uniques": results}


def compile_transforms(transform_dirs: list[str], type_dirs: list[str]) -> dict:
    """Compile each transform dir; attaches the loaded types and prunes the unused."""
    types = LoadTypeLibraries([Path(p) for p in type_dirs])
    results = [CompileTransformLibrary(Path(d), types) for d in transform_dirs]
    return {"types": {ns: len(lib.types) for ns, lib in types.items()}, "transforms": results}


def build_all(
    type_dirs: list[str],
    transform_dirs: list[str],
    unique_dirs: list[str] | None = None,
) -> dict:
    """Run the full pipeline: types → uniques → transforms."""
    return Build(
        data_type_dirs=[Path(p) for p in type_dirs],
        transform_dirs=[Path(p) for p in transform_dirs],
        unique_dirs=[Path(p) for p in (unique_dirs or [])],
    )


def _assert_metadata_present(dst_path: Path) -> None:
    """Refuse a bundle whose `_metadata/` came out empty.

    `_metadata/` is a build product, not tracked source (see
    `_authoring.py`/AGENTS.md): a fresh checkout has none until something
    compiles it. The hash check above catches a bundle going *stale* after
    a source edit, but it cannot catch a bundle that was *always* empty --
    e.g. vendoring run before the compile step, or a compile that silently
    produced nothing. That failure is silent by nature (the library loads
    and resolves nothing; nothing raises), so this is the one place it gets
    turned into a raise.
    """
    found = list(dst_path.rglob("_metadata"))
    if not found:
        raise ValueError(
            f"vendored bundle at [{dst_path}] carries no _metadata/ at all -- "
            f"it was built from an uncompiled source tree. Compile first "
            f"(`dev/libraries.sh -b` or `metasmith build all ...`), then re-vendor."
        )
    empty = [d for d in found if not any(d.rglob("*"))]
    if empty:
        rel = ", ".join(str(d.relative_to(dst_path)) for d in sorted(empty))
        raise ValueError(
            f"vendored bundle at [{dst_path}] carries empty _metadata/ dirs: {rel}. "
            f"A library with no metadata loads and resolves nothing, silently -- "
            f"this is refused rather than shipped. Compile first, then re-vendor."
        )

    # A present, non-empty `_metadata/` is still not proof of a usable library:
    # a compile that resolved no sources writes `types/` and an index whose
    # manifest is `{}`, which is the same silence one directory further in.
    # The manifest is what the loader iterates, so that is what gets asserted.
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
    """Copy each `NAME=PATH` source into `dst/NAME`, replacing `dst` wholesale,
    and stamp the source content hash alongside it so `check_vendor_library`
    can later detect drift.

    Copies only -- it does not compile. `_metadata/` is a build product
    (`metasmith build all`/`dev/libraries.sh -b`), and the caller must run
    that against the *source* directories named in `srcs` before vendoring,
    or this refuses the empty bundle that copying an uncompiled tree
    produces (see `_assert_metadata_present`).
    """
    mapping = _parse_vendor_srcs(srcs)
    dst_path = Path(dst).resolve()
    content_hash = _library_content_hash(mapping)

    if dst_path.exists():
        shutil.rmtree(dst_path)
    dst_path.mkdir(parents=True)
    for name, path in mapping.items():
        shutil.copytree(path, dst_path / name)
    _assert_metadata_present(dst_path)
    (dst_path / "VENDOR_HASH").write_text(content_hash)
    return {"dst": str(dst_path), "vendored": sorted(mapping), "content_hash": content_hash}


def check_vendor_library(srcs: list[str], dst: str) -> dict:
    """Verify an existing vendored bundle at `dst` still matches the live
    content of `srcs` and carries non-empty compiled metadata, without
    copying anything. Raises if either is false."""
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
