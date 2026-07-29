"""Where a cache entry lives on disk — one definition, four readers.

The compile side (which emits the path of a cache hit into the generated
workflow), the promote side (which writes the entry), the telemetry side
(which reads back `logs/`), and the ops side (which lists and prunes) all
have to agree on the same directory names. They agreed by coincidence
before this module: each open-coded `root / hex[:2] / hex[2:] / "out"`.
A shard scheme is exactly the kind of thing that gets "improved" in one
place, and the failure mode is a silent cache miss, not an error.

Deliberately pathlib-only. `caching.store` drags in sqlite and CBOR, which
is why codegen imports it lazily; this module is safe to import at module
scope from anywhere.

The `imported/` space (`ops.data.import_library`) is keyed by a leaf
instance_id rather than a cache key and is a separate namespace under the
same root — see :func:`imported_shard_dir`. It only looks like the same
layout.
"""

from __future__ import annotations

from pathlib import Path

# The cache root's directory name under an agent home.
CACHE_DIR_NAME = "task_cache"

# Subdirectories of a promoted shard.
OUT_DIR_NAME = "out"
LOGS_DIR_NAME = "logs"
MANIFEST_NAME = "manifest.cbor"

# Namespace for `data import-library` rows, keyed by instance_id.
IMPORTED_DIR_NAME = "imported"

_SHARD_PREFIX_LEN = 2


def default_cache_root(agent_home: Path) -> Path:
    """`<agent_home>/task_cache` — the convention every agent follows."""
    return Path(agent_home) / CACHE_DIR_NAME


def shard_dir(cache_root: Path, key_hex: str) -> Path:
    """`<cache_root>/<first 2 hex>/<rest>` — bounded directory fanout."""
    return (
        Path(cache_root) / key_hex[:_SHARD_PREFIX_LEN] / key_hex[_SHARD_PREFIX_LEN:]
    )


def staging_dir(cache_root: Path, key_hex: str) -> Path:
    """`<cache_root>/<key>.tmp` — pre-seal staging, renamed onto the shard.

    Flat at the root rather than beside the shard so the seal is a rename
    within one directory tree, and so the lock file (`<key>.lock`) sits
    next to what it guards.
    """
    return Path(cache_root) / f"{key_hex}.tmp"


def lock_file(cache_root: Path, key_hex: str) -> Path:
    """`<cache_root>/<key>.lock` — O_EXCL promote lock."""
    return Path(cache_root) / f"{key_hex}.lock"


def out_dir(shard: Path) -> Path:
    """The `out/` subdir holding a shard's promoted output files."""
    return Path(shard) / OUT_DIR_NAME


def logs_dir(shard: Path) -> Path:
    """The `logs/` subdir holding a shard's captured `.command.*` files."""
    return Path(shard) / LOGS_DIR_NAME


def imported_shard_dir(cache_root: Path, instance_id_hex: str) -> Path:
    """`<cache_root>/imported/<2>/<rest>` — a different key space.

    Rows written by `data import-library` are addressed by a leaf
    `instance_id`, not by a cache key, and their files stay in the imported
    library rather than being copied into `out/`. Kept apart from
    :func:`shard_dir` on purpose: the two only look alike.
    """
    return shard_dir(Path(cache_root) / IMPORTED_DIR_NAME, instance_id_hex)
