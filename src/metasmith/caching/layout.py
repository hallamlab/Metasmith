# One definition of where a cache entry lives, for four readers that must agree:
# compile (emits a hit's path into the generated workflow), promote (writes the
# entry), telemetry (reads back `logs/`), and ops (lists and prunes). A shard
# scheme is the kind of thing that gets "improved" in one place, and the failure
# mode is a silent cache miss rather than an error.
#
# Deliberately pathlib-only, so this is safe to import at module scope from
# anywhere: `caching.store` drags in sqlite and CBOR.
#
# `imported/` is keyed by a leaf instance_id rather than a cache key and is a
# separate namespace under the same root. It only looks like the same layout.
from __future__ import annotations

from pathlib import Path

CACHE_DIR_NAME = "task_cache"

OUT_DIR_NAME = "out"
LOGS_DIR_NAME = "logs"
MANIFEST_NAME = "manifest.cbor"

IMPORTED_DIR_NAME = "imported"

_SHARD_PREFIX_LEN = 2


def default_cache_root(agent_home: Path) -> Path:
    return Path(agent_home) / CACHE_DIR_NAME


def shard_dir(cache_root: Path, key_hex: str) -> Path:
    return (
        Path(cache_root) / key_hex[:_SHARD_PREFIX_LEN] / key_hex[_SHARD_PREFIX_LEN:]
    )


def out_dir(shard: Path) -> Path:
    return Path(shard) / OUT_DIR_NAME


def logs_dir(shard: Path) -> Path:
    return Path(shard) / LOGS_DIR_NAME


def imported_shard_dir(cache_root: Path, instance_id_hex: str) -> Path:
    return shard_dir(Path(cache_root) / IMPORTED_DIR_NAME, instance_id_hex)
