"""Deterministic content hash of the metasmith source tree.

Written to build_hash.txt at build time; read by constants.BUILD_HASH at
runtime. Kept dependency-free so dev.sh can invoke it before any pip install.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parent
BUILD_HASH_FILE = MODULE_PATH / "build_hash.txt"

EXCLUDE_NAMES = frozenset({"build_hash.txt"})
EXCLUDE_PARTS = frozenset({"__pycache__"})


def _included(p: Path) -> bool:
    if not p.is_file(): return False
    if p.name in EXCLUDE_NAMES: return False
    if EXCLUDE_PARTS & set(p.parts): return False
    if p.suffix == ".pyc": return False
    return True


def compute_build_hash(root: Path = MODULE_PATH) -> str:
    h = hashlib.md5()
    for p in sorted(q for q in root.rglob("*") if _included(q)):
        h.update(str(p.relative_to(root)).encode())
        h.update(b"\0")
        h.update(p.read_bytes())
        h.update(b"\0")
    return h.hexdigest()[:7]


def write_build_hash(root: Path = MODULE_PATH) -> str:
    h = compute_build_hash(root)
    BUILD_HASH_FILE.write_text(h)
    return h


if __name__ == "__main__":
    import sys
    print(write_build_hash() if "--write" in sys.argv else compute_build_hash())
