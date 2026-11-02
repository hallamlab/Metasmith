from __future__ import annotations

import hashlib
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parent
BUILD_HASH_FILE = MODULE_PATH / "build_hash.txt"

EXCLUDE_NAMES = frozenset({"build_hash.txt"})
EXCLUDE_PARTS = frozenset({"__pycache__"})


def _included(p: Path, exclude_parts: frozenset[str] = EXCLUDE_PARTS) -> bool:
    if not p.is_file(): return False
    if p.name in EXCLUDE_NAMES: return False
    if exclude_parts & set(p.parts): return False
    if p.suffix == ".pyc": return False
    return True


def compute_build_hash(root: Path = MODULE_PATH, *, also_exclude: frozenset[str] = frozenset()) -> str:
    exclude_parts = EXCLUDE_PARTS | also_exclude
    h = hashlib.md5()
    for p in sorted(q for q in root.rglob("*") if _included(q, exclude_parts)):
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
