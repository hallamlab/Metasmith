from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

from ...caching.keys import multihash_key, stat_multihash_key
from ...hashing import KeyGenerator
from ..paths import is_deferred


def stat_leaf_id(abs_path: Path, fork_id: str | None = None) -> str | None:
    # The leaf identity of whatever is at `abs_path` on this host, or None if
    # this host cannot see it. One stat, no bytes read -- a directory costs the
    # same as a file, where a content address costs a whole-tree walk.
    #
    # The path is folded in as given: absolute, on the filesystem being stat'd.
    # Callers that mint on one host for a file that lives on another get None
    # and must fall back; the honest re-derivation happens at staging time, on
    # the host that owns the file.
    try:
        st = abs_path.stat()
    except OSError:
        return None
    key = stat_multihash_key(abs_path, st.st_mtime_ns)
    if fork_id:
        key = multihash_key(key + b"\x00fork:" + fork_id.encode("utf-8"))
    return key.hex()


class _LeafIdentity:
    def _mint_leaf_id(self, path: Path) -> str:
        key = None
        if is_deferred(path):
            key = multihash_key(b"deferred\x00" + str(path).encode("utf-8")).hex()
        elif not os.environ.get("METASMITH_LEAF_RANDOM"):
            abs_path = path if path.is_absolute() else self.location / path
            key = stat_leaf_id(abs_path, self.fork_id)
        if key is None:
            raw = uuid.uuid4().bytes + time.time_ns().to_bytes(16, "big", signed=False)
            key = multihash_key(raw).hex()
        self.instance_meta[path] = {
            "instance_id": key,
            "origin": "leaf",
            "lineage_payload": None,
            "fork_id": self.fork_id,
        }
        return self.instance_meta[path]["instance_id"]

    def _refork_leaf_id(self, path: Path, entry: dict) -> dict:
        abs_path = path if path.is_absolute() else self.location / path
        if not os.environ.get("METASMITH_LEAF_RANDOM") and abs_path.exists():
            self._mint_leaf_id(path)
        else:
            seed = f"{entry['instance_id']}\x00fork:{self.fork_id}".encode("utf-8")
            self.instance_meta[path] = {
                "instance_id": multihash_key(seed).hex(),
                "origin": "leaf",
                "lineage_payload": None,
                "fork_id": self.fork_id,
            }
        return self.instance_meta[path]

    def _resolve_instance_meta(self, path: Path, dtype_name: str) -> dict:
        if self.is_pinned:
            entry = self.instance_meta.get(path)
            if entry is None:
                from .pinned import PinnedLibraryError
                raise PinnedLibraryError(
                    f"[{path}] is not recorded in the pinned library at"
                    f" [{self.location}], and a pinned library will not mint an"
                    " id. Rebuild and re-pin it."
                )
            return entry
        if path in self.instance_meta:
            entry = self.instance_meta[path]
            if entry.get("fork_id") == self.fork_id:
                return entry
            if entry.get("origin", "leaf") != "leaf":
                entry["fork_id"] = self.fork_id
                return entry
            return self._refork_leaf_id(path, entry)
        _, legacy_id = KeyGenerator.FromStr("".join([
            str(path), dtype_name, self.GetKey(),
        ]), l=10)
        self.instance_meta[path] = {
            "instance_id": legacy_id,
            "origin": "leaf",
            "lineage_payload": None,
            "fork_id": self.fork_id,
        }
        return self.instance_meta[path]
