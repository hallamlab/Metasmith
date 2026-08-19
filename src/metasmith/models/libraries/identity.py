from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

from ...caching.keys import content_multihash_key, multihash_key, tree_multihash_key
from ...hashing import KeyGenerator
from ..paths import is_deferred


class _LeafIdentity:
    def _mint_leaf_id(self, path: Path) -> str:
        key = None
        if is_deferred(path):
            key = multihash_key(b"deferred\x00" + str(path).encode("utf-8"))
        elif not os.environ.get("METASMITH_LEAF_RANDOM"):
            abs_path = path if path.is_absolute() else self.location / path
            try:
                fold_path = abs_path.relative_to(self.location)
            except ValueError:
                fold_path = path
            try:
                content = None
                if abs_path.is_file():
                    content = content_multihash_key(abs_path)
                elif abs_path.is_dir():
                    content = tree_multihash_key(abs_path)
                if content is not None:
                    fold = str(fold_path).encode("utf-8")
                    if self.fork_id:
                        fold += b"\x00fork:" + self.fork_id.encode("utf-8")
                    key = multihash_key(content + fold)
            except OSError:
                key = None
        if key is None:
            raw = uuid.uuid4().bytes + time.time_ns().to_bytes(16, "big", signed=False)
            key = multihash_key(raw)
        self.instance_meta[path] = {
            "instance_id": key.hex(),
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
