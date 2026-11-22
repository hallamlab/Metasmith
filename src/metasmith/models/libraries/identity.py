from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

from ...caching.keys import _file_digest, multihash_key, stat_multihash_key
from ...hashing import KeyGenerator
from ...logging import Log
from ..paths import is_deferred
from .pinned import _stamp

# A file this small is witnessed by its bytes rather than its mtime, so a copy or
# checkout that only moves the mtime keeps its identity.
_DIGEST_MAX_BYTES = 1 << 20


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


def leaf_stamp(abs_path: Path) -> dict | None:
    # What a leaf looked like when its id was minted, kept beside the id so a
    # later load can tell the file changed. Not part of the id.
    try:
        stamp = _stamp(abs_path)
        if stamp["kind"] == "file" and stamp["size"] <= _DIGEST_MAX_BYTES:
            stamp["digest"] = _file_digest(abs_path, 1 << 20).hex()
    except OSError:
        return None
    return stamp


def _leaf_changed(recorded: dict, observed: dict) -> bool:
    if "digest" in recorded:
        return observed.get("digest") != recorded["digest"]
    return observed != recorded


class _LeafIdentity:
    def _mint_leaf_id(self, path: Path) -> str:
        key = stamp = None
        if is_deferred(path):
            key = multihash_key(b"deferred\x00" + str(path).encode("utf-8")).hex()
        elif not os.environ.get("METASMITH_LEAF_RANDOM"):
            abs_path = path if path.is_absolute() else self.location / path
            key = stat_leaf_id(abs_path, self.fork_id)
            stamp = leaf_stamp(abs_path) if key is not None else None
        if key is None:
            raw = uuid.uuid4().bytes + time.time_ns().to_bytes(16, "big", signed=False)
            key = multihash_key(raw).hex()
        self.instance_meta[path] = {
            "instance_id": key,
            "origin": "leaf",
            "lineage_payload": None,
            "fork_id": self.fork_id,
            # This process invented the number. `Pack` does not carry the flag,
            # so a library that is saved and loaded again is a record rather
            # than a mint -- which is the whole difference the given path cares
            # about. See `plan._refuse_unminted_givens`.
            "minted": True,
        }
        if stamp is not None:
            self.instance_meta[path]["stamp"] = stamp
        return self.instance_meta[path]["instance_id"]

    def _remint_if_changed(self, path: Path, entry: dict) -> dict:
        # A leaf edited after its id was recorded gets a new one, or every step
        # that reads it is served the cache built from the old bytes. A recorded
        # id comes back unchanged while the file does, so existing caches hold.
        if entry.get("origin", "leaf") != "leaf" or is_deferred(path) \
                or os.environ.get("METASMITH_LEAF_RANDOM"):
            return entry
        abs_path = self._abs(path)
        observed = leaf_stamp(abs_path)
        if observed is None:
            return entry
        recorded = entry.get("stamp")
        if recorded is None:
            # An index written before stamps were. Its id is known to describe
            # these bytes only while it still re-derives from them.
            if stat_leaf_id(abs_path, self.fork_id) == entry["instance_id"]:
                entry["stamp"] = observed
            return entry
        if not _leaf_changed(recorded, observed):
            return entry
        old = entry["instance_id"]
        self._mint_leaf_id(path)
        fresh = self.instance_meta[path]
        fresh.pop("minted", None)
        if fresh["instance_id"] == old:
            # New bytes under the same path and mtime, as `cp -p` leaves them.
            seed = f"{old}\x00changed:{sorted(observed.items())}".encode("utf-8")
            fresh["instance_id"] = multihash_key(seed).hex()
        Log.Info(f"[{abs_path}] changed since it was recorded, so its identity moved")
        return fresh

    def _refork_leaf_id(self, path: Path, entry: dict) -> dict:
        # A fork moves an identity the library already held, deliberately, to
        # get a new task key. Like an invalidate it acts on a record rather
        # than inventing one, so it clears the mint mark the given path reads.
        abs_path = path if path.is_absolute() else self.location / path
        if not os.environ.get("METASMITH_LEAF_RANDOM") and abs_path.exists():
            self._mint_leaf_id(path)
            self.instance_meta[path].pop("minted", None)
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
                return self._remint_if_changed(path, entry)
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
            "minted": True,
        }
        return self.instance_meta[path]
