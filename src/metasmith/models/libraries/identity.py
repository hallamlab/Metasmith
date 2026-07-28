"""How a leaf gets its identity -- the one thing that decides cache reuse.

A leaf's `instance_id` is `multihash(blake3(file_bytes) || relpath)` when the
file is present at `AddItem` time, which is what makes two independent runs
over identical inputs hit the same cache shards with no import step. Folding
the relative path in is not decoration: pure content-addressing collapses every
degenerate-but-distinct input -- N empty files, byte-identical samples -- onto
one id, which flattens fan-out and trips the solver's O(n^2) collision path.
Absent or remote inputs fall back to a random per-call id and get no reuse.

Mixed into `DataInstanceLibrary` rather than left inline because a change here
silently invalidates or false-hits every cached run, and that deserves to be a
file someone can read end to end. `_calculate_key` / `GetKey` / `__hash__`
deliberately stayed on the class: they key the *library*, not a leaf, and
moving a `__hash__` into a mixin is how one goes missing.

`..caching.keys` is imported at function scope on purpose -- `caching` reaches
back into this package, so a top-level import here is a genuine cycle.
"""

from __future__ import annotations

import time
from pathlib import Path

from ...hashing import KeyGenerator


class _LeafIdentity:
    def _mint_leaf_id(self, path: Path) -> str:
        """Create a leaf instance_id for `path`.

        Cross-run reentrancy (R1): when the resolved path is a readable
        regular file at mint time, the id is derived from the file's
        content digest AND its library-relative path —
        `multihash(blake3(file_bytes) || relpath)`. Two independent runs
        that lay the same input bytes at the same relative path mint the
        *same* leaf id, so their downstream cache_keys match and the second
        run resumes from the cache without a manual `metasmith data
        import-library` bridge.

        The relative path is folded in (not content alone) so that two
        DISTINCT inputs which happen to share bytes — e.g. N empty/degenerate
        files, or two samples with byte-identical reads — keep DISTINCT
        identities. Pure content-addressing would collapse them to one leaf,
        which both corrupts fan-out (N inputs → 1 identity) and re-triggers
        the solver's O(n^2) id-collision path. Content is still part of the
        key, so a different file reusing a path can never cause a false hit.

        When the file is absent/unreadable at mint time (remote or lazily
        materialized inputs), we fall back to the legacy unique-per-call id
        (uuid4 + time_ns via the multihash encoding). Such leaves get no
        cross-run reuse — acceptable, and it preserves the old behaviour
        exactly for the no-file case.

        Set METASMITH_LEAF_RANDOM=1 to force the legacy random id even when
        the file is present (opt-out kill-switch). The id is stored in
        self.instance_meta and returned. `origin` stays "leaf" either way —
        a content-addressed input is still a user-supplied leaf.
        """
        import os
        import uuid

        from ...caching.keys import (
            content_multihash_key,
            multihash_key,
        )

        key = None
        if not os.environ.get("METASMITH_LEAF_RANDOM"):
            abs_path = path if path.is_absolute() else self.location / path
            # R5 (F2 fix): fold the LIBRARY-RELATIVE path, not the raw argument.
            # Two runs may add the same file via an absolute path on one host
            # and a relative path on another (or with different home roots);
            # folding str(path) verbatim made their leaf ids diverge → cross-run
            # / cross-host cache miss. Normalizing to the path relative to the
            # library location makes the id host-independent while still
            # distinguishing distinct in-library paths. Falls back to the raw
            # path for inputs that live outside the library root.
            try:
                fold_path = abs_path.relative_to(self.location)
            except ValueError:
                fold_path = path
            try:
                if abs_path.is_file():
                    # content digest ⊕ library-relative path → stable across
                    # runs/hosts yet distinct per (path, content) pair.
                    content = content_multihash_key(abs_path)
                    fold = str(fold_path).encode("utf-8")
                    if self.fork_id:
                        # A fork is the user saying "treat these inputs as new"
                        # and its whole purpose is to discard cache reuse. That
                        # used to happen for free, because ids folded in the
                        # library key. Content+path addressing severed it, and
                        # cache keys are a pure function of instance ids -- so
                        # without this the fork mints identical ids and replays
                        # the original run's cached output.
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
        """Re-derive a leaf id after the library's fork id changed.

        A fork exists to say "treat these inputs as new", and cache keys
        are a pure function of instance ids -- so an id that survives a
        fork verbatim replays the original run's cached output. Ids used
        to fold in the library key, which made this automatic; content+path
        addressing severed it.

        A file present at re-fork time goes back through the content-addressed
        mint, which folds the fork id in. For an absent one (remote, or
        lazily materialized) there is no content to hash, so the new id is
        derived from the old -- deterministic across loads rather than
        re-randomizing on every one.
        """
        import os

        from ...caching.keys import multihash_key

        abs_path = path if path.is_absolute() else self.location / path
        if not os.environ.get("METASMITH_LEAF_RANDOM") and abs_path.is_file():
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
        """Return the {instance_id, origin, lineage_payload} entry for path.

        First lookup is self.instance_meta. A miss represents either a
        legacy library that pre-dates per-path metadata, or an in-process
        DataInstance constructed for a path the library doesn't actually
        track (e.g., a transient view from WithDType on an unrelated lib).
        In both cases we mint a deterministic legacy-shape id so existing
        v0.18 serializations resolve identically.
        """
        if path in self.instance_meta:
            entry = self.instance_meta[path]
            if entry.get("fork_id") == self.fork_id:
                return entry
            if entry.get("origin", "leaf") != "leaf":
                # A lineage/imported id is the hash of how the output was
                # produced. A fork of the library it happens to sit in does
                # not change that, so it is stamped, not re-derived.
                entry["fork_id"] = self.fork_id
                return entry
            return self._refork_leaf_id(path, entry)
        # Legacy fallback: derive instance_id from (path, dtype_name, lib_key)
        # so a v0.18 manifest reloads with stable ids. Marked origin="leaf"
        # per the plan's one-way migration rule.
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
