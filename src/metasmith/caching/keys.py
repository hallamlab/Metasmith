"""Canonical encoding + multihash-prefixed cache keys (S1).

Two pieces of borrowed wheel:

* `canonical_cbor(payload)` wraps `cbor2.dumps(..., canonical=True)`.
  RFC 8949 §4.2.2 deterministic encoding: map keys are sorted by their
  encoded bytes (length-first, then lexicographic), integers use the
  shortest representation, and floats use the shortest representation
  that round-trips. The result is byte-stable across Python versions
  and dict insertion orders, which is what makes lineage_key
  reproducible across workspaces.

* The multihash prefix convention (`<algo-code><length><digest>`):
  blake3-32 = `0x1e 0x20`. Adopting the prefix means a future hash
  migration is a prefix swap and a cache walk, not a wholesale re-key.
  We use only the prefix convention — no IPFS / IPLD machinery.

The single load-bearing function is `lineage_key(transform_key,
signature, sorted_inputs)`. Inputs are `(slot_key, instance_id_bytes)`
pairs; sorting the input list is the caller's job (a sort by slot_key
produces a stable order regardless of declaration order in the model).
"""

from __future__ import annotations

import os
from pathlib import Path as _Path

import cbor2
from blake3 import blake3


# Multihash codes: <https://github.com/multiformats/multicodec/blob/master/table.csv>
BLAKE3_MULTIHASH_CODE = 0x1E
BLAKE3_DIGEST_LEN = 32  # 256-bit
KEY_PREFIX = bytes([BLAKE3_MULTIHASH_CODE, BLAKE3_DIGEST_LEN])

# Cache-key epoch. Baked into every lineage_key so a bump renders pre-epoch
# cache shards unreachable; the sqlite metadata row in CacheStore mirrors it
# for runtime checks. Bumped 2 -> 3 in R5 when the lineage signature began
# folding the transform's protocol-body identity (F1 fix).
#
# DELIBERATELY SEPARATE from LIN_PAYLOAD_VERSION below: the cache epoch tracks
# cache-key *semantics*, whereas the wire version tracks the Nextflow-channel
# envelope *shape*. They were one constant until R5; bumping it for the F1
# cache-key change silently broke the on-wire lin envelope, whose Groovy
# emitter (`workflow.py` -> `Orchestrator.JsonforEcho([v:2, ...])`) hardcoded
# the wire version and did not move in lockstep. Keeping them independent
# means a future cache-semantics bump never again desyncs the wire protocol.
CACHE_KEY_VERSION = 3

# On-wire LinPayload envelope version (models/lineage.py). Tracks the SHAPE of
# the `{"v": N, "entries": [...]}` value carried on the Nextflow channel. Do
# not bump this for cache-key changes (bump CACHE_KEY_VERSION instead) — only
# when the envelope shape itself changes.
#
# v3: `entries` is a LIST of per-batch-member maps, matching the list of
#     indexes `_collateBatch` builds. v2 carried a single map (`index[0]`),
#     which silently discarded every member after the first.
# v4: each member may carry `PROV` -- the per-item index maps, one per entry
#     of `FILES`, kept un-flattened so a protocol can ask which item of one
#     grouped slot another item descends from. Additive, but NOT compatible:
#     a v3 parser hashes every value of the raw entry and dies on a nested
#     one, and the `.nf` is written by the client's metasmith while bootstrap
#     runs from the agent container's, so the two ends can be different
#     builds. The bump turns that into a named refusal.
#
# The emitter interpolates this constant (`nextflow_codegen.LIN_ECHO_EXPR`)
# rather than restating it, so emitter and parser cannot drift.
LIN_PAYLOAD_VERSION = 4


def canonical_cbor(payload) -> bytes:
    """Canonical CBOR encoding per RFC 8949 §4.2.2 (deterministic).

    Wraps cbor2.dumps with canonical=True so map-key order and integer
    representation are uniquely determined by the value, not by the
    caller's dict insertion order.
    """
    return cbor2.dumps(payload, canonical=True)


def _digest(payload: bytes) -> bytes:
    return blake3(payload).digest(length=BLAKE3_DIGEST_LEN)


def multihash_key(payload: bytes) -> bytes:
    """Return `<algo-code><length><blake3-digest(payload)>`.

    Intended for opaque blobs (used by tests + internal callers). Most
    cache-key callers should use `lineage_key` which builds the payload
    via canonical CBOR.
    """
    return KEY_PREFIX + _digest(payload)


def content_multihash_key(path, *, chunk_size: int = 1 << 20) -> bytes:
    """Return the multihash key over a file's raw bytes, streamed.

    Same encoding as `multihash_key(open(path,'rb').read())` but reads in
    `chunk_size` chunks so large inputs never fully materialize in memory.
    Used for content-addressed *leaf* identity: two independent runs that
    see byte-identical input files mint the same leaf instance_id, so their
    downstream cache_keys match and the second run resumes from the cache
    (cross-run reentrancy). The caller is responsible for confirming the
    path is a readable regular file; OSError propagates.
    """
    hasher = blake3()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return KEY_PREFIX + hasher.digest(length=BLAKE3_DIGEST_LEN)


def tree_multihash_key(path, *, chunk_size: int = 1 << 20) -> bytes:
    """Return the multihash key over a DIRECTORY's contents, recursively.

    The file analogue of `content_multihash_key`, and it exists for the same
    reason: a leaf that is a directory -- a vendored python package, a profile
    database -- otherwise falls through to a random per-call id, so an unchanged
    tree gets a new identity on every library recompile and invalidates every
    downstream cache entry that ever read it. That is a silent miss, not a
    silent hit, but for a multi-day bake it is expensive enough to be a bug.

    Every entry contributes its LIBRARY-RELATIVE path and its bytes, so a rename
    with no content change and a content change with no rename both move the
    digest. Directories are not hashed as entries themselves: an empty directory
    carries nothing a consumer can read, and git cannot represent one anyway, so
    counting it would make a tree's identity depend on whether it survived a
    checkout. A symlink contributes its TARGET STRING rather than the bytes it
    points at -- following it would make the digest depend on something outside
    the tree, and `Logistics` copies symlinks as symlinks.

    Per-file digests are memoized on `(path, size, mtime_ns)`, so re-staging an
    untouched tree costs one stat per file rather than a full read. The cache is
    keyed on mtime and can therefore be fooled by a write that preserves both
    size and mtime -- which is why it is a within-process cache over a tree the
    build step just wrote, and never a substitute for the digest itself.

    OSError propagates: an unreadable entry in a tree being addressed is not
    something to paper over with a partial digest.
    """
    root = _Path(path)
    hasher = blake3()
    hasher.update(b"tree\x00")
    for p in sorted(root.rglob("*")):
        rel = str(p.relative_to(root)).encode("utf-8")
        if p.is_symlink():
            hasher.update(b"l\x00" + rel + b"\x00" + os.readlink(p).encode("utf-8") + b"\x00")
            continue
        if not p.is_file():
            continue
        hasher.update(b"f\x00" + rel + b"\x00" + _file_digest(p, chunk_size) + b"\x00")
    return KEY_PREFIX + hasher.digest(length=BLAKE3_DIGEST_LEN)


_FILE_DIGEST_CACHE: dict[tuple[str, int, int], bytes] = {}


def _file_digest(path, chunk_size: int) -> bytes:
    st = path.stat()
    ck = (str(path), st.st_size, st.st_mtime_ns)
    hit = _FILE_DIGEST_CACHE.get(ck)
    if hit is not None:
        return hit
    hasher = blake3()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    digest = hasher.digest(length=BLAKE3_DIGEST_LEN)
    _FILE_DIGEST_CACHE[ck] = digest
    return digest


def lineage_key(
    transform_key: str,
    signature: str,
    sorted_inputs: list[tuple[str, bytes]],
) -> bytes:
    """Compute the lineage-addressed cache key for one transform invocation.

    Parameters
    ----------
    transform_key:
        The transform's stable identifier (e.g. `TransformInstance._key`).
        Embedded verbatim into the payload.
    signature:
        Static signature of the transform's contract. As of
        CACHE_KEY_VERSION 3 the caller builds this as
        `f"{model._hash}:{_protocol_source_hash}"` (see
        `workflow.py`) so it captures BOTH the input/output type
        topology AND the transform's protocol-body identity (a digest
        of the definition-file bytes). Two transforms that share an
        in/out type topology but differ in body — or two entirely
        different tools with the same declared types — therefore key
        differently, and editing a transform's protocol busts the
        cross-run cache instead of serving stale output. (Pre-v3 this
        was topology-only, which false-hit on protocol edits.)
    sorted_inputs:
        Sequence of `(slot_key, instance_id_bytes)` pairs. The caller
        must sort by `slot_key` so the encoding is order-independent.
        `instance_id_bytes` should already be the multihash-prefixed
        form for downstream entries (S2's `origin="lineage"`/`"imported"`)
        or the synthesized leaf-identity bytes for `origin="leaf"`.

    Returns
    -------
    The multihash-prefixed digest (length 2 + BLAKE3_DIGEST_LEN).
    """
    payload = canonical_cbor(
        {
            "v": CACHE_KEY_VERSION,
            "tk": transform_key,
            "sig": signature,
            "inputs": [
                [slot, instance_id] for slot, instance_id in sorted_inputs
            ],
        }
    )
    return KEY_PREFIX + _digest(payload)
