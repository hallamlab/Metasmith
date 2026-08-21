from __future__ import annotations

import os
from pathlib import Path

import cbor2
from blake3 import blake3


BLAKE3_MULTIHASH_CODE = 0x1E
BLAKE3_DIGEST_LEN = 32
KEY_PREFIX = bytes([BLAKE3_MULTIHASH_CODE, BLAKE3_DIGEST_LEN])

# Cache-key epoch. Baked into every lineage_key so a bump renders pre-epoch
# cache shards unreachable; the sqlite metadata row in CacheStore mirrors it
# for runtime checks. Bumped 2 -> 3 in R5 when the lineage signature began
# folding the transform's protocol-body identity (F1 fix). Bumped 3 -> 4 when a
# step's inputs began naming the producing step's slot id instead of the
# transform archetype's, so a downstream key now moves when its producer does.
#
# DELIBERATELY SEPARATE from LIN_PAYLOAD_VERSION below: the cache epoch tracks
# cache-key *semantics*, whereas the wire version tracks the Nextflow-channel
# envelope *shape*. They were one constant until R5; bumping it for the F1
# cache-key change silently broke the on-wire lin envelope, whose Groovy
# emitter (`workflow.py` -> `Orchestrator.JsonforEcho([v:2, ...])`) hardcoded
# the wire version and did not move in lockstep. Keeping them independent
# means a future cache-semantics bump never again desyncs the wire protocol.
CACHE_KEY_VERSION = 4

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
    return cbor2.dumps(payload, canonical=True)


def _digest(payload: bytes) -> bytes:
    return blake3(payload).digest(length=BLAKE3_DIGEST_LEN)


def multihash_key(payload: bytes) -> bytes:
    return KEY_PREFIX + _digest(payload)


def content_multihash_key(path, *, chunk_size: int = 1 << 20) -> bytes:
    hasher = blake3()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return KEY_PREFIX + hasher.digest(length=BLAKE3_DIGEST_LEN)


def stat_multihash_key(abs_path, mtime_ns: int) -> bytes:
    # Identity of a leaf input as *where it is and when it last changed*, which
    # is the only identity derivable on the host that owns a 24 GB reference
    # without reading it. The path is the absolute one on that host, so an id
    # minted here is meaningful only against that filesystem -- two hosts
    # holding identical bytes do not agree, and that is the trade this makes.
    payload = (
        b"stat\x00" + str(abs_path).encode("utf-8")
        + b"\x00" + str(int(mtime_ns)).encode("ascii")
    )
    return multihash_key(payload)


def tree_multihash_key(path, *, chunk_size: int = 1 << 20, force: bool = False) -> bytes:
    root = Path(path)
    hasher = blake3()
    hasher.update(b"tree\x00")
    for p in sorted(root.rglob("*")):
        rel = str(p.relative_to(root)).encode("utf-8")
        if p.is_symlink():
            hasher.update(b"l\x00" + rel + b"\x00" + os.readlink(p).encode("utf-8") + b"\x00")
            continue
        if not p.is_file():
            continue
        hasher.update(b"f\x00" + rel + b"\x00" + _file_digest(p, chunk_size, force) + b"\x00")
    return KEY_PREFIX + hasher.digest(length=BLAKE3_DIGEST_LEN)


_FILE_DIGEST_CACHE: dict[tuple[str, int, int], bytes] = {}


def _file_digest(path, chunk_size: int, force: bool = False) -> bytes:
    st = path.stat()
    ck = (str(path), st.st_size, st.st_mtime_ns)
    # `force` is for the deep verify, which asks whether these bytes are still the
    # ones an id was derived from. The memo is keyed on the same `(size, mtime_ns)`
    # a same-size in-place edit preserves, so serving it would answer with the
    # digest of the bytes that were there when the memo was filled.
    hit = None if force else _FILE_DIGEST_CACHE.get(ck)
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
