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

import cbor2
from blake3 import blake3


# Multihash codes: <https://github.com/multiformats/multicodec/blob/master/table.csv>
BLAKE3_MULTIHASH_CODE = 0x1E
BLAKE3_DIGEST_LEN = 32  # 256-bit
KEY_PREFIX = bytes([BLAKE3_MULTIHASH_CODE, BLAKE3_DIGEST_LEN])

# Hard-breaking version stamp on the lineage payload. Baked into every
# lineage_key so a bump renders pre-v2 cache shards unreachable; the
# sqlite metadata row in CacheStore mirrors it for runtime checks.
LIN_PAYLOAD_VERSION = 2


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
        Static signature of the transform's contract (e.g.
        `TransformInstance._hash`). Captures the input/output type
        topology + protocol identity so that two transforms with the
        same name but different bodies key differently.
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
            "v": LIN_PAYLOAD_VERSION,
            "tk": transform_key,
            "sig": signature,
            "inputs": [
                [slot, instance_id] for slot, instance_id in sorted_inputs
            ],
        }
    )
    return KEY_PREFIX + _digest(payload)
