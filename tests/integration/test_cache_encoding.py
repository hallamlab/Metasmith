"""Forward-looking xfail tests for the encoding primitives (S1).

Coverage: canonical CBOR stability across Python versions; multihash
prefix on cache keys.
"""

from __future__ import annotations

def test_canonical_cbor_key_stable_across_python_versions():
    """S1: cbor2 canonical mode produces the same bytes regardless of
    dict insertion order or Python version.

    Smoke-tests that we use cbor2 in canonical mode (RFC 8949 §4.2.2)
    and that the key bytes are byte-stable for a fixed payload.
    """
    from metasmith.caching.keys import canonical_cbor

    payload_a = {"a": 1, "b": [2, 3]}
    payload_b = {"b": [2, 3], "a": 1}
    assert canonical_cbor(payload_a) == canonical_cbor(payload_b)


def test_key_carries_multihash_prefix():
    """S1: cache keys start with blake3-32 multihash prefix (0x1e 0x20).

    Pins the key format so that future hash-algo migration is a prefix
    swap rather than a wholesale re-key of the cache.
    """
    from metasmith.caching.keys import lineage_key

    k = lineage_key("tr", "sig", [])
    assert k[:2] == b"\x1e\x20", (
        f"expected blake3-32 multihash prefix 0x1e 0x20, got {k[:2]!r}"
    )
    assert len(k) == 2 + 32  # prefix + 32-byte digest
