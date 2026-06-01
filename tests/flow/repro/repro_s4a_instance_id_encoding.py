"""Pin for the S4a "instance_id double-encoding" trap.

Background: an earlier iteration of the lineage-robustness work stored
`instance_id` as the hex of the ASCII-hex multihash digest (i.e., a
two-level encoding). `TraceIndex.by_slot` then could not look up entries
because trace events carried the inner-hex form while DataInstances
carried the outer-hex form. The S4a-amend fix is that instance_id is
ALWAYS plain hex of the multihash bytes (`multihash_key(...).hex()`).

This pin holds three invariants:

1. **Shape** — a freshly-minted leaf instance_id is plain hex of the
   multihash-prefixed BLAKE3 digest (length 2 + 32 → 68 hex chars).
2. **Idempotence** — `Pack → Unpack → Pack` is the identity on the
   `instance_id` field.
3. **No double-encoding** — the instance_id is NOT the hex of an
   ASCII-hex string (which would have length 136 = 68 * 2).

Catalog reference: see `tests/flow/AGENTS.md` "trap cases".
"""

from __future__ import annotations

import re
from pathlib import Path

from metasmith.caching.keys import BLAKE3_DIGEST_LEN
from metasmith.models.libraries import (
    DataInstanceLibrary,
    DataTypeLibrary,
)
from metasmith.models.solver import Endpoint


_EXPECTED_HEX_LEN = 2 * (2 + BLAKE3_DIGEST_LEN)  # 68 chars
_HEX_RE = re.compile(r"^[0-9a-f]+$")


def _seed_library(tmp_path: Path) -> DataInstanceLibrary:
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    types = DataTypeLibrary()
    types["assembly"] = Endpoint(properties={"assembly"})
    lib.AddTypeLibrary(namespace="mock", lib=types)
    (lib.location / "asm.fa").write_text(">c\nACGT\n", encoding="utf-8")
    lib.AddItem(path=Path("asm.fa"), dtype="mock::assembly")
    return lib


def test_repro_s4a_instance_id_is_plain_hex(tmp_path):
    """Pins S4a: leaf instance_id is plain hex of the multihash-prefixed
    digest, not hex-of-ASCII-hex."""
    lib = _seed_library(tmp_path)
    inst = lib.Get(Path("asm.fa"))

    assert isinstance(inst.instance_id, str), (
        f"instance_id must be a str, got {type(inst.instance_id).__name__}"
    )
    assert _HEX_RE.match(inst.instance_id), (
        f"instance_id is not lowercase hex: {inst.instance_id!r}"
    )
    assert len(inst.instance_id) == _EXPECTED_HEX_LEN, (
        f"instance_id length {len(inst.instance_id)} != expected "
        f"{_EXPECTED_HEX_LEN} (= 2 * (2 + BLAKE3_DIGEST_LEN))"
    )

    # Negative — the double-encoded form would be exactly 2x as long.
    assert len(inst.instance_id) != 2 * _EXPECTED_HEX_LEN, (
        f"instance_id has the legacy double-encoded length "
        f"{2 * _EXPECTED_HEX_LEN}; the S4a fix has regressed"
    )


def test_repro_s4a_pack_unpack_is_idempotent(tmp_path):
    """Pins S4a: Pack → Unpack → Pack is the identity on instance_id.

    Double-encoding manifests as `Unpack(Pack(x)).instance_id` differing
    from `x.instance_id` — the round-trip would re-hex the already-hex
    string. This test ensures Pack stores the same hex string verbatim.
    """
    lib = _seed_library(tmp_path)
    inst = lib.Get(Path("asm.fa"))

    packed_1 = inst.Pack()
    assert packed_1["instance_id"] == inst.instance_id, (
        f"Pack() rewrote instance_id: {inst.instance_id!r} -> "
        f"{packed_1['instance_id']!r}"
    )

    unpacked = inst.Unpack(packed_1, {lib.GetKey(): lib})
    assert unpacked.instance_id == inst.instance_id, (
        f"Unpack() mutated instance_id: {inst.instance_id!r} -> "
        f"{unpacked.instance_id!r}"
    )

    packed_2 = unpacked.Pack()
    assert packed_2["instance_id"] == packed_1["instance_id"], (
        f"second Pack drifted: {packed_1['instance_id']!r} -> "
        f"{packed_2['instance_id']!r}"
    )

    # The hex length must be stable across the round trip — a 2x growth
    # would be the smoking gun of double-encoding.
    assert len(packed_2["instance_id"]) == len(inst.instance_id) == _EXPECTED_HEX_LEN
