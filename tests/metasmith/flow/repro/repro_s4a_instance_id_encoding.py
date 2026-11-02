from __future__ import annotations

import re
from pathlib import Path

from metasmith.caching.keys import BLAKE3_DIGEST_LEN
from metasmith.models.libraries import (
    DataInstanceLibrary,
    DataTypeLibrary,
)
from metasmith.models.solver import Endpoint


_EXPECTED_HEX_LEN = 2 * (2 + BLAKE3_DIGEST_LEN)
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

    assert len(inst.instance_id) != 2 * _EXPECTED_HEX_LEN, (
        f"instance_id has the legacy double-encoded length "
        f"{2 * _EXPECTED_HEX_LEN}; the S4a fix has regressed"
    )


def test_repro_s4a_pack_unpack_is_idempotent(tmp_path):
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

    assert len(packed_2["instance_id"]) == len(inst.instance_id) == _EXPECTED_HEX_LEN
