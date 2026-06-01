"""Cache identity axis.

Covers cache key derivation (lineage_key: transform_key + sorted input
instance_ids → canonical CBOR → blake3 multihash), encoding stability
across Python versions and dict orderings, and the cross-workspace
identity preservation contract that `metasmith data import-library`
relies on.

Coverage: G1 (lineage key shape + multihash prefix), G8 (leaf AddItem
unique), G9 (lineage-only identity is workspace-independent), G10
(cross-workspace import preserves identity), S1 (canonical CBOR
stability).
"""

from __future__ import annotations

from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Encoding primitives (S1)
# ---------------------------------------------------------------------------


def test_canonical_cbor_key_stable_across_dict_orderings():
    """S1: cbor2 canonical mode produces the same bytes regardless of
    dict insertion order.

    Pins that we use cbor2 in canonical mode (RFC 8949 §4.2.2) and that
    the resulting key bytes are byte-stable for a fixed payload. The two
    payloads below are equal as Python dicts but constructed with
    different insertion orders; canonical CBOR must collapse them.
    """
    from metasmith.caching.keys import canonical_cbor

    payload_a = {"a": 1, "b": [2, 3]}
    payload_b = {"b": [2, 3], "a": 1}
    assert canonical_cbor(payload_a) == canonical_cbor(payload_b)


def test_key_carries_multihash_prefix():
    """S1: cache keys start with blake3-32 multihash prefix (0x1e 0x20).

    Pins the key format so future hash-algo migration is a prefix swap
    rather than a wholesale re-key of the cache.
    """
    from metasmith.caching.keys import lineage_key

    k = lineage_key("tr", "sig", [])
    assert k[:2] == b"\x1e\x20", (
        f"expected blake3-32 multihash prefix 0x1e 0x20, got {k[:2]!r}"
    )
    assert len(k) == 2 + 32  # prefix + 32-byte digest


# ---------------------------------------------------------------------------
# Leaf identity (G8) + lineage identity (G9, G10)
# ---------------------------------------------------------------------------


def test_addItem_unique_per_call(tmp_path):
    """G8: leaf instance_ids are minted per AddItem call, not path-derived.

    Two independent library builds — same location, same (path, dtype) —
    produce DataInstances with distinct `instance_id`s, because each
    AddItem call mints a fresh `multihash(blake3, uuid4 || time_ns)`.
    The discriminating fact vs main is that identity used to be a pure
    function of (path, dtype, lib_key); under S2 it is not.
    """
    from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
    from metasmith.models.solver import Endpoint

    types = DataTypeLibrary()
    types["seed"] = Endpoint(properties={"seed"})
    tpath = tmp_path / "types.yml"
    types.Save(tpath)

    def _build_once() -> str:
        lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
        lib.Purge()
        lib.AddTypeLibrary(tpath, namespace="cf")
        (lib.location / "a.txt").write_text("payload\n", encoding="utf-8")
        lib.AddItem(Path("a.txt"), "cf::seed")
        return lib.Get(Path("a.txt")).instance_id

    id1 = _build_once()
    id2 = _build_once()
    assert id1 != id2, (
        "two independent AddItem calls produced the same leaf id; "
        "leaf identity should be unique-per-call (not derived from path)"
    )


def test_lineage_id_static_no_inputs(tmp_path):
    """G9: leaf-less transform output id is identical across workspaces.

    Pinned to S2. A transform with no leaf-origin inputs (e.g.
    download_gtdb) should compute the same output instance_id in two
    fresh workspaces using only its static metadata.
    """
    from metasmith.caching.keys import lineage_key  # noqa: F401  S1+S2

    # Identical (transform_key, signature, inputs=[]) in two independent
    # builds must produce identical bytes.
    k1 = lineage_key("tr.download_gtdb", "sig_v1", [])
    k2 = lineage_key("tr.download_gtdb", "sig_v1", [])
    assert k1 == k2


def test_lineage_id_static_with_inputs(tmp_path):
    """G9: same inputs in different workspaces -> same output id.

    Pinned to S2. With identical input instance_ids (e.g. imported from
    a shared upstream library), the lineage_key must be byte-identical
    even though the workspaces have different absolute paths.
    """
    from metasmith.caching.keys import lineage_key

    inputs = [("dep_a", b"id_aaa"), ("dep_b", b"id_bbb")]
    k1 = lineage_key("tr.align", "sig_v3", inputs)
    k2 = lineage_key("tr.align", "sig_v3", inputs)
    assert k1 == k2


def test_container_change_invalidates(tmp_path):
    """G1: changing the container input flips the output id.

    Pinned to S2. The container is a regular input (no special
    type-space carve-out); swapping its instance_id should change the
    lineage_key.
    """
    from metasmith.caching.keys import lineage_key

    base = lineage_key("tr.align", "sig_v3", [("container", b"img_v1")])
    bumped = lineage_key("tr.align", "sig_v3", [("container", b"img_v2")])
    assert base != bumped


def _build_export_lib(workspace: Path, *, lineage_id_hex: str) -> Path:
    """Build a small library in `workspace` containing one leaf + one
    lineage-origin DataInstance, and serialize it for transport.

    Returns the path to the serialized library directory.
    """
    from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
    from metasmith.models.solver import Endpoint

    types = DataTypeLibrary()
    types["seed"] = Endpoint(properties={"seed"})
    types["product"] = Endpoint(properties={"product"})

    src = workspace / "lib.xgdb"
    src.parent.mkdir(parents=True, exist_ok=True)
    lib = DataInstanceLibrary(src)
    lib.AddTypeLibrary(types, namespace="t")

    (src / "leaf.txt").parent.mkdir(parents=True, exist_ok=True)
    (src / "leaf.txt").write_text("leaf payload\n")
    lib.AddItem(Path("leaf.txt"), "t::seed")

    (src / "lineage.txt").write_text("lineage payload\n")
    lib.AddItem(Path("lineage.txt"), "t::product")
    lib.SetLineageInstance(
        Path("lineage.txt"),
        instance_id=lineage_id_hex,
        lineage_payload=b"fake-lineage-cbor-payload",
        origin="lineage",
    )
    lib.Save()
    return src


def test_import_library_preserves_identity(tmp_path):
    """G10: import-library round-trip preserves instance_id exactly.

    Build a library in ws_a with a known lineage-origin instance_id,
    import into ws_b, and assert ws_b's library carries the same
    instance_id verbatim.
    """
    from metasmith.models.libraries import DataInstanceLibrary
    from metasmith.models.remote import Source
    from metasmith.ops.data import import_library

    lineage_id = "1e20" + "ab" * 32  # multihash-shaped fake digest
    src_dir = _build_export_lib(tmp_path / "ws_a", lineage_id_hex=lineage_id)

    dst_dir = tmp_path / "ws_b" / "lib.xgdb"
    result = import_library(
        src_uri=str(src_dir),
        dest_path=str(dst_dir),
        as_image=False,
    )
    assert result["imported_cache_entries"] == 1
    assert result["skipped_leaf_entries"] == 1

    reloaded = DataInstanceLibrary.Load(dst_dir, check_integrity=False)
    assert reloaded.instance_meta[Path("lineage.txt")]["instance_id"] == lineage_id


def test_import_library_leaf_origin_no_cache_row(tmp_path):
    """G10: imported origin=leaf instances do NOT create cache rows.

    Leaf identities are unique-per-AddItem and not cache-meaningful;
    only origin in {"lineage","imported"} entries should populate the
    destination cache.
    """
    import sqlite3

    from metasmith.ops.data import import_library

    lineage_id = "1e20" + "cd" * 32
    src_dir = _build_export_lib(tmp_path / "ws_a", lineage_id_hex=lineage_id)
    dst_dir = tmp_path / "ws_b" / "lib.xgdb"
    cache_root = tmp_path / "ws_b" / "task_cache"

    import_library(
        src_uri=str(src_dir),
        dest_path=str(dst_dir),
        cache_root=str(cache_root),
        as_image=False,
    )

    conn = sqlite3.connect(cache_root / "cache.sqlite")
    try:
        rows = conn.execute(
            "SELECT origin FROM entries WHERE tombstoned_at IS NULL"
        ).fetchall()
    finally:
        conn.close()
    origins = [r[0] for r in rows]
    assert origins == ["imported"], (
        f"expected exactly one 'imported' row (leaf must not upsert), got {origins}"
    )
