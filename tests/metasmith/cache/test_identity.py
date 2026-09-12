from __future__ import annotations

from pathlib import Path

import pytest


def test_canonical_cbor_key_stable_across_dict_orderings():
    from metasmith.caching.keys import canonical_cbor

    payload_a = {"a": 1, "b": [2, 3]}
    payload_b = {"b": [2, 3], "a": 1}
    assert canonical_cbor(payload_a) == canonical_cbor(payload_b)


def test_key_carries_multihash_prefix():
    from metasmith.caching.keys import lineage_key

    k = lineage_key("tr", "sig", [])
    assert k[:2] == b"\x1e\x20", (
        f"expected blake3-32 multihash prefix 0x1e 0x20, got {k[:2]!r}"
    )
    assert len(k) == 2 + 32


def test_addItem_stat_addressed_when_file_present(tmp_path):
    from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
    from metasmith.models.solver import Endpoint

    types = DataTypeLibrary()
    types["seed"] = Endpoint(properties={"seed"})
    tpath = tmp_path / "types.yml"
    types.Save(tpath)

    (tmp_path / "samples.xgdb").mkdir(parents=True, exist_ok=True)
    (tmp_path / "samples.xgdb" / "a.txt").write_text("payload\n", encoding="utf-8")

    def _register_once() -> str:
        lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
        lib.AddTypeLibrary(tpath, namespace="cf")
        lib.AddItem(Path("a.txt"), "cf::seed")
        return lib.Get(Path("a.txt")).instance_id

    id1 = _register_once()
    id2 = _register_once()
    assert id1 == id2, (
        "registering an untouched file twice produced different leaf ids; a "
        "leaf id must be stable across runs for cross-run reentrancy"
    )
    assert bytes.fromhex(id1)[:2] == b"\x1e\x20"

    (tmp_path / "samples.xgdb" / "a.txt").write_text("DIFFERENT\n", encoding="utf-8")
    assert _register_once() != id1, "an edited file kept its leaf id"


def test_addItem_same_bytes_distinct_paths_are_distinct(tmp_path):
    from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
    from metasmith.models.solver import Endpoint

    types = DataTypeLibrary()
    types["seed"] = Endpoint(properties={"seed"})
    tpath = tmp_path / "types.yml"
    types.Save(tpath)

    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    lib.Purge()
    lib.AddTypeLibrary(tpath, namespace="cf")
    (lib.location / "a.txt").write_text("", encoding="utf-8")
    (lib.location / "b.txt").write_text("", encoding="utf-8")
    lib.AddItem(Path("a.txt"), "cf::seed")
    lib.AddItem(Path("b.txt"), "cf::seed")

    id_a = lib.Get(Path("a.txt")).instance_id
    id_b = lib.Get(Path("b.txt")).instance_id
    assert id_a != id_b, (
        "identical-byte files at different paths collapsed to one leaf id; "
        "the relative path must participate in leaf identity"
    )


def test_addItem_unique_per_call_when_file_absent(tmp_path):
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
        lib.AddItem(Path("a.txt"), "cf::seed")
        return lib.Get(Path("a.txt")).instance_id

    id1 = _build_once()
    id2 = _build_once()
    assert id1 != id2, (
        "two absent-file AddItem calls produced the same leaf id; the "
        "fallback should stay unique-per-call"
    )


def test_addItem_directory_leaf_costs_one_stat(tmp_path):
    # A directory leaf is stat'd, not walked: a 300k-file reference folder costs
    # what a small file costs. The price is reach -- a change nested inside one
    # does not move the directory's own mtime, and so is invisible here. The
    # smoke alarm for that is `models/libraries/pinned.py`, not this id.
    from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
    from metasmith.models.solver import Endpoint

    types = DataTypeLibrary()
    types["seed"] = Endpoint(properties={"seed"})
    tpath = tmp_path / "types.yml"
    types.Save(tpath)

    pkg = tmp_path / "samples.xgdb" / "pkg"
    (pkg / "sub").mkdir(parents=True)
    (pkg / "mod.py").write_text("X = 1\n", encoding="utf-8")
    (pkg / "sub" / "leaf.py").write_text("Y = 2\n", encoding="utf-8")

    def _register_once() -> str:
        lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
        lib.AddTypeLibrary(tpath, namespace="cf")
        lib.AddItem(Path("pkg"), "cf::seed")
        return lib.Get(Path("pkg")).instance_id

    id1 = _register_once()
    assert _register_once() == id1, (
        "re-registering an unchanged directory minted a different id; every "
        "recompile would throw away the cache"
    )
    assert bytes.fromhex(id1)[:2] == b"\x1e\x20"

    (pkg / "added.py").write_text("Z = 3\n", encoding="utf-8")
    assert _register_once() != id1, "a new immediate entry did not move the id"


def test_tree_key_ignores_empty_directories_and_reads_symlink_targets(tmp_path):
    from metasmith.caching.keys import tree_multihash_key

    base = tmp_path / "t"
    (base / "sub").mkdir(parents=True)
    (base / "sub" / "f.txt").write_text("hello", encoding="utf-8")
    before = tree_multihash_key(base)

    (base / "empty").mkdir()
    assert tree_multihash_key(base) == before, "an empty directory moved the digest"

    (base / "link").symlink_to("sub/f.txt")
    with_link = tree_multihash_key(base)
    assert with_link != before, "a new symlink did not move the digest"

    (base / "link").unlink()
    (base / "link").symlink_to("sub/other.txt")
    assert tree_multihash_key(base) != with_link, (
        "retargeting a symlink did not move the digest; the target string is "
        "what is staged, so it has to participate"
    )


def test_lineage_id_static_no_inputs(tmp_path):
    from metasmith.caching.keys import lineage_key  # noqa: F401  S1+S2

    k1 = lineage_key("tr.download_gtdb", "sig_v1", [])
    k2 = lineage_key("tr.download_gtdb", "sig_v1", [])
    assert k1 == k2


def test_lineage_id_static_with_inputs(tmp_path):
    from metasmith.caching.keys import lineage_key

    inputs = [("dep_a", b"id_aaa"), ("dep_b", b"id_bbb")]
    k1 = lineage_key("tr.align", "sig_v3", inputs)
    k2 = lineage_key("tr.align", "sig_v3", inputs)
    assert k1 == k2


def test_container_change_invalidates(tmp_path):
    from metasmith.caching.keys import lineage_key

    base = lineage_key("tr.align", "sig_v3", [("container", b"img_v1")])
    bumped = lineage_key("tr.align", "sig_v3", [("container", b"img_v2")])
    assert base != bumped


def _build_export_lib(workspace: Path, *, lineage_id_hex: str) -> Path:
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
    from metasmith.models.libraries import DataInstanceLibrary
    from metasmith.models.remote import Source
    from metasmith.ops.data import import_library

    lineage_id = "1e20" + "ab" * 32
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
