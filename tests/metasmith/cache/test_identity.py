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


def test_addItem_content_addressed_when_file_present(tmp_path):
    from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
    from metasmith.models.solver import Endpoint

    types = DataTypeLibrary()
    types["seed"] = Endpoint(properties={"seed"})
    tpath = tmp_path / "types.yml"
    types.Save(tpath)

    def _build_once(payload: str) -> str:
        lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
        lib.Purge()
        lib.AddTypeLibrary(tpath, namespace="cf")
        (lib.location / "a.txt").write_text(payload, encoding="utf-8")
        lib.AddItem(Path("a.txt"), "cf::seed")
        return lib.Get(Path("a.txt")).instance_id

    id1 = _build_once("payload\n")
    id2 = _build_once("payload\n")
    assert id1 == id2, (
        "same input bytes produced different leaf ids; content-addressed "
        "leaves must be byte-stable across runs for cross-run reentrancy"
    )
    assert bytes.fromhex(id1)[:2] == b"\x1e\x20"

    id3 = _build_once("DIFFERENT\n")
    assert id3 != id1


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


def test_addItem_directory_leaf_is_content_addressed(tmp_path):
    from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
    from metasmith.models.solver import Endpoint

    types = DataTypeLibrary()
    types["seed"] = Endpoint(properties={"seed"})
    tpath = tmp_path / "types.yml"
    types.Save(tpath)

    def _build_once(files: dict[str, str]) -> str:
        lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
        lib.Purge()
        lib.AddTypeLibrary(tpath, namespace="cf")
        pkg = lib.location / "pkg"
        for rel, payload in files.items():
            p = pkg / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(payload, encoding="utf-8")
        lib.AddItem(Path("pkg"), "cf::seed")
        return lib.Get(Path("pkg")).instance_id

    tree = {"__init__.py": "", "mod.py": "X = 1\n", "sub/leaf.py": "Y = 2\n"}
    id1 = _build_once(tree)
    id2 = _build_once(tree)
    assert id1 == id2, (
        "re-staging an unchanged directory minted a different id; a directory "
        "leaf must be tree-addressed or every recompile throws away the cache"
    )
    assert bytes.fromhex(id1)[:2] == b"\x1e\x20"

    assert _build_once({**tree, "sub/leaf.py": "Y = 3\n"}) != id1, "content change not seen"
    renamed = {"__init__.py": "", "mod.py": "X = 1\n", "sub/other.py": "Y = 2\n"}
    assert _build_once(renamed) != id1, "rename with identical bytes not seen"


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
