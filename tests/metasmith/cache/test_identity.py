"""Cache identity axis.

Covers cache key derivation (lineage_key: transform_key + sorted input
instance_ids → canonical CBOR → blake3 multihash), encoding stability
across Python versions and dict orderings, and the cross-workspace
identity preservation contract that `metasmith data import-library`
relies on.

Coverage: G1 (lineage key shape + multihash prefix), R1 (leaf AddItem
content-addressed when the file is present → cross-run stable; unique
random fallback when absent), G9 (lineage-only identity is
workspace-independent), G10 (cross-workspace import preserves identity),
S1 (canonical CBOR stability).
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


def test_addItem_content_addressed_when_file_present(tmp_path):
    """R1: a present leaf file is content-addressed → stable across builds.

    This is the cross-run reentrancy contract. Two independent library
    builds — same location, same (path, dtype), same file *bytes* —
    produce DataInstances with IDENTICAL `instance_id`s, because AddItem
    mints `multihash(blake3(file_bytes))` when the file is readable. That
    byte-stability is exactly what lets a second run's cache_keys match
    the first run's and resume from the cache with no import-library step.

    (Supersedes the pre-R1 `test_addItem_unique_per_call`, which pinned
    the old always-random G8 model; uniqueness now holds only for the
    absent-file fallback — see the next test.)
    """
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
    # multihash-shaped
    assert bytes.fromhex(id1)[:2] == b"\x1e\x20"

    # Different bytes → different id (same path, so content participates).
    id3 = _build_once("DIFFERENT\n")
    assert id3 != id1


def test_addItem_same_bytes_distinct_paths_are_distinct(tmp_path):
    """R1: identical bytes at DIFFERENT relative paths mint DISTINCT ids.

    The leaf id folds the library-relative path into the content digest,
    so two distinct inputs that happen to share bytes (e.g. N empty files,
    or two samples with byte-identical reads) keep distinct identities.
    Pure content-addressing would collapse them to one leaf — corrupting
    fan-out and re-triggering the solver's O(n^2) id-collision path. This
    is the guard for that regression.
    """
    from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
    from metasmith.models.solver import Endpoint

    types = DataTypeLibrary()
    types["seed"] = Endpoint(properties={"seed"})
    tpath = tmp_path / "types.yml"
    types.Save(tpath)

    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    lib.Purge()
    lib.AddTypeLibrary(tpath, namespace="cf")
    # two files, identical (empty) bytes, different relative paths
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
    """R1: the absent-file fallback preserves the legacy unique-per-call id.

    When no readable file exists at the path at AddItem time (remote /
    lazily materialized inputs), the leaf id falls back to the old
    `multihash(blake3, uuid4 || time_ns)` random shape — so two builds
    still differ, and those leaves simply get no cross-run cache reuse.
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
        # note: no file written at a.txt → absent-file random fallback
        lib.AddItem(Path("a.txt"), "cf::seed")
        return lib.Get(Path("a.txt")).instance_id

    id1 = _build_once()
    id2 = _build_once()
    assert id1 != id2, (
        "two absent-file AddItem calls produced the same leaf id; the "
        "fallback should stay unique-per-call"
    )


def test_addItem_directory_leaf_is_content_addressed(tmp_path):
    """R1: a DIRECTORY leaf is tree-addressed, not random.

    The case is a vendored python package or a profile database staged as
    one item. Before the directory arm existed these fell to the random
    fallback, so a library recompile over an unchanged tree minted a new id
    and discarded every cached run that had read it -- days of compute for
    a tree nobody touched. Three claims, and the third is the one that
    makes it safe: an unchanged tree is stable, a changed file moves it,
    and a RENAME with no content change moves it too (the digest folds each
    entry's relative path, so a tree cannot be reshuffled invisibly).
    """
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
    """The two shape decisions in `tree_multihash_key`, pinned.

    An empty directory contributes nothing: git cannot represent one, so
    counting it would make a tree's identity depend on whether it survived
    a checkout. A symlink contributes its TARGET STRING rather than the
    bytes it points at -- following it would make the digest depend on
    something outside the tree, and Logistics copies symlinks as symlinks,
    so the target is what actually gets staged.
    """
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
    (base / "link").symlink_to("sub/other.txt")   # dangling, deliberately
    assert tree_multihash_key(base) != with_link, (
        "retargeting a symlink did not move the digest; the target string is "
        "what is staged, so it has to participate"
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
