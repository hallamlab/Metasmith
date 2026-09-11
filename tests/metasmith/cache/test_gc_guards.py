"""Three ways the store could take something it cannot give back.

All three predate the pool being able to hold imports, and all three are the
kind that report success while doing the damage.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from metasmith.caching.admission import IMPORTED, PRODUCT, PoolFile, admit
from metasmith.caching.invocation import TOMBSTONE_NAME
from metasmith.caching.keys import CACHE_KEY_VERSION
from metasmith.caching.layout import imported_shard_dir, shard_dir
from metasmith.caching.store import CACHE_EPOCH_KEY, CacheStore
from metasmith.ops import cache as ops
from metasmith.ops import data as data_ops


def _a_product(store: Path, tmp_path: Path, key_hex: str) -> None:
    src = tmp_path / f"{key_hex[:4]}.txt"
    src.write_text("payload")
    admit(
        cache_root=store, key=bytes.fromhex(key_hex), origin=PRODUCT,
        files=[PoolFile(
            dtype_name="cf::out", relpath="out/x.txt", slot_id="s" * 64,
            size=7, src=str(src),
        )],
    )


class TestCollectionStaysInsideTheStore:
    def test_a_row_pointing_outside_is_refused(self, tmp_path):
        # `output_root` is rebuilt by joining onto the cache root, and an
        # absolute component wins that join. A row like this used to resolve
        # outside the store and be rmtree'd with errors suppressed.
        outside = tmp_path / "precious"
        outside.mkdir()
        (outside / "data.fa").write_text("do not delete me")

        store = tmp_path / "task_cache"
        with CacheStore.open(store) as s:
            s.upsert(
                key=b"\xab" * 8, transform_key="tk", payload=b"",
                output_root=str(outside), size_bytes=1, origin="lineage",
            )
            s.tombstone(b"\xab" * 8)
            s.conn.execute(
                "UPDATE entries SET tombstoned_at = 0 WHERE key = ?", (b"\xab" * 8,),
            )
            s.conn.commit()

        res = ops.gc_cache(str(store), delete=True, grace_seconds=0)
        assert res["refused"] == ["ab" * 8]
        assert (outside / "data.fa").read_text() == "do not delete me"

    def test_the_store_still_reclaims_its_own(self, tmp_path):
        store = tmp_path / "task_cache"
        key = "cd" * 32
        _a_product(store, tmp_path, key)
        with CacheStore.open(store) as s:
            s.tombstone(bytes.fromhex(key))
        res = ops.gc_cache(str(store), delete=True, grace_seconds=0)
        assert res["deleted"] == [key]
        assert not shard_dir(store, key).exists()


class TestCollectionLeavesImportsAlone:
    def test_an_lru_pass_does_not_tombstone_an_import(self, tmp_path):
        store = tmp_path / "task_cache"
        f = tmp_path / "ref.fa"
        f.write_text(">x\n")
        imported = data_ops.import_item(str(f), "cf::seed", cache_root=str(store))
        _a_product(store, tmp_path, "cd" * 32)

        res = ops.gc_cache(str(store), max_size_bytes=0)
        assert res["kept_imports"] == 1
        assert imported["instance_id"] not in res["tombstoned"]
        assert "cd" * 32 in res["tombstoned"]

    def test_an_age_pass_does_not_either(self, tmp_path):
        store = tmp_path / "task_cache"
        f = tmp_path / "ref.fa"
        f.write_text(">x\n")
        key = data_ops.import_item(str(f), "cf::seed", cache_root=str(store))
        # Age the row rather than passing `older_than_seconds=0`: that cutoff is
        # `now`, and whether a row written this second falls under it depends on
        # which side of a second boundary the two calls landed.
        with CacheStore.open(store) as s:
            s.conn.execute(
                "UPDATE entries SET last_hit_at = 0 WHERE key = ?",
                (bytes.fromhex(key["instance_id"]),),
            )
            s.conn.commit()
        res = ops.gc_cache(str(store), older_than_seconds=60)
        assert res["tombstoned"] == []
        assert res["kept_imports"] == 1


class TestTheTombstoneReachesTheRightNamespace:
    def test_an_imported_shard_is_marked_on_disk(self, tmp_path):
        from metasmith.caching.promote import tombstone_shard

        store = tmp_path / "task_cache"
        f = tmp_path / "ref.fa"
        f.write_text(">x\n")
        res = data_ops.import_item(str(f), "cf::seed", cache_root=str(store))
        key = res["instance_id"]

        tombstone_shard(store, key, origin=IMPORTED)
        assert (imported_shard_dir(store, key) / TOMBSTONE_NAME).exists()
        # And the product namespace was not conjured into existence for it.
        assert not shard_dir(store, key).exists()


class TestTheEpochSweepScopesItself:
    def test_a_bump_strands_products_and_spares_imports(self, tmp_path):
        store = tmp_path / "task_cache"
        f = tmp_path / "ref.fa"
        f.write_text(">x\n")
        imported = data_ops.import_item(str(f), "cf::seed", cache_root=str(store))
        _a_product(store, tmp_path, "cd" * 32)

        # Wind the recorded epoch back, which is what a bump looks like from
        # the next process's side.
        with CacheStore.open(store) as s:
            s.conn.execute(
                "UPDATE schema_meta SET v = ? WHERE k = ?",
                (str(CACHE_KEY_VERSION - 1), CACHE_EPOCH_KEY),
            )
            s.conn.commit()

        with CacheStore.open(store) as s:
            assert s.probe(bytes.fromhex("cd" * 32)) is None, (
                "a product keyed under the old epoch is unreachable and should "
                "be tombstoned"
            )
            surviving = s.probe(bytes.fromhex(imported["instance_id"]))
            assert surviving is not None, (
                "an import is keyed outside the epoch, so a bump invalidates "
                "nothing about it -- and it may be the only copy"
            )
            assert surviving.origin == IMPORTED
