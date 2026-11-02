"""What the ops layer can answer about a store, so nothing downstream computes it."""

from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.caching.admission import PRODUCT, PoolFile, admit
from metasmith.caching.layout import CACHE_DIR_NAME
from metasmith.ops import cache as ops
from metasmith.ops import data as data_ops


@pytest.fixture
def populated(tmp_path) -> Path:
    store = tmp_path / CACHE_DIR_NAME
    for i, (dtype, tags) in enumerate([
        ("cf::seed", ["refs"]), ("cf::mid", ["refs", "v2"]), ("cf::seed", []),
    ]):
        f = tmp_path / f"in_{i}.txt"
        f.write_text("x" * (i + 1))
        data_ops.import_item(str(f), dtype, cache_root=str(store), tags=tags)

    src = tmp_path / "prod.txt"
    src.write_text("produced")
    from metasmith.caching.store import CacheStore
    with CacheStore.open(store) as s:
        written = admit(
            cache_root=store, key=bytes.fromhex("cd" * 32), origin=PRODUCT,
            files=[PoolFile(
                dtype_name="cf::out", relpath="out/p.txt", slot_id="s" * 64,
                size=8, src=str(src),
            )],
            store=s, run="run_ABC",
        )
        assert written.status == "promoted"
    return store


class TestFiltering:
    def test_origin_separates_products_from_imports(self, populated):
        imports = ops.list_cache(str(populated), origin="imported")["entries"]
        products = ops.list_cache(str(populated), origin="lineage")["entries"]
        assert len(imports) == 3
        assert [r["dtype"] for r in products] == ["cf::out"]

    def test_a_run_names_only_what_it_produced(self, populated):
        rows = ops.list_cache(str(populated), run="run_ABC")["entries"]
        assert [r["dtype"] for r in rows] == ["cf::out"]

    def test_a_tag_narrows(self, populated):
        assert len(ops.list_cache(str(populated), tag="refs")["entries"]) == 2
        assert len(ops.list_cache(str(populated), tag="v2")["entries"]) == 1

    def test_a_type_narrows(self, populated):
        rows = ops.list_cache(str(populated), dtype="cf::seed")["entries"]
        assert len(rows) == 2

    def test_a_row_names_the_file_not_just_the_entry(self, populated):
        for r in ops.list_cache(str(populated))["entries"]:
            assert r["path"] and r["instance_id"] and r["dtype"]


class TestGrouping:
    def test_by_origin(self, populated):
        groups = ops.list_cache(str(populated), group_by="origin")["groups"]
        assert set(groups) == {"imported", "lineage"}

    def test_by_run_calls_imports_their_own_category(self, populated):
        groups = ops.list_cache(str(populated), group_by="run")["groups"]
        assert set(groups) == {"run_ABC", "(imported)"}
        assert len(groups["(imported)"]) == 3

    def test_by_tag_keeps_the_untagged_visible(self, populated):
        groups = ops.list_cache(str(populated), group_by="tag")["groups"]
        assert set(groups) == {"refs", "v2", "(untagged)"}
        # One row carries two tags and appears under both.
        assert len(groups["refs"]) == 2

    def test_an_unknown_grouping_is_refused(self, populated):
        with pytest.raises(ValueError):
            ops.list_cache(str(populated), group_by="colour")


class TestSorting:
    def test_by_size_descending_by_default(self, populated):
        rows = ops.list_cache(str(populated), sort_by="size_bytes")["entries"]
        sizes = [r["size_bytes"] for r in rows]
        assert sizes == sorted(sizes, reverse=True)

    def test_ascending_when_asked(self, populated):
        rows = ops.list_cache(
            str(populated), sort_by="size_bytes", descending=False,
        )["entries"]
        sizes = [r["size_bytes"] for r in rows]
        assert sizes == sorted(sizes)


class TestTagging:
    def test_tags_are_added_removed_and_replaced(self, populated):
        key = ops.list_cache(str(populated), origin="lineage")["entries"][0]["key"]
        assert ops.set_entry_tags(key, ["a", "b"], cache_root=str(populated))["tags"] == ["a", "b"]
        assert ops.set_entry_tags(key, ["a"], cache_root=str(populated), remove=True)["tags"] == ["b"]
        assert ops.set_entry_tags(key, ["z"], cache_root=str(populated), replace=True)["tags"] == ["z"]

    def test_an_absent_entry_says_so(self, populated):
        res = ops.set_entry_tags("ab" * 32, ["x"], cache_root=str(populated))
        assert res["found"] is False


class TestStoreRoot:
    def test_an_agent_home_names_its_own_store(self, tmp_path):
        assert ops.resolve_store_root(agent_home=str(tmp_path)) == tmp_path / CACHE_DIR_NAME

    def test_an_explicit_root_wins(self, tmp_path):
        assert ops.resolve_store_root(str(tmp_path / "x"), str(tmp_path)) == tmp_path / "x"
