from __future__ import annotations

import time
from pathlib import Path

import pytest

from metasmith.caching.admission import IMPORTED, structural_import_id
from metasmith.caching.layout import CACHE_DIR_NAME
from metasmith.caching.projection import project_store
from metasmith.models.libraries import DataTypeLibrary
from metasmith.ops import data as ops

from tests.metasmith.cache._cache_harness import build_types_library


TYPE_NAMES = ("seed", "mid", "out")


@pytest.fixture
def store(tmp_path) -> Path:
    return tmp_path / CACHE_DIR_NAME


@pytest.fixture
def types(tmp_path) -> Path:
    return build_types_library(tmp_path / "types", TYPE_NAMES)


def _file(tmp_path: Path, name: str = "ref.fa") -> Path:
    p = tmp_path / name
    p.write_text(">x\nACGT\n")
    return p


class TestIdentity:
    def test_the_same_declaration_is_one_entry(self, tmp_path, store):
        f = _file(tmp_path)
        a = ops.import_item(str(f), "cf::seed", cache_root=str(store))
        b = ops.import_item(str(f), "cf::seed", cache_root=str(store))
        assert a["instance_id"] == b["instance_id"]
        assert b["status"] == "exists"

    def test_a_different_type_is_a_different_entry(self, tmp_path, store):
        f = _file(tmp_path)
        a = ops.import_item(str(f), "cf::seed", cache_root=str(store))
        b = ops.import_item(str(f), "cf::mid", cache_root=str(store))
        assert a["instance_id"] != b["instance_id"]

    def test_a_name_survives_a_move(self, tmp_path, store):
        (tmp_path / "a").mkdir()
        (tmp_path / "b").mkdir()
        first = _file(tmp_path / "a", "ref.fa")
        second = _file(tmp_path / "b", "ref.fa")
        a = ops.import_item(str(first), "cf::seed", cache_root=str(store), name="refs/x")
        b = ops.import_item(str(second), "cf::seed", cache_root=str(store), name="refs/x")
        assert a["instance_id"] == b["instance_id"]

    def test_nothing_is_read_or_stat_walked(self, tmp_path, store):
        # The id is a function of the declaration alone: a path that does not
        # exist mints the same id as one that does.
        here = structural_import_id("cf::seed", "/nowhere/at/all")
        assert here == structural_import_id("cf::seed", "/nowhere/at/all")
        res = ops.import_item("/nowhere/at/all", "cf::seed", cache_root=str(store))
        assert res["instance_id"] == here

    def test_a_folder_costs_what_a_file_costs(self, tmp_path, store):
        big = tmp_path / "refs"
        big.mkdir()
        for i in range(2000):
            (big / f"{i}.hmm").write_text("x")
        started = time.monotonic()
        res = ops.import_item(str(big), "cf::seed", cache_root=str(store))
        elapsed = time.monotonic() - started
        assert res["status"] == "promoted"
        # A tree walk over 2000 files is not what this should be doing. The
        # bound is loose on purpose: what it refuses is an implementation that
        # scales with the file count, not a slow disk.
        assert elapsed < 2.0, elapsed
        assert not list((store / "imported").rglob("*.hmm"))


class TestTypeResolution:
    def test_a_known_name_records_its_endpoint(self, tmp_path, store, types):
        f = _file(tmp_path)
        res = ops.import_item(
            str(f), "cf::seed", cache_root=str(store),
            type_library_paths=[f"cf={types}"],
        )
        assert res["type_resolved"]

    def test_an_unknown_name_is_refused_when_a_library_was_given(
        self, tmp_path, store, types,
    ):
        f = _file(tmp_path)
        with pytest.raises(ValueError) as e:
            ops.import_item(
                str(f), "cf::nosuch", cache_root=str(store),
                type_library_paths=[f"cf={types}"],
            )
        assert "nosuch" in str(e.value)

    def test_without_a_library_the_declaration_stands(self, tmp_path, store):
        f = _file(tmp_path)
        res = ops.import_item(str(f), "cf::anything", cache_root=str(store))
        assert res["status"] == "promoted"
        assert not res["type_resolved"]


class TestProjectionOfImports:
    def test_an_import_projects_as_an_instance_where_it_sits(
        self, tmp_path, store, types,
    ):
        f = _file(tmp_path)
        res = ops.import_item(str(f), "cf::seed", cache_root=str(store))
        proj = project_store(store, types={"cf": DataTypeLibrary.Load(types)})
        assert list(proj.library.manifest) == [f]
        assert proj.items[f].origin == IMPORTED
        assert proj.library.Get(f).instance_id == res["instance_id"]

    def test_a_parent_becomes_an_ancestry_edge(self, tmp_path, store, types):
        parent = _file(tmp_path, "genome.fa")
        child = _file(tmp_path, "calls.vcf")
        ops.import_item(str(parent), "cf::seed", cache_root=str(store))
        ops.import_item(
            str(child), "cf::mid", cache_root=str(store), parents=[str(parent)],
        )
        proj = project_store(store, types={"cf": DataTypeLibrary.Load(types)})
        assert [pm.name for pm in proj.library.parents.get(child, [])] == ["cf::seed"]

    def test_a_parent_that_is_not_in_the_pool_is_refused(self, tmp_path, store):
        child = _file(tmp_path, "calls.vcf")
        with pytest.raises(ValueError) as e:
            ops.import_item(
                str(child), "cf::mid", cache_root=str(store),
                parents=[str(tmp_path / "absent.fa")],
            )
        assert "points at nothing" in str(e.value)


class TestForget:
    def test_forgetting_leaves_the_data_alone(self, tmp_path, store, types):
        f = _file(tmp_path)
        res = ops.import_item(str(f), "cf::seed", cache_root=str(store))
        out = ops.forget_item(
            res["instance_id"], cache_root=str(store), delete=True,
        )
        assert out["tombstoned"] and out["shard_removed"]
        assert f.exists() and f.read_text().startswith(">x")
        proj = project_store(store, types={"cf": DataTypeLibrary.Load(types)})
        assert proj.library.manifest == {}

    def test_a_product_is_not_forgettable(self, tmp_path, store):
        from metasmith.caching.admission import PRODUCT, PoolFile, admit

        key = bytes.fromhex("ab" * 32)
        src = _file(tmp_path)
        admit(
            cache_root=store, key=key, origin=PRODUCT,
            files=[PoolFile(
                dtype_name="cf::out", relpath="out/x.txt", slot_id="s" * 64,
                size=1, src=str(src),
            )],
        )
        with pytest.raises(ValueError) as e:
            ops.forget_item(key.hex(), cache_root=str(store))
        assert "re-derived" in str(e.value)


class TestStoreRoot:
    def test_the_pool_is_at_an_agent_home(self, tmp_path, monkeypatch):
        monkeypatch.delenv("AGENT_HOME", raising=False)
        home = tmp_path / "msm_home"
        root = ops.resolve_store_root(str(home))
        assert root == home / CACHE_DIR_NAME

    def test_with_no_agent_it_says_so(self, monkeypatch):
        monkeypatch.delenv("AGENT_HOME", raising=False)
        with pytest.raises(ValueError) as e:
            ops.resolve_store_root()
        assert "AGENT_HOME" in str(e.value)
