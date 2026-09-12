from __future__ import annotations

from pathlib import Path

import pytest

from metasmith.caching.admission import IMPORTED, mint_import_id
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
    def test_importing_one_path_twice_is_two_entries(self, tmp_path, store):
        from metasmith.caching.store import CacheStore

        f = _file(tmp_path)
        a = ops.import_item(str(f), "cf::seed", cache_root=str(store))
        b = ops.import_item(str(f), "cf::seed", cache_root=str(store))
        assert a["instance_id"] != b["instance_id"]
        assert a["status"] == b["status"] == "promoted"
        with CacheStore.open(store) as s:
            assert s.probe(bytes.fromhex(a["instance_id"])) is not None
            assert s.probe(bytes.fromhex(b["instance_id"])) is not None

    def test_a_different_type_is_a_different_entry(self, tmp_path, store):
        f = _file(tmp_path)
        a = ops.import_item(str(f), "cf::seed", cache_root=str(store))
        b = ops.import_item(str(f), "cf::mid", cache_root=str(store))
        assert a["instance_id"] != b["instance_id"]

    def test_two_files_under_one_name_stay_two_things(self, tmp_path, store):
        # The case the derived id collapsed: same type, same name, physically
        # separate files.
        (tmp_path / "a").mkdir()
        (tmp_path / "b").mkdir()
        first = _file(tmp_path / "a", "ref.fa")
        second = _file(tmp_path / "b", "ref.fa")
        a = ops.import_item(str(first), "cf::seed", cache_root=str(store), name="refs/x")
        b = ops.import_item(str(second), "cf::seed", cache_root=str(store), name="refs/x")
        assert a["instance_id"] != b["instance_id"]

    def test_the_name_is_recorded_because_the_key_no_longer_carries_it(
        self, tmp_path, store,
    ):
        from metasmith.caching.store import CacheStore, decode_manifest

        f = _file(tmp_path)
        res = ops.import_item(
            str(f), "cf::seed", cache_root=str(store), name="refs/x",
        )
        with CacheStore.open(store) as s:
            entry = s.probe(bytes.fromhex(res["instance_id"]))
        assert decode_manifest(entry.payload)["name"] == "refs/x"

    def test_a_mint_is_a_mint_and_not_a_derivation(self, tmp_path, store):
        # Nothing is read and nothing is stat-walked, so a path that does not
        # exist imports -- and it imports to a different id every time.
        assert mint_import_id("cf::seed", "x") != mint_import_id("cf::seed", "x")
        a = ops.import_item("/nowhere/at/all", "cf::seed", cache_root=str(store))
        b = ops.import_item("/nowhere/at/all", "cf::seed", cache_root=str(store))
        assert a["instance_id"] != b["instance_id"]

    def test_an_identity_outlives_the_filesystem_it_names(self, tmp_path, store):
        # Touching the file, moving the pool on disk and bumping the cache epoch
        # all leave an imported id where it was. That is the whole point of the
        # entry being a record rather than a calculation.
        import os
        import shutil

        from metasmith.caching import store as store_mod

        f = _file(tmp_path)
        res = ops.import_item(str(f), "cf::seed", cache_root=str(store))
        iid = res["instance_id"]

        os.utime(f, (1, 1))
        moved = tmp_path / "elsewhere" / "pool"
        moved.parent.mkdir()
        shutil.move(str(store), str(moved))

        # `store` binds the epoch at import time, so patching `keys` here would
        # leave the sweep reading the old number and prove nothing.
        epoch = store_mod.CACHE_KEY_VERSION
        try:
            store_mod.CACHE_KEY_VERSION = epoch + 1
            assert ops.store_ids_by_path(moved)[str(f)] == iid
        finally:
            store_mod.CACHE_KEY_VERSION = epoch
        assert ops.store_ids_by_path(moved)[str(f)] == iid

    def test_the_newest_import_of_a_path_is_the_one_that_projects(
        self, tmp_path, store, types,
    ):
        # A library manifest is keyed by path, so one of the two has to win. The
        # later declaration is the current one; the earlier stays in the store,
        # because shards keyed on it are still valid.
        f = _file(tmp_path)
        old = ops.import_item(str(f), "cf::seed", cache_root=str(store))
        new = ops.import_item(str(f), "cf::seed", cache_root=str(store))
        proj = project_store(store, types={"cf": DataTypeLibrary.Load(types)})
        assert proj.library.Get(f).instance_id == new["instance_id"]
        assert old["instance_id"] in proj.skipped["path already claimed"]

    def test_a_folder_costs_what_a_file_costs(self, tmp_path, store, monkeypatch):
        # Not a stopwatch: a wall-clock bound measures the machine. What must
        # hold is that the import does not scale with the file count, so count
        # the calls. One stat on the folder itself, and nothing enumerated.
        import os

        big = tmp_path / "refs"
        big.mkdir()
        for i in range(2000):
            (big / f"{i}.hmm").write_text("x")

        seen: list[str] = []
        real_stat = os.stat

        def counting_stat(path, *a, **kw):
            try:
                s = os.fspath(path)
            except TypeError:
                s = ""
            if isinstance(s, bytes):
                s = s.decode("utf-8", "replace")
            if s.startswith(str(big)):
                seen.append(s)
            return real_stat(path, *a, **kw)

        def refuse(*a, **kw):
            raise AssertionError("the import enumerated the folder")

        monkeypatch.setattr(os, "stat", counting_stat)
        monkeypatch.setattr(os, "scandir", refuse)
        monkeypatch.setattr(os, "listdir", refuse)

        res = ops.import_item(str(big), "cf::seed", cache_root=str(store))
        assert res["status"] == "promoted"
        # A couple of stats on the folder itself -- resolving it and asking its
        # size -- and not one on anything inside it.
        assert set(seen) == {str(big)}, sorted(set(seen))
        monkeypatch.undo()
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


class TestGrouping:
    def test_an_import_carries_its_tags_and_no_run(self, tmp_path, store):
        from metasmith.caching.store import CacheStore

        f = _file(tmp_path)
        res = ops.import_item(
            str(f), "cf::seed", cache_root=str(store), tags=["refs", "v2"],
        )
        with CacheStore.open(store) as s:
            entry = s.probe(bytes.fromhex(res["instance_id"]))
        assert entry.tags == ("refs", "v2")
        assert entry.run == "", "nothing produced an import, so it names no run"
        assert entry.created_at > 0

    def test_tags_can_be_set_and_cleared(self, tmp_path, store):
        from metasmith.caching.store import CacheStore

        f = _file(tmp_path)
        res = ops.import_item(str(f), "cf::seed", cache_root=str(store), tags=["a"])
        key = bytes.fromhex(res["instance_id"])
        with CacheStore.open(store) as s:
            s.add_tags(key, ["b"])
            assert s.probe(key).tags == ("a", "b")
            s.remove_tags(key, ["a"])
            assert s.probe(key).tags == ("b",)
            s.set_tags(key, ["c", "d"])
            assert s.probe(key).tags == ("c", "d")

    def test_an_older_database_gains_the_run_column(self, tmp_path):
        # The table statements are all "if not exists", so an existing database
        # gains no column that way. The migration is what does it.
        import sqlite3

        root = tmp_path / "old_cache"
        root.mkdir()
        conn = sqlite3.connect(root / "cache.sqlite")
        conn.execute("""
            CREATE TABLE entries(
                key BLOB PRIMARY KEY, transform_key TEXT NOT NULL,
                payload BLOB NOT NULL, output_root TEXT NOT NULL,
                size_bytes INTEGER NOT NULL, created_at INTEGER NOT NULL,
                last_hit_at INTEGER NOT NULL, hit_count INTEGER NOT NULL DEFAULT 0,
                origin TEXT NOT NULL, tombstoned_at INTEGER)
        """)
        conn.execute("CREATE TABLE schema_meta(k TEXT PRIMARY KEY, v TEXT)")
        conn.execute("INSERT INTO schema_meta VALUES ('schema_version', '1')")
        from metasmith.caching.keys import CACHE_KEY_VERSION
        conn.execute(
            "INSERT INTO schema_meta VALUES ('lineage_payload_version', ?)",
            (str(CACHE_KEY_VERSION),),
        )
        conn.execute(
            "INSERT INTO entries VALUES (?, 'tk', X'', 'x', 1, 1, 1, 0, 'lineage', NULL)",
            (b"\x01" * 8,),
        )
        conn.commit()
        conn.close()

        from metasmith.caching.store import CacheStore

        with CacheStore.open(root) as s:
            entry = s.probe(b"\x01" * 8)
            assert entry is not None, "the migration tombstoned an entry"
            assert entry.run == ""
            assert entry.tags == ()


class TestPoolRetention:
    """The pool is state, so where it lives is a correctness question.

    An assigned identity cannot be rebuilt. A pool on storage the site sweeps
    has a delete scheduled against the meaning of every shard keyed on it, and
    the failure arrives months after the mistake with nothing to diagnose.
    """

    def test_a_pool_under_a_swept_path_says_so_at_the_first_import(
        self, tmp_path, caplog,
    ):
        home = tmp_path / "scratch" / "someone" / "campaign"
        home.mkdir(parents=True)
        f = _file(tmp_path)
        ops.import_item(str(f), "cf::seed", agent_home=str(home))
        assert "unreadable" in caplog.text
        assert "scratch" in caplog.text

    def test_a_pool_on_ordinary_storage_says_nothing(self):
        # Asked of the classifier rather than of an import, because pytest's
        # own tmp_path lives under /tmp -- which is a swept path, so an import
        # there correctly warns and could never show the quiet case.
        assert ops.pool_retention_warning(
            Path("/project/def-someone/campaign/task_cache")
        ) is None

    def test_a_swept_path_is_named_wherever_it_sits(self):
        msg = ops.pool_retention_warning(Path("/scratch/someone/campaign"))
        assert msg is not None and "scratch" in msg

    def test_it_is_said_once_and_not_on_every_import(self, tmp_path, caplog):
        home = tmp_path / "scratch" / "campaign"
        home.mkdir(parents=True)
        f = _file(tmp_path)
        ops.import_item(str(f), "cf::seed", agent_home=str(home))
        caplog.clear()
        ops.import_item(str(f), "cf::mid", agent_home=str(home))
        assert "unreadable" not in caplog.text
