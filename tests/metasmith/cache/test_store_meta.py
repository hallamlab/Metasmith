from __future__ import annotations

import sqlite3

import pytest

from metasmith.caching.keys import CACHE_KEY_VERSION
from metasmith.caching.store import (
    CACHE_EPOCH_KEY,
    SHARD_LAYOUT_VERSION,
    SHARD_LAYOUT_VERSION_KEY,
    TRACE_SESSION_COUNTER_KEY,
    CacheStore,
)


def _read_meta(cache_root, key):
    conn = sqlite3.connect(cache_root / "cache.sqlite")
    try:
        row = conn.execute(
            "SELECT v FROM schema_meta WHERE k = ?", (key,)
        ).fetchone()
    finally:
        conn.close()
    return row[0] if row else None


def test_open_stamps_lin_and_shard_versions(tmp_path):
    cache_root = tmp_path / "cache"
    store = CacheStore.open(cache_root)
    try:
        assert _read_meta(cache_root, CACHE_EPOCH_KEY) == str(
            CACHE_KEY_VERSION
        )
        assert _read_meta(cache_root, SHARD_LAYOUT_VERSION_KEY) == str(
            SHARD_LAYOUT_VERSION
        )
        assert _read_meta(cache_root, TRACE_SESSION_COUNTER_KEY) == "0"
    finally:
        store.close()


def test_allocate_session_id_monotonic_across_opens(tmp_path):
    cache_root = tmp_path / "cache"
    store = CacheStore.open(cache_root)
    try:
        a = store.allocate_session_id()
        b = store.allocate_session_id()
        c = store.allocate_session_id()
    finally:
        store.close()
    assert (a, b, c) == (1, 2, 3)

    store2 = CacheStore.open(cache_root)
    try:
        d = store2.allocate_session_id()
    finally:
        store2.close()
    assert d == 4


def test_open_upgrades_stale_lineage_version(tmp_path, caplog):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    conn = sqlite3.connect(cache_root / "cache.sqlite")
    conn.execute(
        "CREATE TABLE schema_meta(k TEXT PRIMARY KEY, v TEXT)"
    )
    conn.execute(
        "INSERT INTO schema_meta(k, v) VALUES (?, ?)",
        (CACHE_EPOCH_KEY, "1"),
    )
    conn.commit()
    conn.close()

    store = CacheStore.open(cache_root)
    try:
        assert _read_meta(cache_root, CACHE_EPOCH_KEY) == str(
            CACHE_KEY_VERSION
        )
    finally:
        store.close()


def test_lineage_key_version_baked_in(tmp_path):
    from metasmith.caching import keys as keys_mod
    from metasmith.caching.keys import lineage_key

    base = lineage_key("tr.x", "sig", [("a", b"id")])
    original = keys_mod.CACHE_KEY_VERSION
    try:
        keys_mod.CACHE_KEY_VERSION = original + 1
        bumped = lineage_key("tr.x", "sig", [("a", b"id")])
    finally:
        keys_mod.CACHE_KEY_VERSION = original
    assert base != bumped


def test_an_epoch_bump_tombstones_what_it_strands(tmp_path):
    # The warning names `msm cache gc --delete`, and gc only unlinks rows that
    # are already tombstoned. Without this the command reclaims nothing, and
    # every user who takes an epoch bump keeps paying for shards no key reaches.
    from metasmith.ops.cache import gc_cache

    cache_root = tmp_path / "cache"
    store = CacheStore.open(cache_root)
    try:
        shard = cache_root / "shard"
        shard.mkdir(parents=True)
        (shard / "out.txt").write_text("stale\n", encoding="utf-8")
        store.upsert(
            key=b"\x01" * 34,
            transform_key="tr.x",
            payload=b"{}",
            output_root=str(shard),
            size_bytes=6,
            origin="lineage",
        )
        conn = store.conn
        conn.execute(
            "UPDATE schema_meta SET v = ? WHERE k = ?",
            (str(CACHE_KEY_VERSION - 1), CACHE_EPOCH_KEY),
        )
        conn.commit()
    finally:
        store.close()

    store = CacheStore.open(cache_root)
    try:
        live = list(store.iter_entries())
        stranded = list(store.iter_entries(include_tombstoned=True))
        assert live == [], "a pre-epoch shard is still offered as reachable"
        assert len(stranded) == 1
        assert stranded[0].tombstoned_at is not None
    finally:
        store.close()

    res = gc_cache(str(cache_root), delete=True)
    assert res["deleted"] == [(b"\x01" * 34).hex()], (
        "the command the epoch warning names did not reclaim the shards it"
        " stranded"
    )
    assert not shard.exists()


def test_an_epoch_bump_does_not_touch_post_epoch_shards(tmp_path):
    # The indiscriminate version of the fix takes the shards this run is about
    # to write with it.
    from metasmith.ops.cache import gc_cache

    cache_root = tmp_path / "cache"
    store = CacheStore.open(cache_root)
    try:
        shard = cache_root / "shard"
        shard.mkdir(parents=True)
        store.upsert(
            key=b"\x02" * 34,
            transform_key="tr.x",
            payload=b"{}",
            output_root=str(shard),
            size_bytes=1,
            origin="lineage",
        )
    finally:
        store.close()

    store = CacheStore.open(cache_root)
    try:
        assert len(list(store.iter_entries())) == 1
    finally:
        store.close()

    res = gc_cache(str(cache_root), delete=True)
    assert res["deleted"] == []
    assert shard.exists()
