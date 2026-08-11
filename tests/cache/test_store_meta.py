"""Sqlite metadata + session counter for the cache store (C2).

Focused unit tests covering the new schema_meta rows and the
`allocate_session_id()` atomic counter. End-to-end behavior is covered
by the existing cache integration suite.
"""

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

    # counter persists across re-open
    store2 = CacheStore.open(cache_root)
    try:
        d = store2.allocate_session_id()
    finally:
        store2.close()
    assert d == 4


def test_open_upgrades_stale_lineage_version(tmp_path, caplog):
    cache_root = tmp_path / "cache"
    # Pre-populate as if a pre-v2 cache existed.
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
    """A bump of CACHE_KEY_VERSION must alter the cache key bytes.

    The cache-key epoch — not the on-wire LIN_PAYLOAD_VERSION — is what is
    folded into the lineage payload, so it is the constant whose bump must
    invalidate old shards.
    """
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
