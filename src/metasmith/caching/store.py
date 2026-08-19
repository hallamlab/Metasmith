from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .keys import CACHE_KEY_VERSION
from ..logging import Log


SCHEMA_VERSION = "1"

# Sqlite schema_meta keys — bumping CACHE_KEY_VERSION renders old shards
# unreachable (their cache_keys no longer collide). The session counter feeds
# trace.jsonl rotation: each compile reads + increments. The key string is
# kept as "lineage_payload_version" for backward-compat with DBs stamped
# before the cache-epoch / wire-version split (R5); the stored VALUE now
# tracks CACHE_KEY_VERSION, the cache epoch.
CACHE_EPOCH_KEY = "lineage_payload_version"
TRACE_SESSION_COUNTER_KEY = "trace_session_counter"
SHARD_LAYOUT_VERSION_KEY = "shard_layout_version"
SHARD_LAYOUT_VERSION = 2


_CREATE_SQL = [
    """
    CREATE TABLE IF NOT EXISTS entries(
        key            BLOB PRIMARY KEY,
        transform_key  TEXT NOT NULL,
        payload        BLOB NOT NULL,
        output_root    TEXT NOT NULL,
        size_bytes     INTEGER NOT NULL,
        created_at     INTEGER NOT NULL,
        last_hit_at    INTEGER NOT NULL,
        hit_count      INTEGER NOT NULL DEFAULT 0,
        origin         TEXT NOT NULL,
        tombstoned_at  INTEGER
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_entries_tomb
        ON entries(tombstoned_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_entries_lru
        ON entries(last_hit_at) WHERE tombstoned_at IS NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS schema_meta(
        k TEXT PRIMARY KEY,
        v TEXT
    )
    """,
]


@dataclass(frozen=True)
class CacheEntry:
    key: bytes
    transform_key: str
    payload: bytes
    output_root: Path
    size_bytes: int
    origin: str
    created_at: int
    last_hit_at: int
    hit_count: int
    tombstoned_at: int | None


class CacheStore:
    def __init__(self, cache_root: Path, conn: sqlite3.Connection) -> None:
        self.cache_root = cache_root
        self.conn = conn

    @classmethod
    def open(cls, cache_root: Path) -> "CacheStore":
        cache_root.mkdir(parents=True, exist_ok=True)
        db = cache_root / "cache.sqlite"
        conn = sqlite3.connect(db)
        conn.execute("PRAGMA journal_mode = WAL")
        for stmt in _CREATE_SQL:
            conn.execute(stmt)
        conn.execute(
            "INSERT OR IGNORE INTO schema_meta(k, v) VALUES (?, ?)",
            ("schema_version", SCHEMA_VERSION),
        )
        conn.execute(
            "INSERT OR IGNORE INTO schema_meta(k, v) VALUES (?, ?)",
            (CACHE_EPOCH_KEY, str(CACHE_KEY_VERSION)),
        )
        conn.execute(
            "INSERT OR IGNORE INTO schema_meta(k, v) VALUES (?, ?)",
            (SHARD_LAYOUT_VERSION_KEY, str(SHARD_LAYOUT_VERSION)),
        )
        conn.execute(
            "INSERT OR IGNORE INTO schema_meta(k, v) VALUES (?, ?)",
            (TRACE_SESSION_COUNTER_KEY, "0"),
        )
        row = conn.execute(
            "SELECT v FROM schema_meta WHERE k = ?",
            (CACHE_EPOCH_KEY,),
        ).fetchone()
        stored = int(row[0]) if row is not None else 0
        if stored < CACHE_KEY_VERSION:
            Log.Warn(
                f"cache epoch v{CACHE_KEY_VERSION} supersedes v{stored}; "
                f"old shards at {cache_root} are unreachable. "
                f"Run `msm cache gc --delete` to reclaim."
            )
            conn.execute(
                "UPDATE schema_meta SET v = ? WHERE k = ?",
                (str(CACHE_KEY_VERSION), CACHE_EPOCH_KEY),
            )
        conn.commit()
        return cls(cache_root, conn)

    def allocate_session_id(self) -> int:
        with self.conn:
            cur = self.conn.execute(
                "UPDATE schema_meta SET v = CAST(CAST(v AS INTEGER) + 1 AS TEXT) "
                "WHERE k = ?",
                (TRACE_SESSION_COUNTER_KEY,),
            )
            if cur.rowcount == 0:
                self.conn.execute(
                    "INSERT INTO schema_meta(k, v) VALUES (?, ?)",
                    (TRACE_SESSION_COUNTER_KEY, "1"),
                )
            row = self.conn.execute(
                "SELECT v FROM schema_meta WHERE k = ?",
                (TRACE_SESSION_COUNTER_KEY,),
            ).fetchone()
        return int(row[0])

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "CacheStore":
        return self

    def __exit__(self, *args) -> None:
        self.close()


    def probe(self, key: bytes) -> CacheEntry | None:
        row = self.conn.execute(
            """
            SELECT key, transform_key, payload, output_root, size_bytes,
                   origin, created_at, last_hit_at, hit_count, tombstoned_at
            FROM entries WHERE key = ?
            """,
            (key,),
        ).fetchone()
        if row is None:
            return None
        if row[9] is not None:
            return None
        return CacheEntry(
            key=row[0],
            transform_key=row[1],
            payload=row[2],
            output_root=self.cache_root / row[3],
            size_bytes=row[4],
            origin=row[5],
            created_at=row[6],
            last_hit_at=row[7],
            hit_count=row[8],
            tombstoned_at=row[9],
        )

    def files_exist(self, entry: CacheEntry) -> bool:
        return entry.output_root.is_dir()

    def touch(self, key: bytes) -> None:
        now = int(time.time())
        self.conn.execute(
            """
            UPDATE entries
               SET last_hit_at = ?, hit_count = hit_count + 1
             WHERE key = ?
            """,
            (now, key),
        )
        self.conn.commit()


    def upsert(
        self,
        *,
        key: bytes,
        transform_key: str,
        payload: bytes,
        output_root: str,
        size_bytes: int,
        origin: str,
    ) -> None:
        assert origin in {"lineage", "imported"}, (
            f"origin must be lineage or imported, got {origin!r}"
        )
        now = int(time.time())
        self.conn.execute(
            """
            INSERT INTO entries(
                key, transform_key, payload, output_root, size_bytes,
                created_at, last_hit_at, hit_count, origin, tombstoned_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, NULL)
            ON CONFLICT(key) DO UPDATE SET
                transform_key = excluded.transform_key,
                payload       = excluded.payload,
                output_root   = excluded.output_root,
                size_bytes    = excluded.size_bytes,
                last_hit_at   = excluded.last_hit_at,
                origin        = excluded.origin,
                tombstoned_at = NULL
            """,
            (
                key, transform_key, payload, output_root, size_bytes,
                now, now, origin,
            ),
        )
        self.conn.commit()

    def tombstone(self, key: bytes) -> None:
        now = int(time.time())
        self.conn.execute(
            "UPDATE entries SET tombstoned_at = ? WHERE key = ?",
            (now, key),
        )
        self.conn.commit()

    def iter_entries(self, *, include_tombstoned: bool = False) -> Iterable[CacheEntry]:
        sql = """
            SELECT key, transform_key, payload, output_root, size_bytes,
                   origin, created_at, last_hit_at, hit_count, tombstoned_at
            FROM entries
        """
        if not include_tombstoned:
            sql += " WHERE tombstoned_at IS NULL"
        sql += " ORDER BY last_hit_at DESC"
        for row in self.conn.execute(sql):
            yield CacheEntry(
                key=row[0],
                transform_key=row[1],
                payload=row[2],
                output_root=self.cache_root / row[3],
                size_bytes=row[4],
                origin=row[5],
                created_at=row[6],
                last_hit_at=row[7],
                hit_count=row[8],
                tombstoned_at=row[9],
            )


def encode_manifest(
    *,
    cache_key: bytes,
    transform_key: str,
    signature: str,
    lineage_payload: bytes,
    output_files: list[dict],
    out_identities: dict[str, str],
    index_payload: list[dict],
) -> bytes:
    from .keys import canonical_cbor

    return canonical_cbor(
        {
            "v": 1,
            "key": cache_key,
            "tk": transform_key,
            "sig": signature,
            "lineage": lineage_payload,
            "files": output_files,
            "ids": out_identities,
            "index": index_payload,
        }
    )


def decode_manifest(blob: bytes) -> dict:
    import cbor2

    return cbor2.loads(blob)
