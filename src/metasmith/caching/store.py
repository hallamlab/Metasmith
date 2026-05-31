"""SQLite-backed cache store for lineage-addressed entries (S5).

One table, ``entries``, keyed by the multihash-prefixed cache key.
Schema mirrors the one named in the plan; columns are kept narrow so
that an entry row is everything `msm cache explain` needs without a
second lookup.

The probe/upsert surface is small on purpose. Callers do:
    store = CacheStore.open(cache_root)
    hit = store.probe(key)           # → CacheEntry | None
    store.upsert(key, payload, output_root, origin, size_bytes)
    store.touch(key)                 # update last_hit_at + hit_count

A 'hit' must also verify the on-disk output dir still exists — the
probe routine does this for callers via .files_exist().
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .keys import LIN_PAYLOAD_VERSION
from ..logging import Log


SCHEMA_VERSION = "1"

# Sqlite schema_meta keys — bumping LIN_PAYLOAD_VERSION here renders old
# shards unreachable (their cache_keys no longer collide). The session
# counter feeds trace.jsonl rotation: each compile reads + increments.
LIN_PAYLOAD_VERSION_KEY = "lineage_payload_version"
TRACE_SESSION_COUNTER_KEY = "trace_session_counter"
SHARD_LAYOUT_VERSION_KEY = "shard_layout_version"
SHARD_LAYOUT_VERSION = 2  # v2: <shard>/logs/.command.{sh,out,err,log} captured


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
    """A thin wrapper around the cache SQLite DB + on-disk cache root.

    Constructed via `CacheStore.open(cache_root)`; that ensures the
    directory exists, opens / initializes the DB at
    ``<cache_root>/cache.sqlite``, and stamps schema version metadata.
    """

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
        # Stamp the lineage-payload + shard-layout versions, and seed the
        # session counter on first open. Existing DBs keep their stored
        # value; the warn below fires when the stored value is older.
        conn.execute(
            "INSERT OR IGNORE INTO schema_meta(k, v) VALUES (?, ?)",
            (LIN_PAYLOAD_VERSION_KEY, str(LIN_PAYLOAD_VERSION)),
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
            (LIN_PAYLOAD_VERSION_KEY,),
        ).fetchone()
        stored = int(row[0]) if row is not None else 0
        if stored < LIN_PAYLOAD_VERSION:
            Log.Warn(
                f"lin payload v{LIN_PAYLOAD_VERSION} supersedes v{stored}; "
                f"old shards at {cache_root} are unreachable. "
                f"Run `msm cache gc --delete` to reclaim."
            )
            conn.execute(
                "UPDATE schema_meta SET v = ? WHERE k = ?",
                (str(LIN_PAYLOAD_VERSION), LIN_PAYLOAD_VERSION_KEY),
            )
        conn.commit()
        return cls(cache_root, conn)

    def allocate_session_id(self) -> int:
        """Atomic-increment + return the trace-session counter.

        The trace.jsonl rotator calls this on every compile to stamp a
        fresh `SessionStart` row and tag every `InvocationEvent` of the
        run. Monotonic; survives across runs (sqlite-persisted).
        """
        with self.conn:
            cur = self.conn.execute(
                "UPDATE schema_meta SET v = CAST(CAST(v AS INTEGER) + 1 AS TEXT) "
                "WHERE k = ?",
                (TRACE_SESSION_COUNTER_KEY,),
            )
            if cur.rowcount == 0:
                # First call on a DB that pre-dates the counter row.
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

    # ------------------------------------------------------------------
    # Probe / read

    def probe(self, key: bytes) -> CacheEntry | None:
        """Return the entry for `key` if it exists and is not tombstoned."""
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
        """Return True iff the entry's output_root dir is on disk."""
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

    # ------------------------------------------------------------------
    # Write

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
        """Insert or replace an entry.

        `output_root` is relative to self.cache_root (the on-disk dir
        name; e.g. the hex key plus a 1-char shard prefix).
        """
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


# ---------------------------------------------------------------------------
# Manifest format (CBOR sidecar in <cache_root>/<dir>/manifest.cbor)


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
    """Encode the per-entry manifest as canonical CBOR.

    `output_files` is a list of `{slot_key, branch, dtype_key, relpath}`
    dicts (relpath relative to the entry's output_root). `out_identities`
    maps dep_key → instance_id (hex). `index_payload` is the list of
    index Maps emitted into the Nextflow channel (one per per-sample
    invocation), preserved so synthetic channels reproduce them exactly.
    """
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
    """Decode a CBOR manifest previously written by encode_manifest."""
    import cbor2

    return cbor2.loads(blob)
