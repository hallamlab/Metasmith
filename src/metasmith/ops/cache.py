"""S8 — Cache and status ops backing `msm cache ...` and `msm status`."""
from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

from ..caching.layout import default_cache_root
from ..caching.store import CacheStore, decode_manifest


def _resolve_cache_root(cache_root: str | None) -> Path:
    if cache_root is not None:
        return Path(cache_root).resolve()
    # Default to <CWD>/task_cache for now; agent-home resolution is the
    # caller's job (CLI passes --cache-root explicitly in agent contexts).
    return default_cache_root(Path.cwd())


def list_cache(
    cache_root: str | None = None,
    *,
    include_tombstoned: bool = False,
) -> dict:
    root = _resolve_cache_root(cache_root)
    if not (root / "cache.sqlite").exists():
        return {"cache_root": str(root), "entries": []}
    store = CacheStore.open(root)
    try:
        rows = []
        for e in store.iter_entries(include_tombstoned=include_tombstoned):
            rows.append({
                "key": e.key.hex(),
                "transform_key": e.transform_key,
                "origin": e.origin,
                "size_bytes": e.size_bytes,
                "created_at": e.created_at,
                "last_hit_at": e.last_hit_at,
                "hit_count": e.hit_count,
                "tombstoned_at": e.tombstoned_at,
            })
    finally:
        store.close()
    return {"cache_root": str(root), "entries": rows}


def gc_cache(
    cache_root: str | None = None,
    *,
    older_than_seconds: int | None = None,
    max_size_bytes: int | None = None,
    grace_seconds: int = 24 * 60 * 60,
    delete: bool = False,
) -> dict:
    """Tombstone candidate entries; delete those past the grace period.

    Two-phase: tombstone marks entries for delayed removal so in-flight
    materializations can complete. A subsequent pass (or this same call
    when `delete=True`) physically unlinks entries whose `tombstoned_at`
    is older than `grace_seconds`.
    """
    root = _resolve_cache_root(cache_root)
    if not (root / "cache.sqlite").exists():
        return {"cache_root": str(root), "tombstoned": [], "deleted": []}
    store = CacheStore.open(root)
    try:
        now = int(time.time())
        tombstoned: list[str] = []
        deleted: list[str] = []

        if older_than_seconds is not None:
            cutoff = now - older_than_seconds
            for e in list(store.iter_entries()):
                if e.last_hit_at < cutoff:
                    store.tombstone(e.key)
                    tombstoned.append(e.key.hex())

        if max_size_bytes is not None:
            total = sum(e.size_bytes for e in store.iter_entries())
            if total > max_size_bytes:
                ordered = sorted(
                    store.iter_entries(),
                    key=lambda e: e.last_hit_at,
                )
                for e in ordered:
                    if total <= max_size_bytes:
                        break
                    store.tombstone(e.key)
                    tombstoned.append(e.key.hex())
                    total -= e.size_bytes

        if delete:
            for e in list(store.iter_entries(include_tombstoned=True)):
                if e.tombstoned_at is None:
                    continue
                if (now - e.tombstoned_at) < grace_seconds:
                    continue
                if e.output_root.exists():
                    shutil.rmtree(e.output_root, ignore_errors=True)
                store.conn.execute("DELETE FROM entries WHERE key = ?", (e.key,))
                store.conn.commit()
                deleted.append(e.key.hex())

        return {
            "cache_root": str(root),
            "tombstoned": tombstoned,
            "deleted": deleted,
        }
    finally:
        store.close()


def explain_cache_entry(
    key_hex: str,
    cache_root: str | None = None,
) -> dict:
    root = _resolve_cache_root(cache_root)
    store = CacheStore.open(root)
    try:
        key = bytes.fromhex(key_hex)
        entry = store.probe(key)
        if entry is None:
            return {"key": key_hex, "found": False}
        try:
            manifest = decode_manifest(entry.payload)
        except Exception as exc:  # noqa: BLE001 — surface error verbatim
            manifest = {"_decode_error": str(exc)}
        return {
            "key": key_hex,
            "found": True,
            "transform_key": entry.transform_key,
            "origin": entry.origin,
            "size_bytes": entry.size_bytes,
            "output_root": str(entry.output_root),
            "created_at": entry.created_at,
            "last_hit_at": entry.last_hit_at,
            "hit_count": entry.hit_count,
            "manifest": manifest,
        }
    finally:
        store.close()


def status_run(run_dir: str) -> dict:
    """Alias kept for the documented `msm status` entry point."""
    return status_for_run(run_dir)


def status_for_run(run_dir: str) -> dict:
    """Join _metasmith/trace.jsonl with workflow.step_N.meta files.

    Tolerates both v2 InvocationEvent rows (schema_version=2) and the
    SessionStart sentinel that leads every fresh trace. The returned
    `trace` key carries the InvocationEvent rows; `session_starts`
    surfaces SessionStart rows separately so callers can correlate
    `session_id` without re-parsing. Legacy v1 rows (no schema_version)
    pass through as-is for backwards compatibility with older run dirs.
    """
    run = Path(run_dir).resolve()
    trace = run / "_metasmith" / "trace.jsonl"
    rows: list[dict] = []
    session_starts: list[dict] = []
    if trace.exists():
        for line in trace.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                continue
            if raw.get("event") == "session_start":
                session_starts.append(raw)
                continue
            rows.append(raw)
    metas: dict[int, dict] = {}
    for meta in sorted(run.glob("workflow.step_*.meta")):
        try:
            order = int(meta.stem.rsplit("_", 1)[1])
        except (IndexError, ValueError):
            continue
        body: dict[str, str] = {}
        for ln in meta.read_text().splitlines():
            head, _, rest = ln.partition(" ")
            body[head] = rest.strip()
        metas[order] = body
    return {
        "run_dir": str(run),
        "trace": rows,
        "session_starts": session_starts,
        "meta": metas,
    }
