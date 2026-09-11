from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

from ..caching.admission import IMPORTED
from ..caching.layout import default_cache_root
from ..caching.promote import tombstone_shard
from ..caching.store import CacheStore, decode_manifest
from ..logging import Log


def _inside(root: Path, path: Path) -> bool:
    """Whether collection may remove `path`.

    `output_root` is rebuilt by joining the stored value onto the cache root,
    and an absolute component wins that join -- so a row holding an absolute
    path resolves outside the store entirely, and `rmtree(..., ignore_errors)`
    would take it without a word. The store only ever deletes its own.
    """
    try:
        return Path(path).resolve().is_relative_to(Path(root).resolve())
    except OSError:
        return False


GROUPINGS = ("origin", "run", "dtype", "tag", "day")
SORTS = ("created_at", "last_hit_at", "size_bytes", "hit_count", "path")


def resolve_store_root(
    cache_root: str | None = None, agent_home: str | None = None,
) -> Path:
    """Which pool to act on. Taken as an argument, never assumed.

    The pool lives at an agent's home. Defaulting to the current directory is
    right for a shell sitting in a project and wrong for anything that acts on
    a chosen agent, so the caller says which.
    """
    if cache_root is not None:
        return Path(cache_root).resolve()
    if agent_home is not None:
        return default_cache_root(Path(agent_home).resolve())
    return default_cache_root(Path.cwd())


def _entry_rows(store, *, include_tombstoned: bool) -> list[dict]:
    """One row per file the store holds, not per entry.

    A step with two products is one entry and two instances, and the thing a
    user sorts, groups and tags against is the instance.
    """
    from ..caching.admission import manifest_files

    rows: list[dict] = []
    for e in store.iter_entries(include_tombstoned=include_tombstoned):
        try:
            manifest = decode_manifest(e.payload) if e.payload else {}
        except Exception:
            manifest = {}
        files = manifest_files(manifest)
        step_name = str(manifest.get("step_name", ""))
        # An import's key is minted, so two rows for one path are ordinary and
        # the name is the only thing that tells them apart.
        name = str(manifest.get("name", ""))
        for f in files or [None]:
            row = {
                "key": e.key.hex(),
                "name": name,
                "origin": e.origin,
                "run": e.run,
                "tags": list(e.tags),
                "transform_key": e.transform_key,
                "step_name": step_name,
                "size_bytes": e.size_bytes,
                "created_at": e.created_at,
                "last_hit_at": e.last_hit_at,
                "hit_count": e.hit_count,
                "tombstoned_at": e.tombstoned_at,
                "instance_id": "",
                "path": "",
                "dtype": "",
            }
            if f is not None:
                row.update({
                    "instance_id": f.InstanceId(),
                    "path": str(f.Resolve(e.output_root)),
                    "dtype": f.dtype_name,
                    "size_bytes": f.size or e.size_bytes,
                })
            rows.append(row)
    return rows


def list_cache(
    cache_root: str | None = None,
    *,
    agent_home: str | None = None,
    include_tombstoned: bool = False,
    origin: str | None = None,
    run: str | None = None,
    tag: str | None = None,
    dtype: str | None = None,
    group_by: str | None = None,
    sort_by: str = "created_at",
    descending: bool = True,
) -> dict:
    """What the store holds, filtered and grouped by the four things it records.

    Origin is the category, `run` is what produced it, `created_at` is when it
    arrived, and tags are what the user called it. Everything a caller could
    want to ask is answered here, so nothing downstream has to compute it.
    """
    root = resolve_store_root(cache_root, agent_home)
    empty = {"cache_root": str(root), "entries": []}
    if not (root / "cache.sqlite").exists():
        return empty
    if group_by is not None and group_by not in GROUPINGS:
        raise ValueError(f"group by one of {GROUPINGS}, got {group_by!r}")
    if sort_by not in SORTS:
        raise ValueError(f"sort by one of {SORTS}, got {sort_by!r}")

    store = CacheStore.open(root)
    try:
        rows = _entry_rows(store, include_tombstoned=include_tombstoned)
    finally:
        store.close()

    if origin is not None:
        rows = [r for r in rows if r["origin"] == origin]
    if run is not None:
        rows = [r for r in rows if r["run"] == run]
    if tag is not None:
        rows = [r for r in rows if tag in r["tags"]]
    if dtype is not None:
        rows = [r for r in rows if r["dtype"] == dtype]
    rows.sort(key=lambda r: r[sort_by], reverse=descending)

    out = {"cache_root": str(root), "entries": rows}
    if group_by is not None:
        out["group_by"] = group_by
        out["groups"] = _group(rows, group_by)
    return out


def _group(rows: list[dict], group_by: str) -> dict:
    groups: dict[str, list[dict]] = {}
    for r in rows:
        if group_by == "tag":
            # The one grouping where a row belongs to several groups, or to
            # none. An untagged row is not invisible: it is untagged.
            labels = r["tags"] or ["(untagged)"]
        elif group_by == "day":
            labels = [time.strftime("%Y-%m-%d", time.localtime(r["created_at"]))]
        elif group_by == "run":
            labels = [r["run"] or "(imported)"]
        else:
            labels = [r[group_by] or f"({group_by} unknown)"]
        for label in labels:
            groups.setdefault(label, []).append(r)
    return groups


def set_entry_tags(
    key_hex: str,
    tags: list[str],
    *,
    cache_root: str | None = None,
    agent_home: str | None = None,
    replace: bool = False,
    remove: bool = False,
) -> dict:
    """Add, remove or replace one entry's tags."""
    root = resolve_store_root(cache_root, agent_home)
    key = bytes.fromhex(key_hex)
    store = CacheStore.open(root)
    try:
        if store.probe(key) is None:
            return {"cache_root": str(root), "key": key_hex, "found": False}
        if remove:
            store.remove_tags(key, tags)
        elif replace:
            store.set_tags(key, tags)
        else:
            store.add_tags(key, tags)
        return {
            "cache_root": str(root),
            "key": key_hex,
            "found": True,
            "tags": list(store.tags_of(key)),
        }
    finally:
        store.close()


def gc_cache(
    cache_root: str | None = None,
    *,
    older_than_seconds: int | None = None,
    max_size_bytes: int | None = None,
    grace_seconds: int = 24 * 60 * 60,
    delete: bool = False,
    agent_home: str | None = None,
) -> dict:
    root = resolve_store_root(cache_root, agent_home)
    if not (root / "cache.sqlite").exists():
        return {"cache_root": str(root), "tombstoned": [], "deleted": []}
    store = CacheStore.open(root)
    try:
        now = int(time.time())
        tombstoned: list[str] = []
        deleted: list[str] = []

        kept_imports = 0

        def _collectable(e) -> bool:
            # Collection reclaims what it can get back. An import may be the
            # user's only copy of something, so it leaves on `data forget` and
            # nothing else.
            nonlocal kept_imports
            if e.origin == IMPORTED:
                kept_imports += 1
                return False
            return True

        if older_than_seconds is not None:
            cutoff = now - older_than_seconds
            for e in list(store.iter_entries()):
                if e.last_hit_at < cutoff and _collectable(e):
                    store.tombstone(e.key)
                    tombstone_shard(root, e.key.hex(), origin=e.origin)
                    tombstoned.append(e.key.hex())

        if max_size_bytes is not None:
            collectable = [e for e in store.iter_entries() if _collectable(e)]
            total = sum(e.size_bytes for e in store.iter_entries())
            if total > max_size_bytes:
                for e in sorted(collectable, key=lambda e: e.last_hit_at):
                    if total <= max_size_bytes:
                        break
                    store.tombstone(e.key)
                    tombstone_shard(root, e.key.hex(), origin=e.origin)
                    tombstoned.append(e.key.hex())
                    total -= e.size_bytes

        refused: list[str] = []
        if delete:
            for e in list(store.iter_entries(include_tombstoned=True)):
                if e.tombstoned_at is None:
                    continue
                if (now - e.tombstoned_at) < grace_seconds:
                    continue
                if e.output_root.exists():
                    if not _inside(root, e.output_root):
                        Log.Warn(
                            f"entry {e.key.hex()[:8]} points at "
                            f"[{e.output_root}], outside the store at [{root}]. "
                            "Left alone: collection removes only the store's own."
                        )
                        refused.append(e.key.hex())
                        continue
                    shutil.rmtree(e.output_root, ignore_errors=True)
                store.conn.execute("DELETE FROM entries WHERE key = ?", (e.key,))
                store.conn.commit()
                deleted.append(e.key.hex())

        return {
            "cache_root": str(root),
            "tombstoned": tombstoned,
            "deleted": deleted,
            "refused": refused,
            "kept_imports": kept_imports,
        }
    finally:
        store.close()


def explain_cache_entry(
    key_hex: str,
    cache_root: str | None = None,
    agent_home: str | None = None,
) -> dict:
    root = resolve_store_root(cache_root, agent_home)
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
            "run": entry.run,
            "tags": list(entry.tags),
            "manifest": manifest,
        }
    finally:
        store.close()


def status_run(run_dir: str) -> dict:
    return status_for_run(run_dir)


def status_for_run(run_dir: str) -> dict:
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
