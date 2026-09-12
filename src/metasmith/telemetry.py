from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable, Optional, Union

from .caching.layout import logs_dir as _logs_dir, shard_dir
from .models.lineage import (
    GroupingFrame,
    InstanceNotFound,
    InvocationEvent,
    InvocationNotFound,
    LeafRecord,
    LineageNode,
    LogBundle,
    ProducedFile,
    SessionStart,
    TraceAlreadyAttached,
    TraceCorruptError,
)
from .logging import Log


_HEX_RE = re.compile(r"^[0-9a-f]{8,}$")


class TraceIndex:
    def __init__(self) -> None:
        self.events: list[InvocationEvent] = []
        self.sentinels: list[SessionStart] = []
        self.by_file: dict[str, list[InvocationEvent]] = {}
        self.by_slot: dict[str, list[InvocationEvent]] = {}
        self.by_task: dict[str, InvocationEvent] = {}
        self.trace_path: Optional[Path] = None
        self.across_sessions: bool = False

    @classmethod
    def empty(cls) -> "TraceIndex":
        return cls()

    @classmethod
    def read(
        cls, trace_path: Path, *, across_sessions: bool = False
    ) -> "TraceIndex":
        idx = cls()
        idx.trace_path = trace_path
        idx.across_sessions = across_sessions
        paths: list[Path] = []
        if trace_path.exists():
            paths.append(trace_path)
        if across_sessions and trace_path.parent.exists():
            paths.extend(sorted(trace_path.parent.glob("trace.*.jsonl")))
        for path in paths:
            idx._read_one(path)
        return idx

    def _read_one(self, path: Path) -> None:
        offset = 0
        try:
            data = path.read_bytes()
        except OSError:
            return
        for line_bytes in data.splitlines(keepends=True):
            line = line_bytes.decode("utf-8", errors="replace").strip()
            if not line:
                offset += len(line_bytes)
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as e:
                raise TraceCorruptError(offset, str(e))
            if raw.get("event") == SessionStart.EVENT_NAME:
                self.sentinels.append(SessionStart.from_dict(raw))
            else:
                event = InvocationEvent.from_dict(raw)
                if event is not None:
                    self._index_event(event)
            offset += len(line_bytes)

    def _index_event(self, e: InvocationEvent) -> None:
        self.events.append(e)
        # Last write wins for task_hash — the latest status reflects the
        # current state (e.g. miss row overwritten by promoted row).
        self.by_task[e.task_hash] = e
        for p in e.produces:
            self.by_file.setdefault(p.file_instance_id, []).append(e)
            if p.slot_id and p.slot_id != p.file_instance_id:
                self.by_slot.setdefault(p.slot_id, []).append(e)
            else:
                self.by_slot.setdefault(p.file_instance_id, []).append(e)


    def find_event_for_instance(self, instance_id: str) -> Optional[InvocationEvent]:
        evs = self.by_file.get(instance_id) or self.by_slot.get(instance_id)
        if not evs:
            return None
        return evs[-1]

    def list_transforms(self) -> list[str]:
        return sorted({e.transform_key for e in self.events if e.transform_key})

    def list_statuses(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for e in self.events:
            counts[e.status] = counts.get(e.status, 0) + 1
        return counts


def normalize_query_target(
    thing: Any,
    *,
    manifest: dict[Path, str],
    instance_meta: dict[Path, dict],
    location: Path,
) -> tuple[str, Optional[Path]]:
    if hasattr(thing, "instance_id") and hasattr(thing, "dtype"):
        return thing.instance_id, getattr(thing, "path", None)

    if isinstance(thing, str) and _HEX_RE.match(thing):
        return thing, None

    if isinstance(thing, (str, Path)):
        path = Path(thing)
        if path in manifest:
            meta = instance_meta.get(path)
            if meta is not None:
                return meta["instance_id"], path
        if not path.is_absolute():
            abs_path = (location / path).resolve()
        else:
            abs_path = path.resolve()
        for key in manifest:
            try:
                key_abs = key if key.is_absolute() else (location / key).resolve()
            except OSError:
                continue
            if key_abs == abs_path:
                meta = instance_meta.get(key)
                if meta is not None:
                    return meta["instance_id"], key
        raise InstanceNotFound(f"no manifest entry for {thing!r}")

    raise InstanceNotFound(f"unsupported query target: {type(thing).__name__}")


def build_lineage_node(
    instance_id: str,
    *,
    index: TraceIndex,
    library: "Any",
    visited: Optional[set[str]] = None,
    depth_remaining: int = 16,
) -> LineageNode:
    if visited is None:
        visited = set()

    dtype_key, path_str = _instance_dtype_and_path(library, instance_id)
    event = index.find_event_for_instance(instance_id)

    if event is None or instance_id in visited or depth_remaining <= 0:
        provenance: Union[InvocationEvent, LeafRecord]
        if event is not None:
            provenance = event
        else:
            provenance = LeafRecord(source="user_added")
        return LineageNode(
            instance_id=instance_id,
            dtype_key=dtype_key,
            path=path_str,
            produced_by=provenance,
            inputs={},
            group=event.group if event else None,
        )

    visited = visited | {instance_id}
    inputs: dict[str, list[LineageNode]] = {}
    for dep_key, parent_ids in event.consumes.items():
        inputs[dep_key] = [
            build_lineage_node(
                pid,
                index=index,
                library=library,
                visited=visited,
                depth_remaining=depth_remaining - 1,
            )
            for pid in parent_ids
        ]
    return LineageNode(
        instance_id=instance_id,
        dtype_key=dtype_key,
        path=path_str,
        produced_by=event,
        inputs=inputs,
        group=event.group,
    )


def _instance_dtype_and_path(library, instance_id: str) -> tuple[str, str]:
    for path, dtype_name in library.manifest.items():
        meta = library.instance_meta.get(path)
        if meta and meta.get("instance_id") == instance_id:
            return dtype_name, str(path)
    return "", ""


def resolve_log_bundle(
    instance_id: str,
    *,
    index: TraceIndex,
    cache_root: Optional[Path],
) -> LogBundle:
    event = index.find_event_for_instance(instance_id)
    if event is None:
        return LogBundle(status="not_applicable", reason="no producing event")
    if cache_root is None:
        return LogBundle(
            status="remote_cache_no_logs",
            reason="no cache_root configured",
        )
    key_hex = event.cache_key or event.task_hash
    if not key_hex:
        return LogBundle(status="missing", reason="event has no cache_key")
    shard = shard_dir(cache_root, key_hex)
    if not shard.exists():
        return LogBundle(status="pruned", reason=f"shard not found at {shard}")
    logs_dir = _logs_dir(shard)
    if not logs_dir.exists():
        return LogBundle(
            status="legacy_shard_no_logs",
            reason=f"shard {key_hex[:8]}… pre-dates C8 log capture",
        )
    out = logs_dir / ".command.out"
    err = logs_dir / ".command.err"
    sh = logs_dir / ".command.sh"
    log = logs_dir / ".command.log"
    if all(p.exists() for p in (out, err, sh, log)):
        return LogBundle(
            status="available",
            stdout=out,
            stderr=err,
            command_sh=sh,
            command_log=log,
        )
    return LogBundle(
        status="missing",
        stdout=out if out.exists() else None,
        stderr=err if err.exists() else None,
        command_sh=sh if sh.exists() else None,
        command_log=log if log.exists() else None,
        reason="one or more .command.* files absent",
    )
