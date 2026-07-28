"""Reading a library's trace: the query half of `DataInstanceLibrary`.

Mixed into `DataInstanceLibrary` rather than living on it, because these are
questions *about* a store rather than operations on one, and because they were
already fenced off in the monolith with their own banner.

Every method here reads a `TraceIndex` attached by `attach_trace` or by
`Load(attach_trace=True)`. With no trace attached they fall back to an empty
index, so every query stays well-defined -- `find_invocations()` returns `[]`,
`summary()` reports zero events. There is deliberately no "trace not attached"
error path.

Each of these used to import `..telemetry` at function scope. Nothing about
that was load-bearing: `telemetry` imports `models.lineage` and `logging` and
neither reaches back here, so the imports are top-level now and a cycle would
fail loudly at import rather than on the first query.
"""

from __future__ import annotations

from pathlib import Path

from ...telemetry import (
    TraceIndex, build_lineage_node, normalize_query_target, resolve_log_bundle,
)
from ..lineage import (
    InvocationNotFound, LeafRecord, LineageNode, LogBundle, TraceAlreadyAttached,
)


class _TelemetryQueries:
    """Mixed into `DataInstanceLibrary`; see the module docstring."""

    @property
    def _trace(self) -> "TraceIndex":
        idx = getattr(self, "_trace_index", None)
        if idx is None:
            idx = TraceIndex.empty()
            self._trace_index = idx
        return idx

    def attach_trace(self, trace_path: Path | str, *, across_sessions: bool = False) -> None:
        """Load a trace.jsonl file into the in-memory telemetry index.

        Idempotent on the same `(trace_path, across_sessions)`. Calling
        again with a different value raises `TraceAlreadyAttached`;
        use `refresh_trace()` to re-read the current file.

        Raises
        ------
        TraceCorruptError
            A trace.jsonl line is not valid JSON; the byte offset is
            reported.
        TraceAlreadyAttached
            A different `trace_path` is already attached.
        """

        trace_path = Path(trace_path)
        existing = getattr(self, "_trace_index", None)
        if existing is not None and existing.trace_path is not None:
            if (
                existing.trace_path == trace_path
                and existing.across_sessions == across_sessions
            ):
                return
            raise TraceAlreadyAttached(
                f"trace already attached at {existing.trace_path}; "
                f"call refresh_trace() to re-read or instantiate a fresh "
                f"library to switch sources"
            )
        self._trace_index = TraceIndex.read(
            trace_path, across_sessions=across_sessions
        )

    def refresh_trace(self) -> None:
        """Re-read the currently attached trace.jsonl. No-op if none attached."""
        idx = getattr(self, "_trace_index", None)
        if idx is None or idx.trace_path is None:
            return
        self._trace_index = TraceIndex.read(
            idx.trace_path, across_sessions=idx.across_sessions
        )

    def _resolve_target(self, thing) -> tuple[str, Path | None]:
        """Normalize a query target. Dispatch order:

          (a) `DataInstance` → its `.instance_id`
          (b) hex string `^[0-9a-f]{8,}$` → instance_id (no manifest lookup)
          (c) absolute `Path|str` → manifest entry
          (d) relative `Path|str` → resolved against `self.location`

        Raises `InstanceNotFound` when (c)/(d) miss.
        """
        return normalize_query_target(
            thing,
            manifest=self.manifest,
            instance_meta=self.instance_meta,
            location=self.location,
        )

    def get_lineage_of(self, thing) -> "LineageNode":
        """Return a frozen `LineageNode` for `thing`.

        Walks back through `event.consumes` building a `LineageNode`
        tree. Cycles are tolerated (revisited nodes appear as leaves);
        the walk caps at depth 16 as a safety net.

        Raises
        ------
        InstanceNotFound
            `thing` is a path or instance_id not known to the library.
        """
        instance_id, _ = self._resolve_target(thing)
        return build_lineage_node(
            instance_id, index=self._trace, library=self
        )

    def get_logs_of(self, thing) -> "LogBundle":
        """Resolve the `.command.*` logs for the event that produced `thing`.

        Returns a `LogBundle` with `status` ∈ {available, missing, pruned,
        legacy_shard_no_logs, remote_cache_no_logs, not_applicable}. Bare
        paths are never returned for "logs unavailable" cases.
        """
        try:
            instance_id, _ = self._resolve_target(thing)
        except KeyError:
            return LogBundle(status="missing", reason="instance not found")
        cache_root = getattr(self, "_cache_root", None)
        return resolve_log_bundle(
            instance_id, index=self._trace, cache_root=cache_root
        )

    def set_cache_root(self, cache_root: Path | str | None) -> None:
        """Register the cache_root used by `get_logs_of` for shard lookup.

        The library doesn't know the cache_root on its own — the workflow
        compile or `msm` CLI sets it post-Load. None means the library
        cannot resolve logs (returns `LogBundle(status="remote_cache_no_logs")`).
        """
        self._cache_root = Path(cache_root) if cache_root is not None else None

    def get_transform_of(self, thing):
        """Return the `InvocationEvent` or `LeafRecord` that produced `thing`."""
        instance_id, _ = self._resolve_target(thing)
        event = self._trace.find_event_for_instance(instance_id)
        if event is not None:
            return event
        return LeafRecord(source="user_added")

    def get_siblings_of(self, thing, scope: str = "slot") -> "list[LineageNode]":
        """Return other instances produced by the same `(slot|task)` frame.

        scope="slot" → other files emitted into the same `slot_id` by the
        same event (e.g., all bins from one binner run).
        scope="task" → all files produced by the same `task_hash` across
        all slots.
        Excludes `thing` itself.
        """
        assert scope in {"slot", "task"}, f"scope must be slot|task, got {scope!r}"
        instance_id, _ = self._resolve_target(thing)
        event = self._trace.find_event_for_instance(instance_id)
        if event is None:
            return []
        sibling_ids: list[str] = []
        if scope == "slot":
            target_slot = None
            for p in event.produces:
                if p.file_instance_id == instance_id:
                    target_slot = p.slot_id
                    break
            if target_slot is None:
                return []
            for p in event.produces:
                if p.slot_id == target_slot and p.file_instance_id != instance_id:
                    sibling_ids.append(p.file_instance_id)
        else:
            for p in event.produces:
                if p.file_instance_id != instance_id:
                    sibling_ids.append(p.file_instance_id)
        return [
            build_lineage_node(sid, index=self._trace, library=self)
            for sid in sibling_ids
        ]

    def walk_ancestors(self, thing, *, order: str = "bfs", strict: bool = False):
        """Yield `LineageNode`s for every ancestor of `thing`.

        order ∈ {"bfs","dfs"}. strict=True raises on cycle revisit;
        strict=False (default) silently de-dups, matching the lenient
        traversal lineage_robustness needs for group-fanin DAGs.

        Yields
        ------
        LineageNode
        """
        assert order in {"bfs", "dfs"}, f"order must be bfs|dfs, got {order!r}"
        instance_id, _ = self._resolve_target(thing)
        seen: set[str] = {instance_id}
        queue: list[str] = []
        event = self._trace.find_event_for_instance(instance_id)
        if event is None:
            return
        for parents in event.consumes.values():
            for pid in parents:
                if pid not in seen:
                    seen.add(pid)
                    queue.append(pid)
        while queue:
            if order == "bfs":
                cur = queue.pop(0)
            else:
                cur = queue.pop()
            yield build_lineage_node(cur, index=self._trace, library=self)
            ev = self._trace.find_event_for_instance(cur)
            if ev is None:
                continue
            for parents in ev.consumes.values():
                for pid in parents:
                    if pid in seen:
                        if strict:
                            raise RuntimeError(
                                f"cycle revisit at {pid} during walk_ancestors"
                            )
                        continue
                    seen.add(pid)
                    queue.append(pid)

    def find_by(self, *, dtype=None, transform_key=None, status=None, group_key=None):
        """Filter library instances. AND across kwargs, OR within iterables.

        Returns `LineageNode`s for matching instances (or plain dtype-only
        matches when no trace is attached). An empty filter returns
        every instance in the manifest.
        """

        def _as_set(v):
            if v is None:
                return None
            if isinstance(v, (list, tuple, set, frozenset)):
                return set(v)
            return {v}

        dtype_set = _as_set(dtype)
        tk_set = _as_set(transform_key)
        st_set = _as_set(status)
        gk_set = _as_set(group_key)

        results: list[LineageNode] = []
        for path, dtype_name in self.manifest.items():
            if dtype_set is not None and dtype_name not in dtype_set:
                continue
            meta = self.instance_meta.get(path)
            if meta is None:
                continue
            instance_id = meta["instance_id"]
            event = self._trace.find_event_for_instance(instance_id)
            if tk_set is not None:
                if event is None or event.transform_key not in tk_set:
                    continue
            if st_set is not None:
                if event is None or event.status not in st_set:
                    continue
            if gk_set is not None:
                if event is None or event.group is None or event.group.group_key not in gk_set:
                    continue
            results.append(
                build_lineage_node(instance_id, index=self._trace, library=self)
            )
        return results

    def list_dtypes(self) -> list[str]:
        """All distinct dtype names present in the manifest, sorted."""
        return sorted(set(self.manifest.values()))

    def list_transforms(self) -> list[str]:
        """All distinct transform_keys observed in the attached trace, sorted."""
        return self._trace.list_transforms()

    def summary(self) -> dict:
        """Telemetry summary of the library + attached trace.

        Shape:
          {
            "schema_version": 2,
            "counts": {"instances": int, "events": int, "sessions": int},
            "by_dtype": {dtype: count, ...},
            "by_transform": {transform_key: count, ...},
            "by_status": {status: count, ...},
            "time": {"first_session": int|None, "last_session": int|None},
          }
        """
        idx = self._trace
        by_dtype: dict[str, int] = {}
        for dtype in self.manifest.values():
            by_dtype[dtype] = by_dtype.get(dtype, 0) + 1
        by_transform: dict[str, int] = {}
        for e in idx.events:
            if e.transform_key:
                by_transform[e.transform_key] = by_transform.get(e.transform_key, 0) + 1
        session_ids = sorted({s.session_id for s in idx.sentinels})
        return {
            "schema_version": 2,
            "counts": {
                "instances": len(self.manifest),
                "events": len(idx.events),
                "sessions": len(session_ids),
            },
            "by_dtype": dict(sorted(by_dtype.items())),
            "by_transform": dict(sorted(by_transform.items())),
            "by_status": idx.list_statuses(),
            "time": {
                "first_session": session_ids[0] if session_ids else None,
                "last_session": session_ids[-1] if session_ids else None,
            },
        }

    def get_invocation(self, task_hash: str):
        """Return the `InvocationEvent` with the given `task_hash`.

        Raises `InvocationNotFound` when not present.
        """
        ev = self._trace.by_task.get(task_hash)
        if ev is None:
            raise InvocationNotFound(f"no invocation with task_hash={task_hash!r}")
        return ev

    def try_get_invocation(self, task_hash: str):
        """Return the `InvocationEvent` or None when missing (no-raise form)."""
        return self._trace.by_task.get(task_hash)

    def find_invocations(
        self,
        *,
        transform_key=None,
        status=None,
        group_key=None,
        started_after: str | None = None,
        started_before: str | None = None,
        exit_code: int | None = None,
    ) -> list:
        """Filter trace events. AND across kwargs, OR within iterables.

        Time filters compare on `started_at` ISO-8601 strings (lexicographic
        order on ISO-8601 matches chronological order). Events with no
        timestamp are excluded from time-bounded queries.
        """

        def _as_set(v):
            if v is None:
                return None
            if isinstance(v, (list, tuple, set, frozenset)):
                return set(v)
            return {v}

        tk = _as_set(transform_key)
        st = _as_set(status)
        gk = _as_set(group_key)

        out = []
        for e in self._trace.events:
            if tk is not None and e.transform_key not in tk:
                continue
            if st is not None and e.status not in st:
                continue
            if gk is not None:
                if e.group is None or e.group.group_key not in gk:
                    continue
            if started_after is not None:
                if not e.started_at or e.started_at < started_after:
                    continue
            if started_before is not None:
                if not e.started_at or e.started_at > started_before:
                    continue
            if exit_code is not None and e.exit_code != exit_code:
                continue
            out.append(e)
        return out

    def get_outputs_of(self, task_hash: str) -> list:
        """Return `LineageNode`s for every output file the named task produced."""
        ev = self._trace.by_task.get(task_hash)
        if ev is None:
            return []
        return [
            build_lineage_node(p.file_instance_id, index=self._trace, library=self)
            for p in ev.produces
        ]

    def find_failures(self) -> list:
        """Return every `InvocationEvent` representing a failed task.

        Predicate: `status == "fail"` OR (`status == "miss"` AND
        `exit_code not in (0, None)`).
        """
        out = []
        for e in self._trace.events:
            if e.status == "fail":
                out.append(e)
            elif e.status == "miss" and e.exit_code not in (0, None):
                out.append(e)
        return out
