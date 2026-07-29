"""Lineage schema — canonical event log + telemetry value types.

This module owns the single shape every trace.jsonl row takes
(`InvocationEvent`) and the value types the telemetry API hands to
users (`LineageNode`, `LogBundle`). Reading it tells you exactly what
the on-disk event log promises and what the public API returns.

Wire vs. post-facto identity
----------------------------
- `slot_id` — minted at Generate, identifies a production *channel*
  (per `(transform, slot, branch_idx)`). Travels on the Nextflow
  channel via `LinPayload`. Routes inter-task data flow.
- `file_instance_id` — minted post-facto in `CollectResults`, identifies
  one emitted *file* (deterministic over `slot_id || relative_path`).
  Does NOT travel on the wire. Surfaced via the user-facing library.

`LinPayload` (slot-level) is the wire format. `InvocationEvent.produces`
carries both ids per file (event-stream view of the same fact).

Per-output ancestry
-------------------
`InvocationEvent.consumes` records the invocation's inputs as a single
`dep_key -> [instance_id]` map — shared by *every* output of that
invocation, so it cannot say which output descends from which input.
`ProducedFile.parents` refines this to the per-file truth: the input
`instance_id`s *that* file descends from. This is the single point of
provenance the telemetry walk and (next session) the cache-walk read.
When `parents` is empty the fact wasn't captured (legacy row, or an
emission route that hasn't recorded runtime ancestry) and consumers
fall back to the event-level `consumes`.

trace.jsonl layout
------------------
A trace.jsonl is a sequence of newline-delimited JSON objects. The
first line of every fresh file is a `SessionStart` sentinel; every
subsequent line is an `InvocationEvent`. On compile, the old file is
rotated to `trace.<session_id>.jsonl` (session_id from the cache sqlite
counter), never truncated. The telemetry API unions across rotated
siblings on demand.

Legacy (pre-v2) rows lacking `schema_version` are tolerated:
`InvocationEvent.from_jsonl` returns None and logs a one-line warn so
old `msm status <run_dir>` invocations don't blow up.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, ClassVar, Iterable, Literal, Optional, Union
import json

from ..caching.keys import LIN_PAYLOAD_VERSION, canonical_cbor, multihash_key
from ..logging import Log


INVOCATION_EVENT_SCHEMA_VERSION = 2


# Closed Literal aliases — used by docstrings + dataclass fields.
InvocationStatus = Literal["hit", "miss", "promoted", "fail"]
TimeSource = Literal["orchestrator", "worker"]
GroupStrategy = Literal["groupTuple", "collect", "flat"]
LeafSource = Literal["user_added", "imported"]
LogStatus = Literal[
    "available",
    "missing",
    "pruned",
    "legacy_shard_no_logs",
    "remote_cache_no_logs",
    "not_applicable",
]


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class TraceCorruptError(Exception):
    """Trace file has a malformed JSON line. Reports the byte offset."""

    def __init__(self, byte_offset: int, reason: str = ""):
        self.byte_offset = byte_offset
        self.reason = reason
        super().__init__(f"trace corrupt at byte {byte_offset}: {reason}")


class TraceAlreadyAttached(Exception):
    """attach_trace called twice with different paths."""


class InstanceNotFound(KeyError):
    """A `get_lineage_of(...)` lookup didn't resolve to a known instance."""


class InvocationNotFound(KeyError):
    """A `get_invocation(...)` lookup didn't find the named task_hash."""


class MissingInstanceError(KeyError):
    """Bootstrap couldn't resolve a required input instance_id."""

    def __init__(self, instance_id: Optional[str], dep_key: str):
        self.instance_id = instance_id
        self.dep_key = dep_key
        super().__init__(
            f"required instance_id={instance_id!r} missing for dep={dep_key!r}"
        )


class ArityMismatchError(ValueError):
    """Bootstrap saw a different number of resolved files than `sar` expected."""

    def __init__(self, expected: int, actual: int, dep_key: str):
        self.expected = expected
        self.actual = actual
        self.dep_key = dep_key
        super().__init__(
            f"arity mismatch for dep={dep_key!r}: expected {expected}, got {actual}"
        )


# ---------------------------------------------------------------------------
# Lin payload — wire format on the Nextflow channel
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LinPayload:
    """Slot-level lineage tag carried on Nextflow channel values.

    The on-wire shape is `{"v": LIN_PAYLOAD_VERSION, "entries": [<map>, ...]}`
    — ONE map per batch member, mirroring the list of indexes
    `Orchestrator._collateBatch` builds. A `batch_size=1` step carries a
    length-1 list; a `batch_size=N` step carries N, and bootstrap turns each
    into one `context.AsBatch()` member. Carrying a single map (wire v2)
    silently discarded every member after the first, so a batched transform
    processed one item and staged the rest unread.

    Each map is `slot_name -> <value>`, where the value is either a list of
    lineage-index hashes (for normal slots) or a `list[list[str]]` of file
    groups for the special "FILES" key Orchestrator injects at task entry.
    `slot_name` keys are the channel-label hashes (`prod_name`) emitted by
    `Orchestrator.groovy`'s namespace — bootstrap looks up actual
    `DataInstance` objects via the `din` line of `workflow.step_N.meta`, not
    via the `lin` payload directly. The payload is the lineage *trace*
    through the DAG, not the input-id list.

    File-level identity (`file_instance_id`) is *not* on the wire — it
    is minted post-facto by `CollectResults` via `mint_file_id`.
    """

    v: int
    entries: list[dict[str, Any]] = field(default_factory=list)

    VERSION: ClassVar[int] = LIN_PAYLOAD_VERSION
    FILES_KEY: ClassVar[str] = "FILES"
    PROV_KEY: ClassVar[str] = "PROV"

    # Keys the Orchestrator injects at task entry and strips at task exit.
    # Neither is lineage: their values are not ancestry-hash lists, so anything
    # that treats an entry as `{slot: [hash, ...]}` has to exclude them. That
    # includes the cache -- `promote` captures `lineage_index` into a shard
    # manifest and the cache-hit path renders it back into a generated `.nf`,
    # where a nested value would stringify into silent garbage.
    RESERVED_KEYS: ClassVar[frozenset[str]] = frozenset({"FILES", "PROV"})

    def Pack(self) -> dict:
        return {"v": self.v, "entries": [dict(m) for m in self.entries]}

    def to_json(self) -> str:
        return json.dumps(self.Pack(), separators=(",", ":"))

    @classmethod
    def Unpack(cls, raw: dict) -> "LinPayload":
        if not isinstance(raw, dict):
            # A bare list is the pre-envelope shape. Report it as a wire
            # error rather than an AttributeError three frames deep, so the
            # caller's `except ValueError` actually catches it.
            raise ValueError(
                f"lin payload must be a {{v, entries}} envelope, got "
                f"{type(raw).__name__}"
            )
        v = raw.get("v")
        if v != LIN_PAYLOAD_VERSION:
            raise ValueError(
                f"unsupported lin payload version {v!r}; expected {LIN_PAYLOAD_VERSION}"
            )
        entries_raw = raw.get("entries", [])
        if not isinstance(entries_raw, list):
            raise ValueError(
                f"lin payload entries must be a list of per-batch-member maps, "
                f"got {type(entries_raw).__name__}"
            )
        for i, m in enumerate(entries_raw):
            if not isinstance(m, dict):
                raise ValueError(
                    f"lin payload member {i} must be a map, got {type(m).__name__}"
                )
        return cls(v=v, entries=[dict(m) for m in entries_raw])

    @classmethod
    def from_json(cls, raw: str) -> "LinPayload":
        return cls.Unpack(json.loads(raw))

    def file_groups(self, member: int) -> list[list[str]]:
        """Pull the Orchestrator-injected `FILES` value for one batch member."""
        return self.entries[member].get(self.FILES_KEY, [])

    def provenance_groups(self, member: int) -> list[list[dict]]:
        """Per-item index maps for one batch member, aligned 1:1 with `file_groups`.

        `group()` builds this and `FILES` from one value in one closure, so
        `provenance_groups(m)[s][i]` describes `file_groups(m)[s][i]` by
        construction rather than by an ordering anyone has to maintain.

        Empty when the producing runtime did not emit it -- the virtual runtime,
        a direct run, the transform harness, or any step with no inputs. Absence
        means "not captured", never "this file has no ancestors", and every
        consumer has to treat the two differently.
        """
        return self.entries[member].get(self.PROV_KEY, [])

    def lineage_index(self, member: int) -> dict[str, list[int]]:
        """Return one batch member's entries minus the Orchestrator's own keys."""
        return {
            k: v for k, v in self.entries[member].items()
            if k not in self.RESERVED_KEYS
        }

    FILE_ID_SEP: ClassVar[str] = "::"

    @staticmethod
    def mint_file_id(slot_id: str, relative_path: Union[str, Path]) -> str:
        """Deterministic `file_instance_id` per (slot, file basename).

        Single point of provenance: the file identity is the md5 of the
        composite `"{slot_id}::{basename}"`. md5 is chosen deliberately —
        it is the one strong-ish digest the Nextflow `Orchestrator.groovy`
        runtime can compute (`"${slot_id}::${item.name}".md5()`) to
        reproduce the *exact same* id on the channel, collapsing the
        on-channel and off-channel identity to one value (G1). This
        codebase already relies on Groovy/Python md5 agreement (the
        given-lineage seed mirrors `Long.parseLong(md5(...)[0..14],16)`),
        so the match is proven. The result is 32 lowercase hex chars, so
        it stays a valid instance-id token everywhere a hex id is expected
        (e.g. `telemetry._HEX_RE`, cache-shard prefixing).

        The path is normalised to its basename: promote stages outputs
        under `<shard>/out/<name>` while the channel only ever sees the
        basename, so basename is the one representation both sides share.
        Basenames are unique within a slot (the canonical filename embeds
        batch/branch/hash/dtype), so this stays collision-free per slot.

        Called from `CollectResults`, `promote_run`, and mirrored by
        `Orchestrator._post`. Re-run with a cache hit produces the same
        id, satisfying G1's deterministic identity postcondition.
        """
        from hashlib import md5

        name = Path(str(relative_path)).name
        composite = f"{slot_id}{LinPayload.FILE_ID_SEP}{name}"
        return md5(composite.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# InvocationEvent — canonical trace.jsonl row
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProducedFile:
    """One file emitted by a transform invocation.

    Carries both ids: `slot_id` (wire identity) and `file_instance_id`
    (post-facto, the id `DataInstanceLibrary` queries route through).

    `parents` is this file's **per-output ancestry**: the concrete input
    `instance_id`s *this* file descends from. It is the single-point-of-
    provenance refinement of the event-level `InvocationEvent.consumes`
    map (which is shared across every output of the invocation and so
    cannot distinguish which output came from which input — the root of
    the cartesian sibling-walk bug). An empty `parents` means "not
    recorded" (legacy rows, or a producer that hasn't captured runtime
    ancestry yet); consumers fall back to the event-level `consumes`.
    """

    file_instance_id: str
    slot_id: str
    path: str
    dtype_key: str = ""
    parents: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = {
            "file_instance_id": self.file_instance_id,
            "slot_id": self.slot_id,
            "path": self.path,
            "dtype_key": self.dtype_key,
        }
        # additive-with-default: only emit when populated so byte-identical
        # to pre-refactor rows whenever ancestry wasn't captured.
        if self.parents:
            d["parents"] = list(self.parents)
        return d

    @classmethod
    def from_dict(cls, raw: dict) -> "ProducedFile":
        return cls(
            file_instance_id=raw["file_instance_id"],
            slot_id=raw["slot_id"],
            path=raw["path"],
            dtype_key=raw.get("dtype_key", ""),
            parents=list(raw.get("parents", [])),
        )


@dataclass(frozen=True)
class GroupingFrame:
    """Group-fanin context for a task whose inputs were aggregated."""

    strategy: GroupStrategy
    group_key: str
    sibling_instance_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "strategy": self.strategy,
            "group_key": self.group_key,
            "sibling_instance_ids": list(self.sibling_instance_ids),
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "GroupingFrame":
        return cls(
            strategy=raw["strategy"],
            group_key=raw["group_key"],
            sibling_instance_ids=list(raw.get("sibling_instance_ids", [])),
        )


@dataclass(frozen=True)
class SessionStart:
    """Sentinel row at the head of every fresh trace.jsonl.

    Distinguishes session boundaries when the telemetry API unions
    across rotated `trace.<session_id>.jsonl` siblings.
    """

    session_id: int
    compile_started_at: str
    metasmith_version: str = ""
    schema_version: int = INVOCATION_EVENT_SCHEMA_VERSION

    EVENT_NAME: ClassVar[str] = "session_start"

    def to_dict(self) -> dict:
        return {
            "event": self.EVENT_NAME,
            "session_id": self.session_id,
            "compile_started_at": self.compile_started_at,
            "metasmith_version": self.metasmith_version,
            "schema_version": self.schema_version,
        }

    def to_jsonl(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, raw: dict) -> "SessionStart":
        return cls(
            session_id=raw["session_id"],
            compile_started_at=raw.get("compile_started_at", ""),
            metasmith_version=raw.get("metasmith_version", ""),
            schema_version=raw.get("schema_version", INVOCATION_EVENT_SCHEMA_VERSION),
        )


@dataclass(frozen=True)
class InvocationEvent:
    """One transform-invocation row in trace.jsonl.

    `status` covers every transition the runtime emits:
      - "hit"      — compile-time cache probe matched; no execution.
      - "miss"     — executed (no prior shard).
      - "promoted" — executed AND outputs promoted to the cache shard.
      - "fail"     — executed AND non-zero exit / partial failure.

    Raises
    ------
    nothing on construction. `from_jsonl` returns `None` on a legacy
    (pre-v2) row missing `schema_version`, with a one-line warn — so
    `msm status <old_run_dir>` keeps working.
    """

    task_hash: str
    transform_key: str
    status: InvocationStatus
    consumes: dict[str, list[str]] = field(default_factory=dict)
    produces: list[ProducedFile] = field(default_factory=list)
    session_id: int = 0
    group: Optional[GroupingFrame] = None
    work_dir: Optional[str] = None
    nxf_task_id: Optional[str] = None
    exit_code: Optional[int] = None
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    time_source: TimeSource = "orchestrator"
    container: Optional[str] = None
    host: Optional[str] = None
    step_order: Optional[int] = None
    step_name: Optional[str] = None
    cache_key: Optional[str] = None
    schema_version: int = INVOCATION_EVENT_SCHEMA_VERSION

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "schema_version": self.schema_version,
            "session_id": self.session_id,
            "task_hash": self.task_hash,
            "transform_key": self.transform_key,
            "status": self.status,
            "consumes": {k: list(v) for k, v in self.consumes.items()},
            "produces": [p.to_dict() for p in self.produces],
            "time_source": self.time_source,
        }
        for k in (
            "group",
            "work_dir",
            "nxf_task_id",
            "exit_code",
            "started_at",
            "ended_at",
            "container",
            "host",
            "step_order",
            "step_name",
            "cache_key",
        ):
            v = getattr(self, k)
            if v is None:
                continue
            d[k] = v.to_dict() if k == "group" else v
        return d

    def to_jsonl(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @classmethod
    def from_dict(cls, raw: dict) -> Optional["InvocationEvent"]:
        if raw.get("event") == SessionStart.EVENT_NAME:
            return None
        if "schema_version" not in raw:
            Log.Warn(
                "trace.jsonl row missing schema_version; legacy v1 row skipped"
            )
            return None
        group_raw = raw.get("group")
        group = GroupingFrame.from_dict(group_raw) if group_raw else None
        produces = [ProducedFile.from_dict(p) for p in raw.get("produces", [])]
        consumes = {k: list(v) for k, v in raw.get("consumes", {}).items()}
        return cls(
            task_hash=raw["task_hash"],
            transform_key=raw["transform_key"],
            status=raw["status"],
            consumes=consumes,
            produces=produces,
            session_id=raw.get("session_id", 0),
            group=group,
            work_dir=raw.get("work_dir"),
            nxf_task_id=raw.get("nxf_task_id"),
            exit_code=raw.get("exit_code"),
            started_at=raw.get("started_at"),
            ended_at=raw.get("ended_at"),
            time_source=raw.get("time_source", "orchestrator"),
            container=raw.get("container"),
            host=raw.get("host"),
            step_order=raw.get("step_order"),
            step_name=raw.get("step_name"),
            cache_key=raw.get("cache_key"),
            schema_version=raw["schema_version"],
        )

    @classmethod
    def from_jsonl(cls, line: str) -> Optional["InvocationEvent"]:
        raw = json.loads(line)
        return cls.from_dict(raw)


def append_invocation_event(trace_path: Path, event: InvocationEvent) -> None:
    """Single writer for trace.jsonl. Append-only, one JSON object per line."""
    with open(trace_path, "a", encoding="utf-8") as f:
        f.write(event.to_jsonl())
        f.write("\n")


# ---------------------------------------------------------------------------
# User-facing value types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LeafRecord:
    """Provenance record for an instance with no producing transform.

    Sits where `LineageNode.produced_by` would normally hold an
    `InvocationEvent`. Distinguishes user-added leaves from leaves
    pulled in via `metasmith data import-library`.
    """

    source: LeafSource
    added_at: str = ""
    origin_workspace: Optional[str] = None

    def to_dict(self) -> dict:
        d = {"kind": "leaf", "source": self.source, "added_at": self.added_at}
        if self.origin_workspace is not None:
            d["origin_workspace"] = self.origin_workspace
        return d


@dataclass(frozen=True)
class LineageNode:
    """Frozen snapshot of one node in the lineage graph.

    `inputs` is always present (empty dict for leaves), keyed by the
    transform-side dep_key. Holding a stale node is safe — the snapshot
    is taken at attach-time; `DataInstanceLibrary.refresh()` produces a
    new tree.
    """

    instance_id: str
    dtype_key: str
    path: str
    produced_by: Union[InvocationEvent, LeafRecord]
    inputs: dict[str, list["LineageNode"]] = field(default_factory=dict)
    group: Optional[GroupingFrame] = None

    def to_json(self, *, indent: Optional[int] = None, depth: Optional[int] = None) -> str:
        """Serialize to JSON.

        `depth` (S7): cap the walk distance from the root. None = no cap
        (every reachable ancestor). 0 = root only with `inputs={}`. Matches
        `to_mermaid`'s depth semantics.
        """
        def encode(node: "LineageNode", remaining: Optional[int]) -> dict:
            pb = node.produced_by
            if isinstance(pb, InvocationEvent):
                produced = {"kind": "invocation", **pb.to_dict()}
            else:
                produced = pb.to_dict()
            if remaining is None:
                child_remaining = None
                inputs = {
                    k: [encode(child, None) for child in v]
                    for k, v in node.inputs.items()
                }
            elif remaining <= 0:
                inputs = {}
            else:
                child_remaining = remaining - 1
                inputs = {
                    k: [encode(child, child_remaining) for child in v]
                    for k, v in node.inputs.items()
                }
            d = {
                "instance_id": node.instance_id,
                "dtype_key": node.dtype_key,
                "path": node.path,
                "produced_by": produced,
                "inputs": inputs,
            }
            if node.group is not None:
                d["group"] = node.group.to_dict()
            return d

        return json.dumps(encode(self, depth), indent=indent, separators=(",", ":") if indent is None else None)

    def to_mermaid(
        self,
        *,
        depth: int = 2,
        include_groups: bool = True,
    ) -> str:
        """Render the snapshot as a `graph TD` mermaid diagram.

        `depth` caps the walk distance from the root; `include_groups`
        toggles annotation of group-fanin frames on edges.
        """

        lines = ["graph TD"]
        seen: set[str] = set()

        def node_id(n: "LineageNode") -> str:
            return f"n_{n.instance_id[:10]}"

        def label(n: "LineageNode") -> str:
            return f"{n.dtype_key}\\n{n.instance_id[:8]}"

        def emit(node: "LineageNode", remaining: int) -> None:
            nid = node_id(node)
            if nid not in seen:
                lines.append(f'  {nid}["{label(node)}"]')
                seen.add(nid)
            if remaining <= 0:
                return
            for dep_key, children in node.inputs.items():
                edge_label = dep_key
                if include_groups and node.group is not None:
                    edge_label = f"{dep_key} [{node.group.strategy}]"
                for child in children:
                    cid = node_id(child)
                    if cid not in seen:
                        lines.append(f'  {cid}["{label(child)}"]')
                        seen.add(cid)
                    lines.append(f"  {cid} -->|{edge_label}| {nid}")
                    emit(child, remaining - 1)

        emit(self, depth)
        return "\n".join(lines)


@dataclass(frozen=True)
class LogBundle:
    """Canonical handle to a task's `.command.*` logs.

    `status` is the discriminator — callers branch on it, not on whether
    individual path fields are None. Bare paths are never returned for
    "logs unavailable" cases.
    """

    status: LogStatus
    stdout: Optional[Path] = None
    stderr: Optional[Path] = None
    command_sh: Optional[Path] = None
    command_log: Optional[Path] = None
    reason: Optional[str] = None

    def is_available(self) -> bool:
        return self.status == "available"
