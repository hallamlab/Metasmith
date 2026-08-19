from __future__ import annotations

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, ClassVar, Iterable, Literal, Optional, Union
import json

from ..caching.keys import LIN_PAYLOAD_VERSION, canonical_cbor, multihash_key
from ..logging import Log


INVOCATION_EVENT_SCHEMA_VERSION = 2


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


class TraceCorruptError(Exception):
    def __init__(self, byte_offset: int, reason: str = ""):
        self.byte_offset = byte_offset
        self.reason = reason
        super().__init__(f"trace corrupt at byte {byte_offset}: {reason}")


class TraceAlreadyAttached(Exception):
    pass
class InstanceNotFound(KeyError):
    pass
class InvocationNotFound(KeyError):
    pass
class MissingInstanceError(KeyError):
    def __init__(self, instance_id: Optional[str], dep_key: str):
        self.instance_id = instance_id
        self.dep_key = dep_key
        super().__init__(
            f"required instance_id={instance_id!r} missing for dep={dep_key!r}"
        )


class ArityMismatchError(ValueError):
    def __init__(self, expected: int, actual: int, dep_key: str):
        self.expected = expected
        self.actual = actual
        self.dep_key = dep_key
        super().__init__(
            f"arity mismatch for dep={dep_key!r}: expected {expected}, got {actual}"
        )


@dataclass(frozen=True)
class LinPayload:
    v: int
    entries: list[dict[str, Any]] = field(default_factory=list)

    VERSION: ClassVar[int] = LIN_PAYLOAD_VERSION
    FILES_KEY: ClassVar[str] = "FILES"
    PROV_KEY: ClassVar[str] = "PROV"

    RESERVED_KEYS: ClassVar[frozenset[str]] = frozenset({"FILES", "PROV"})

    def Pack(self) -> dict:
        return {"v": self.v, "entries": [dict(m) for m in self.entries]}

    def to_json(self) -> str:
        return json.dumps(self.Pack(), separators=(",", ":"))

    @classmethod
    def Unpack(cls, raw: dict) -> "LinPayload":
        if not isinstance(raw, dict):
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
        return self.entries[member].get(self.FILES_KEY, [])

    def provenance_groups(self, member: int) -> list[list[dict]]:
        return self.entries[member].get(self.PROV_KEY, [])

    def lineage_index(self, member: int) -> dict[str, list[int]]:
        return {
            k: v for k, v in self.entries[member].items()
            if k not in self.RESERVED_KEYS
        }

    FILE_ID_SEP: ClassVar[str] = "::"

    @staticmethod
    def mint_file_id(slot_id: str, relative_path: Union[str, Path]) -> str:
        from hashlib import md5

        name = Path(str(relative_path)).name
        composite = f"{slot_id}{LinPayload.FILE_ID_SEP}{name}"
        return md5(composite.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ProducedFile:
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
    with open(trace_path, "a", encoding="utf-8") as f:
        f.write(event.to_jsonl())
        f.write("\n")


@dataclass(frozen=True)
class LeafRecord:
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
    instance_id: str
    dtype_key: str
    path: str
    produced_by: Union[InvocationEvent, LeafRecord]
    inputs: dict[str, list["LineageNode"]] = field(default_factory=dict)
    group: Optional[GroupingFrame] = None

    def to_json(self, *, indent: Optional[int] = None, depth: Optional[int] = None) -> str:
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
    status: LogStatus
    stdout: Optional[Path] = None
    stderr: Optional[Path] = None
    command_sh: Optional[Path] = None
    command_log: Optional[Path] = None
    reason: Optional[str] = None

    def is_available(self) -> bool:
        return self.status == "available"
