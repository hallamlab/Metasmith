"""Promotion of one member's products into its shard, and the run's record of it.

`promote_members` runs inside the task, right after the protocol, on the
member keys the orchestrator stamped. It writes shards and one record line per
member to `.command.cache` beside `.command.metadata`, and never opens sqlite:
a task on a cluster node has no business in the driver's database.

`record_run` runs in the driver once nextflow has exited. It reads every task's
record and the orchestrator's hit log, appends one `InvocationEvent` per
member to the trace, and brings the sqlite index up to date.
"""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from .admission import PoolFile, index_shard, shard_for, write_shard
from .invocation import KEY_KEY, TOMBSTONE_NAME, consumed_of, read_manifest
from .layout import logs_dir as _logs_dir

CACHE_RECORD_FILE = ".command.cache"
CACHE_HITS_LOG = Path("_metasmith") / "cache_hits.jsonl"
CANONICAL_OUTPUT_PREFIX = re.compile(r"^(\d+)-(\d+)-(\d+)\.")
TASK_LOG_NAMES = (".command.out", ".command.err", ".command.sh", ".command.log")


@dataclass(frozen=True)
class StepCacheMeta:
    """What a task knows about its step, read off `.command.metadata`."""
    order: int
    transform_key: str
    signature: str
    step_name: str
    cacheable: bool
    slot_files: list = field(default_factory=list)
    slot_channels: dict = field(default_factory=dict)
    # The trace session this stage opened. A work directory outlives a run,
    # so a record names the session it belongs to and the driver reads only
    # its own.
    session: int = 0

    @property
    def channels(self) -> list[str]:
        return list(self.slot_channels.values())

    @classmethod
    def from_raw(cls, order: int, raw: dict) -> "StepCacheMeta":
        def _json(key, default):
            try:
                v = json.loads(raw.get(key, ""))
            except (json.JSONDecodeError, TypeError):
                return default
            return v if isinstance(v, type(default)) else default
        return cls(
            order=order,
            transform_key=raw.get("transform_key", "").strip(),
            signature=raw.get("signature", "").strip(),
            step_name=raw.get("step_name", "").strip(),
            cacheable=raw.get("cacheable", "false").strip().lower() == "true",
            slot_files=_json("slot_files", []),
            slot_channels=_json("slk", {}),
            session=int(raw.get("session", "0").strip() or 0),
        )


def read_step_meta(meta_path: Path) -> StepCacheMeta | None:
    try:
        order = int(meta_path.stem.rsplit("_", 1)[1])
    except (IndexError, ValueError):
        return None
    raw: dict[str, str] = {}
    for line in meta_path.read_text().splitlines():
        if not line.strip():
            continue
        head, _, rest = line.partition(" ")
        raw[head] = rest
    if "transform_key" not in raw:
        return None
    return StepCacheMeta.from_raw(order, raw)


def _entry_size(path: Path) -> int:
    if not path.is_dir():
        try:
            return path.stat().st_size
        except OSError:
            return 0
    total = 0
    for fp in path.rglob("*"):
        if fp.is_symlink() or not fp.is_file():
            continue
        try:
            total += fp.stat().st_size
        except OSError:
            continue
    return total


def direct_parents(payload, slot_channels: list[str], member: int) -> list[str]:
    """The identities one batch member's task descends from.

    `PROV[s][i]` is the index of the i-th file staged into slot `s`, and that
    file's own identity is the entry under the slot's channel name -- the exact
    direct parents. A step that never groups has none; there the member's own
    index stands in, which is the same ancestry flattened rather than a guess
    at it.
    """
    prov = payload.provenance_groups(member)
    if prov and len(prov) == len(slot_channels):
        parents = [
            pid
            for chan, group in zip(slot_channels, prov)
            for item_index in group
            for pid in item_index.get(chan, [])
        ]
        if parents:
            return sorted(set(parents))
    return sorted({
        pid for ids in payload.lineage_index(member).values() for pid in ids
    })


def match_output_slot(name: str, slot_files: list[dict]) -> dict | None:
    """Which declared output slot a file named `<pos>-<i>-<branch>.<token>-<key><ext>` fills."""
    prefix, _dot, rest = name.partition(".")
    tokens = prefix.split("-")
    if len(tokens) < 3 or not rest:
        return None
    try:
        branch_idx = int(tokens[2]) - 1
    except ValueError:
        return None
    # Longest dtype key first: one key can be a suffix of another.
    for sf in sorted(
        slot_files, key=lambda d: len(str(d.get("dtype_key", ""))), reverse=True,
    ):
        if sf.get("branch_idx") != branch_idx:
            continue
        dk = sf.get("dtype_key", "")
        if dk and name.endswith(f"-{dk}{sf.get('ext', '')}"):
            return sf
    return None


def member_outputs(cwd: Path, position: int) -> list[Path]:
    """The products in `cwd` that the member at `position` (1-based) wrote."""
    out: list[Path] = []
    for fp in sorted(cwd.iterdir()):
        if fp.is_symlink():
            continue
        m = CANONICAL_OUTPUT_PREFIX.match(fp.name)
        if m is None or int(m.group(1)) != position:
            continue
        out.append(fp)
    return out


def promote_members(
    *,
    cwd: Path,
    entries: list[dict],
    meta: StepCacheMeta,
    cache_root: Path,
    successes: list[bool],
    record_file: Path | None = None,
) -> list[dict]:
    """Promote every successful member of one task, and record all of them.

    A member is promoted when it has a key, its protocol reported success, and
    every required branch has a product. The shard is staged under a name no
    other task can pick, then renamed onto its final path; a shard already
    there wins. Returns the records written to `record_file`.
    """
    from ..models.lineage import LinPayload

    payload = LinPayload(v=LinPayload.VERSION, entries=list(entries))
    channels = meta.channels
    required_branches = {
        int(sf.get("branch_idx", 0)) for sf in meta.slot_files
    } if len({int(sf.get("branch_idx", 0)) for sf in meta.slot_files}) == 1 else set()
    records: list[dict] = []
    for member, entry in enumerate(entries):
        position = member + 1
        key_hex = str(entry.get(KEY_KEY, "-") or "-")
        ok = bool(successes[member]) if member < len(successes) else False
        consumed = consumed_of(entry, channels) if channels else None
        files: list[dict] = []
        for src in member_outputs(cwd, position):
            matched = match_output_slot(src.name, meta.slot_files)
            if matched is None:
                continue
            files.append({
                "src": str(src),
                "relpath": f"out/{LinPayload.canonical_output_name(src.name)}",
                "slot_id": matched.get("slot_id", ""),
                "dtype_key": matched.get("dtype_key", ""),
                # The name the slot's type was declared under. Carried so a
                # promoted file can be read back as a data instance; a record
                # written before the compiler wrote it has no name.
                "dtype_name": matched.get("dtype_name", ""),
                "branch_idx": int(matched.get("branch_idx", 0)),
                "parents": direct_parents(payload, channels, member),
                "size": _entry_size(src),
            })
        produces = [
            {k: v for k, v in f.items() if k != "src"} for f in files
        ]
        record = {
            "session": meta.session,
            "step": meta.order,
            "step_name": meta.step_name,
            "member": member,
            "key": key_hex,
            "consumes": consumed or {},
            "produces": produces,
            "lineage": {
                k: v for k, v in entry.items() if k not in LinPayload.RESERVED_KEYS
            },
        }
        if key_hex == "-" or not meta.cacheable:
            record["status"] = "uncacheable"
        elif not ok:
            record["status"] = "failed"
        elif not files or any(
            b not in {f["branch_idx"] for f in files} for b in required_branches
        ):
            record["status"] = "incomplete"
        else:
            record["status"], record["shard"] = _promote_one(
                cache_root, key_hex, meta, files, record,
            )
        records.append(record)

    target = record_file if record_file is not None else cwd / CACHE_RECORD_FILE
    with open(target, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")
    return records


def _promote_one(
    cache_root: Path, key_hex: str, meta: StepCacheMeta, files: list[dict], record: dict,
) -> tuple[str, str]:
    written = write_shard(
        cache_root=cache_root,
        key=bytes.fromhex(key_hex),
        origin="lineage",
        files=[
            PoolFile(
                dtype_name=f.get("dtype_name", ""),
                dtype_key=f.get("dtype_key", ""),
                relpath=f["relpath"],
                slot_id=f.get("slot_id", ""),
                branch_idx=int(f.get("branch_idx", 0)),
                parents=list(f.get("parents") or []),
                size=int(f.get("size", 0)),
                src=f["src"],
            )
            for f in files
        ],
        transform_key=meta.transform_key,
        signature=meta.signature,
        step_name=meta.step_name,
        consumes=record["consumes"],
        lineage=record["lineage"],
    )
    return written.status, str(written.shard)


def _read_session_id(workspace: Path) -> int:
    trace_path = workspace / "_metasmith" / "trace.jsonl"
    if not trace_path.exists():
        return 0
    try:
        for line in trace_path.read_text().splitlines():
            if not line.strip():
                continue
            head = json.loads(line)
            if head.get("event") == "session_start":
                return int(head.get("session_id", 0))
            return 0
    except Exception:
        return 0
    return 0


def _record_files(workspace: Path) -> list[Path]:
    # `nxf_work` is where the runner points nextflow; a caller that does not
    # pass `-work-dir` gets nextflow's own `work` beside the workspace.
    roots = [p for p in (workspace / "nxf_work", workspace / "work") if p.is_dir()]
    return sorted(p for root in roots for p in root.rglob(CACHE_RECORD_FILE))


def _produced_files(files: list[dict]):
    from ..models.lineage import LinPayload, ProducedFile

    out = []
    for f in sorted(files, key=lambda d: d.get("relpath", "")):
        sid = f.get("slot_id", "")
        if not sid:
            continue
        out.append(ProducedFile(
            file_instance_id=LinPayload.mint_file_id(sid, f["relpath"]),
            slot_id=sid,
            path=f["relpath"],
            dtype_key=f.get("dtype_key", ""),
            parents=list(f.get("parents") or []),
        ))
    return out


def _copy_task_logs(task_dir: Path, shard: Path) -> None:
    logs = _logs_dir(shard)
    if logs.exists():
        return
    srcs = [task_dir / n for n in TASK_LOG_NAMES if (task_dir / n).is_file()]
    if not srcs:
        return
    logs.mkdir(parents=True, exist_ok=True)
    for src in srcs:
        try:
            shutil.copy2(src, logs / src.name)
        except OSError:
            continue


def record_run(*, workspace: Path, cache_root: Path, log: list | None = None) -> dict:
    """Append this run's member events to the trace and index its shards."""
    from ..models.lineage import InvocationEvent, append_invocation_event
    from .store import CacheStore

    log = log if log is not None else []
    # The run a product belongs to, as the agent names it on disk: the driver
    # is the only thing that knows, and a store must not go looking.
    run_label = Path(workspace).name
    session_id = _read_session_id(workspace)
    trace_path = workspace / "_metasmith" / "trace.jsonl"
    trace_path.parent.mkdir(parents=True, exist_ok=True)
    meta_by_order = {
        m.order: m for m in (
            read_step_meta(p) for p in sorted(workspace.glob("workflow.step_*.meta"))
        ) if m is not None
    }

    promoted: list[str] = []
    hits: list[str] = []
    cache_root.mkdir(parents=True, exist_ok=True)
    store = CacheStore.open(cache_root)
    try:
        for rec_file in _record_files(workspace):
            try:
                lines = rec_file.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue
            for n, line in enumerate(lines):
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    log.append(("warn", f"unreadable record in {rec_file}"))
                    continue
                if int(rec.get("session", -1)) != session_id:
                    continue
                # One unusable record must not cost the run every other one:
                # this pass is the only thing that writes the run's member
                # events, and it runs after nextflow has already exited.
                try:
                    meta = meta_by_order.get(int(rec.get("step", 0)))
                    status = rec.get("status", "")
                    key_hex = str(rec.get("key", "-"))
                    if status == "promoted":
                        shard = shard_for(cache_root, key_hex, "lineage")
                        manifest = read_manifest(shard) or {}
                        index_shard(
                            store,
                            cache_root=cache_root,
                            key=bytes.fromhex(key_hex),
                            origin="lineage",
                            shard=shard,
                            manifest=manifest,
                            transform_key=meta.transform_key if meta else "",
                            run=run_label,
                        )
                        _copy_task_logs(rec_file.parent, shard)
                        promoted.append(key_hex)
                    event_status = "promoted" if status == "promoted" else (
                        "fail" if status == "failed" else "miss"
                    )
                    if event_status == "fail" and not rec.get("produces"):
                        continue
                    task_hash = key_hex if key_hex != "-" else (
                        f"nokey:{rec.get('step')}:{rec_file.parent.name}:{n}"
                    )
                    append_invocation_event(trace_path, InvocationEvent(
                        task_hash=task_hash,
                        transform_key=meta.transform_key if meta else "",
                        status=event_status,
                        consumes={k: list(v) for k, v in (rec.get("consumes") or {}).items()},
                        produces=_produced_files(rec.get("produces") or []),
                        session_id=session_id,
                        step_order=int(rec.get("step", 0)) or None,
                        step_name=rec.get("step_name") or (meta.step_name if meta else ""),
                        cache_key=key_hex if key_hex != "-" else None,
                        work_dir=str(rec_file.parent),
                    ))
                except Exception as e:
                    log.append(("warn", f"record {n} in {rec_file} not indexed: {e}"))
                    continue

        hits_log = workspace / CACHE_HITS_LOG
        if hits_log.is_file():
            for line in hits_log.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    hit = json.loads(line)
                except json.JSONDecodeError:
                    log.append(("warn", "unreadable line in the cache hit log"))
                    continue
                key_hex = str(hit.get("key", ""))
                shard = shard_for(cache_root, key_hex, "lineage")
                manifest = read_manifest(shard)
                if not key_hex or manifest is None:
                    log.append(("warn", f"hit {key_hex[:8]} has no readable shard"))
                    continue
                try:
                    meta = meta_by_order.get(int(hit.get("step", 0)))
                    entry = hit.get("entry") or {}
                    consumed = consumed_of(entry, meta.channels) if meta else None
                    if store.probe(bytes.fromhex(key_hex)) is None:
                        index_shard(
                            store,
                            cache_root=cache_root,
                            key=bytes.fromhex(key_hex),
                            origin="lineage",
                            shard=shard,
                            manifest=manifest,
                        )
                    store.touch(bytes.fromhex(key_hex))
                    hits.append(key_hex)
                    append_invocation_event(trace_path, InvocationEvent(
                        task_hash=key_hex,
                        transform_key=str(manifest.get("tk", "")),
                        status="hit",
                        consumes=consumed or {k: list(v) for k, v in (manifest.get("consumes") or {}).items()},
                        produces=_produced_files(manifest.get("files") or []),
                        session_id=session_id,
                        step_order=int(hit.get("step", 0)) or None,
                        step_name=hit.get("step_name") or (meta.step_name if meta else ""),
                        cache_key=key_hex,
                    ))
                except Exception as e:
                    log.append(("warn", f"hit {key_hex[:8]} not indexed: {e}"))
                    continue
    finally:
        store.close()
    return {"promoted": promoted, "hits": hits}


def tombstone_shard(cache_root: Path, key_hex: str) -> None:
    shard = shard_for(cache_root, key_hex, "lineage")
    if shard.is_dir():
        (shard / TOMBSTONE_NAME).touch()
