from __future__ import annotations

import json
import os
import re
import shutil
import socket
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

from .keys import canonical_cbor
from .layout import (
    MANIFEST_NAME,
    lock_file,
    logs_dir as _logs_dir,
    out_dir as _out_dir,
    shard_dir as _shard_dir,
    staging_dir,
)
from .store import CacheStore, encode_manifest


@dataclass(frozen=True)
class StepPromoteSpec:
    order: int
    cache_key: bytes
    cacheable: bool
    transform_key: str
    signature: str
    out_identities: dict[str, str]
    dep_out: list[dict]
    step_name: str = ""
    sorted_inputs: list = field(default_factory=list)
    slot_files: list = field(default_factory=list)
    batches: list = field(default_factory=list)


def _read_step_meta(meta_path: Path) -> StepPromoteSpec | None:
    name = meta_path.stem
    try:
        order = int(name.rsplit("_", 1)[1])
    except (IndexError, ValueError):
        return None

    cache_key_hex: str | None = None
    cacheable = True
    out_identities: dict[str, str] = {}
    transform_key = ""
    step_name = ""
    signature = ""
    dep_out: list[dict] = []
    sorted_inputs: list = []
    slot_files: list = []
    batches: list = []

    for line in meta_path.read_text().splitlines():
        if not line.strip():
            continue
        head, _, rest = line.partition(" ")
        if head == "cache_key":
            cache_key_hex = rest.strip()
        elif head == "cacheable":
            cacheable = rest.strip().lower() == "true"
        elif head == "out_identities":
            try:
                out_identities = json.loads(rest)
            except json.JSONDecodeError:
                pass
        elif head == "dot":
            try:
                dep_out = json.loads(rest)
            except json.JSONDecodeError:
                pass
        elif head == "transform_key":
            transform_key = rest.strip()
        elif head == "step_name":
            step_name = rest.strip()
        elif head == "sorted_inputs":
            try:
                raw = json.loads(rest)
            except json.JSONDecodeError:
                raw = []
            sorted_inputs = []
            for entry in raw:
                if not isinstance(entry, list) or len(entry) != 2:
                    continue
                k, v = entry
                if isinstance(v, list):
                    sorted_inputs.append((k, [str(x) for x in v]))
                elif isinstance(v, str):
                    sorted_inputs.append((k, v.split("+") if v else []))
        elif head == "slot_files":
            try:
                slot_files = json.loads(rest)
                if not isinstance(slot_files, list):
                    slot_files = []
            except json.JSONDecodeError:
                slot_files = []
        elif head == "batches":
            try:
                raw = json.loads(rest)
            except json.JSONDecodeError:
                raw = []
            if isinstance(raw, list):
                batches = []
                for b in raw:
                    if not isinstance(b, dict):
                        continue
                    bsi = []
                    for entry in b.get("sorted_inputs", []):
                        if isinstance(entry, list) and len(entry) == 2:
                            k, v = entry
                            if isinstance(v, list):
                                bsi.append((k, [str(x) for x in v]))
                            elif isinstance(v, str):
                                bsi.append((k, v.split("+") if v else []))
                    batches.append({
                        "batch_idx": int(b.get("batch_idx", len(batches))),
                        "start": int(b.get("start", 0)),
                        "end": int(b.get("end", 0)),
                        "sorted_inputs": bsi,
                    })
    if cache_key_hex is None:
        return None
    return StepPromoteSpec(
        order=order,
        cache_key=bytes.fromhex(cache_key_hex),
        cacheable=cacheable,
        transform_key=transform_key,
        step_name=step_name,
        signature=signature,
        out_identities=out_identities,
        dep_out=dep_out,
        sorted_inputs=sorted_inputs,
        slot_files=slot_files,
        batches=batches,
    )


def _acquire_lock(cache_root: Path, key_hex: str) -> Path | None:
    lock = lock_file(cache_root, key_hex)
    pid = os.getpid()
    host = socket.gethostname()
    payload = f"{pid} {host} {time.time():.6f}\n"
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        os.write(fd, payload.encode())
        os.close(fd)
        return lock
    except FileExistsError:
        pass
    try:
        existing = lock.read_text().strip().split()
        if len(existing) >= 3:
            old_pid = int(existing[0])
            old_host = existing[1]
            old_ts = float(existing[2])
            stale_same_host = old_host == host and not _pid_alive(old_pid)
            stale_other_host = old_host != host and (time.time() - old_ts) > 3600
            if stale_same_host or stale_other_host:
                lock.unlink(missing_ok=True)
                return _acquire_lock(cache_root, key_hex)
    except (OSError, ValueError):
        pass
    return None


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False
    except OSError:
        return False


def _release_lock(lock: Path) -> None:
    lock.unlink(missing_ok=True)


def _find_step_outputs(workspace: Path, step_order: int) -> list[Path]:
    out: list[Path] = []
    step_dir = workspace / "nxf_work" / f"step_{step_order:02}"
    if not step_dir.exists():
        return out
    _walk_step_outputs(step_dir, out)
    return sorted(out)


def _walk_step_outputs(directory: Path, out: list[Path]) -> None:
    # A directory named in the canonical output spelling IS one output. The
    # walk stops there: descending would promote its members as separate files
    # and the directory itself would never reach the shard. Every other
    # directory -- the run's own layout, `batch_*` -- is a container.
    try:
        entries = sorted(directory.iterdir())
    except OSError:
        return
    for fp in entries:
        if fp.name.startswith(".command") or fp.name == "META":
            continue
        if fp.is_dir():
            if fp.is_symlink():
                # A staged input, not a product.
                continue
            if CANONICAL_OUTPUT_PREFIX.match(fp.name):
                out.append(fp)
            else:
                _walk_step_outputs(fp, out)
            continue
        out.append(fp)


CANONICAL_OUTPUT_PREFIX = re.compile(r"^(\d+)-(\d+)-(\d+)\.")


def _entry_size(path: Path) -> int:
    # What the shard costs on disk, which is what `msm cache gc --max-size`
    # budgets against. A directory's own inode size answers a few kilobytes for
    # a product of any size.
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
    file's own identity is the entry under the slot's channel name — the exact
    direct parents. `PROV` is stamped by `Orchestrator.group`, so a step that
    never groups has none; there the member's own index stands in, which is the
    same ancestry flattened rather than a guess at it.

    Never paired against a mismatched arity: a mispaired parent answers
    confidently and wrongly, which is the failure this record exists to remove.
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


def _match_output_slot(name: str, slot_files: list[dict]) -> dict | None:
    """Which declared output slot a file named `<batch>-<i>-<branch>.<hash>-<key><ext>` fills."""
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
        slot_files,
        key=lambda d: len(str(d.get("dtype_key", ""))),
        reverse=True,
    ):
        if sf.get("branch_idx") != branch_idx:
            continue
        dk = sf.get("dtype_key", "")
        if dk and name.endswith(f"-{dk}{sf.get('ext', '')}"):
            return sf
    return None


def _batch_idx_of(path: Path, have_batch_dirs: bool, counter: list[int]) -> int:
    if path.parent.name.startswith("batch_"):
        try:
            return int(path.parent.name.split("_")[1])
        except (IndexError, ValueError):
            return -1
    if have_batch_dirs:
        return -1
    counter[0] += 1
    return counter[0] - 1


def _collect_output_provenance(workspace: Path) -> dict[str, dict]:
    """Per output filename, the on-channel index and direct parents of its task.

    Both come off the `.command.metadata` the task itself wrote, which is the
    only first-hand record of what it read. A name is dropped when two tasks
    claim it with different answers.
    """
    from ..models.lineage import LinPayload
    from ..models.workflow import METADATA_FILE

    # `nxf_work` is where the runner points nextflow; a caller that does not
    # pass `-work-dir` gets nextflow's own default beside the workspace, and the
    # record has to be found either way.
    root = workspace / "nxf_work"
    if not root.exists():
        root = workspace
    found: dict[str, list[dict]] = {}
    for meta in root.rglob(METADATA_FILE):
        payload = None
        slot_channels: list[str] = []
        try:
            for line in meta.read_text(errors="replace").splitlines():
                head, _, rest = line.partition(" ")
                if head == "lin":
                    payload = LinPayload.from_json(rest)
                elif head == "slk":
                    slot_channels = list(json.loads(rest).values())
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if payload is None:
            continue
        try:
            siblings = list(meta.parent.iterdir())
        except OSError:
            continue
        for fp in siblings:
            # Staged inputs are symlinks; products are not.
            if fp.is_symlink():
                continue
            m = CANONICAL_OUTPUT_PREFIX.match(fp.name)
            if m is None:
                continue
            member = int(m.group(1)) - 1
            if not (0 <= member < len(payload.entries)):
                continue
            found.setdefault(fp.name, []).append({
                "index": payload.lineage_index(member),
                "parents": direct_parents(payload, slot_channels, member),
            })
    out: dict[str, dict] = {}
    for name, candidates in found.items():
        first = candidates[0]
        if all(c == first for c in candidates[1:]):
            out[name] = first
    return out


def _find_step_logs(workspace: Path, step_order: int) -> list[Path]:
    out: list[Path] = []
    step_dir = workspace / "nxf_work" / f"step_{step_order:02}"
    if not step_dir.exists():
        return out
    for fp in sorted(step_dir.rglob(".command.*")):
        if not fp.is_file():
            continue
        out.append(fp)
    return out


def recover_orphan_tmp_dirs(
    cache_root: Path, owned_keys: Iterable[str]
) -> dict[str, str]:
    actions: dict[str, str] = {}
    if not cache_root.exists():
        return actions
    for key_hex in sorted(set(owned_keys)):
        tmp = staging_dir(cache_root, key_hex)
        if not tmp.is_dir():
            continue
        if (tmp / MANIFEST_NAME).exists():
            final = _shard_dir(cache_root, key_hex)
            final.parent.mkdir(parents=True, exist_ok=True)
            if final.exists():
                shutil.rmtree(tmp)
                actions[key_hex] = "raced"
            else:
                tmp.rename(final)
                actions[key_hex] = "promoted"
        else:
            try:
                shutil.rmtree(tmp)
                actions[key_hex] = "deleted"
            except OSError as e:
                actions[key_hex] = f"delete-failed: {e}"
    return actions


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


def _parse_batch_idx_from_relpath(relpath: str) -> int:
    name = Path(relpath).name
    prefix = name.partition(".")[0]
    tokens = prefix.split("-")
    if len(tokens) < 3:
        return -1
    try:
        return int(tokens[0]) - 1
    except ValueError:
        return -1


def _append_invocation_event_v2(
    workspace: Path,
    *,
    session_id: int,
    spec: "StepPromoteSpec",
    status: str,
    cache_key_hex: str,
    files_meta: list[dict] | None = None,
) -> None:
    from ..models.lineage import (
        InvocationEvent,
        LinPayload,
        ProducedFile,
        append_invocation_event,
    )

    trace_dir = workspace / "_metasmith"
    trace_dir.mkdir(parents=True, exist_ok=True)
    trace_path = trace_dir / "trace.jsonl"

    batched: dict[int, list[dict]] = {}
    have_per_file = files_meta is not None and any(
        f.get("slot_id") and not f.get("unmatched")
        for f in (files_meta or [])
    )
    if have_per_file:
        for f in (files_meta or []):
            if f.get("unmatched") or not f.get("slot_id"):
                continue
            bi = f.get("batch_idx", None)
            if bi is None or bi < 0:
                bi = _parse_batch_idx_from_relpath(f.get("relpath", ""))
            batched.setdefault(int(bi), []).append(f)

    aggregate_consumes = {slot_key: list(ids) for slot_key, ids in spec.sorted_inputs}
    consumes_for_batch: dict[int, dict[str, list[str]]] = {}
    if len(spec.batches) > 1:
        for b in spec.batches:
            bi = int(b.get("batch_idx", 0))
            consumes_for_batch[bi] = {
                slot_key: list(ids)
                for slot_key, ids in b.get("sorted_inputs", [])
            }

    def _emit(batch_idx: int, produces: list[ProducedFile]) -> None:
        if len(spec.batches) > 1 or len(batched) > 1:
            task_hash = f"{cache_key_hex}:{batch_idx}"
        else:
            task_hash = cache_key_hex
        consumes = consumes_for_batch.get(batch_idx, aggregate_consumes)
        event = InvocationEvent(
            task_hash=task_hash,
            transform_key=spec.transform_key,
            status=status,  # type: ignore[arg-type]
            consumes=consumes,
            produces=produces,
            session_id=session_id,
            step_order=spec.order,
            step_name=spec.step_name,
            cache_key=cache_key_hex,
            time_source="orchestrator",
        )
        append_invocation_event(trace_path, event)

    if have_per_file and batched:
        for batch_idx in sorted(batched.keys()):
            if batch_idx < 0:
                continue
            produces: list[ProducedFile] = []
            for f in sorted(batched[batch_idx], key=lambda d: d.get("relpath", "")):
                sid = f.get("slot_id", "")
                rel = f.get("relpath", "")
                dk = f.get("dtype_key", "")
                produces.append(
                    ProducedFile(
                        file_instance_id=LinPayload.mint_file_id(
                            slot_id=sid, relative_path=rel
                        ),
                        slot_id=sid,
                        path=rel,
                        dtype_key=dk,
                        parents=list(f.get("parents") or []),
                    )
                )
            if produces:
                _emit(batch_idx, produces)
    else:
        produces = []
        for slot_branch, instance_id_hex in sorted(spec.out_identities.items()):
            dtype_key = slot_branch.split("::", 1)[0]
            produces.append(
                ProducedFile(
                    file_instance_id=instance_id_hex,
                    slot_id=instance_id_hex,
                    path="",
                    dtype_key=dtype_key,
                )
            )
        _emit(0, produces)


def _record_uncacheable_step(
    workspace: Path, spec, output_provenance: dict[str, dict]
) -> None:
    outputs = _find_step_outputs(workspace, spec.order)
    if not outputs:
        return
    have_batch_dirs = any(p.parent.name.startswith("batch_") for p in outputs)
    counter = [0]
    files_meta: list[dict] = []
    for src in outputs:
        batch_idx = _batch_idx_of(src, have_batch_dirs, counter)
        matched = _match_output_slot(src.name, spec.slot_files)
        if matched is None:
            files_meta.append({"relpath": src.name, "unmatched": True})
            continue
        prov = output_provenance.get(src.name) or {}
        files_meta.append({
            "relpath": f"out/{src.name}",
            "slot_id": matched.get("slot_id", ""),
            "dtype_key": matched.get("dtype_key", ""),
            "branch_idx": matched.get("branch_idx", 0),
            "batch_idx": batch_idx,
            "parents": list(prov.get("parents") or []),
        })
    _append_invocation_event_v2(
        workspace,
        session_id=_read_session_id(workspace),
        spec=spec,
        status="miss",
        cache_key_hex=spec.cache_key.hex(),
        files_meta=files_meta,
    )


def promote_run(
    *,
    workspace: Path,
    cache_root: Path,
    log: list | None = None,
) -> dict:
    log = log if log is not None else []
    cache_root.mkdir(parents=True, exist_ok=True)
    output_provenance = _collect_output_provenance(workspace)
    store = CacheStore.open(cache_root)
    try:
        promoted: list[str] = []
        skipped: list[str] = []
        owned_keys: list[str] = [
            spec.cache_key.hex()
            for spec in (
                _read_step_meta(mp)
                for mp in sorted(workspace.glob("workflow.step_*.meta"))
            )
            if spec is not None and spec.cacheable
        ]
        for meta_path in sorted(workspace.glob("workflow.step_*.meta")):
            spec = _read_step_meta(meta_path)
            if spec is None:
                continue
            if not spec.cacheable:
                # Nothing to promote, but the step still ran and its outputs are
                # still someone's parents. A trace that skips it leaves a hole
                # every consumer downstream of it falls into.
                _record_uncacheable_step(
                    workspace, spec, output_provenance
                )
                continue
            key_hex = spec.cache_key.hex()
            final_dir = _shard_dir(cache_root, key_hex)
            if final_dir.exists():
                store.touch(spec.cache_key)
                skipped.append(key_hex)
                continue
            lock = _acquire_lock(cache_root, key_hex)
            if lock is None:
                skipped.append(key_hex)
                continue
            try:
                tmp = staging_dir(cache_root, key_hex)
                outputs = _find_step_outputs(workspace, spec.order)
                if not outputs and tmp.exists():
                    skip_names = {
                        _out_dir(tmp).name, _logs_dir(tmp).name, MANIFEST_NAME,
                    }
                    outputs = [
                        p for p in tmp.iterdir()
                        if p.name not in skip_names
                        and not p.name.startswith(".command")
                    ]
                if not outputs:
                    skipped.append(key_hex)
                    continue
                tmp.mkdir(parents=True, exist_ok=True)
                out_dir = _out_dir(tmp)
                out_dir.mkdir(parents=True, exist_ok=True)
                files_meta: list[dict] = []
                index_payload: list[dict] = []
                total_bytes = 0
                unmatched: list[str] = []
                no_index: list[str] = []
                files_have_batch_dir = any(
                    p.parent.name.startswith("batch_") for p in outputs
                )
                fallback_batch_counter = [0]
                for src in outputs:
                    batch_dir_idx = _batch_idx_of(
                        src, files_have_batch_dir, fallback_batch_counter
                    )
                    dest = out_dir / src.name
                    try:
                        src_in_tmp = src.parent.resolve() == tmp.resolve()
                    except (OSError, RuntimeError):
                        src_in_tmp = False
                    if src.resolve() != dest.resolve():
                        if src_in_tmp:
                            shutil.move(str(src), str(dest))
                        elif src.is_dir():
                            # A half-copied tree from a killed attempt would
                            # make every retry of this shard raise.
                            if dest.exists():
                                shutil.rmtree(dest)
                            shutil.copytree(src, dest, symlinks=True)
                        else:
                            shutil.copy2(src, dest)
                    relpath = str(dest.relative_to(tmp))
                    total_bytes += _entry_size(dest)
                    name = src.name
                    matched = _match_output_slot(name, spec.slot_files)
                    if matched is not None:
                        prov = output_provenance.get(name) or {}
                        ix = prov.get("index")
                        if not ix:
                            no_index.append(name)
                        else:
                            index_payload.append(
                                {"relpath": relpath, "index": ix}
                            )
                        files_meta.append({
                            "relpath": relpath,
                            "slot_id": matched.get("slot_id", ""),
                            "dtype_key": matched.get("dtype_key", ""),
                            "branch_idx": matched.get("branch_idx", 0),
                            "batch_idx": batch_dir_idx,
                            # What this file's task read. A hit replays the
                            # shard, so the answer has to survive in it.
                            "parents": list(prov.get("parents") or []),
                        })
                    else:
                        files_meta.append({
                            "relpath": relpath,
                            "unmatched": True,
                        })
                        unmatched.append(src.name)
                if no_index:
                    log.append((
                        "warn",
                        f"promote {key_hex[:8]}: no on-channel index captured "
                        f"for {len(no_index)} output(s): {no_index}; a hit on "
                        "this shard will be demoted to a re-run",
                    ))
                if unmatched and spec.slot_files:
                    log.append((
                        "warn",
                        f"promote {key_hex[:8]}: "
                        f"{len(unmatched)} unmatched output(s): {unmatched}",
                    ))
                log_srcs = _find_step_logs(workspace, spec.order)
                if log_srcs:
                    logs_dir = _logs_dir(tmp)
                    logs_dir.mkdir(parents=True, exist_ok=True)
                    for lsrc in log_srcs:
                        try:
                            shutil.copy2(lsrc, logs_dir / lsrc.name)
                        except OSError:
                            continue
                lineage_payload = canonical_cbor(
                    {
                        "tk": spec.transform_key,
                        "sig": spec.signature,
                        "ids": dict(sorted(spec.out_identities.items())),
                    }
                )
                manifest_bytes = encode_manifest(
                    cache_key=spec.cache_key,
                    transform_key=spec.transform_key,
                    signature=spec.signature,
                    lineage_payload=lineage_payload,
                    output_files=files_meta,
                    out_identities=spec.out_identities,
                    index_payload=index_payload,
                )
                (tmp / MANIFEST_NAME).write_bytes(manifest_bytes)
                final_dir.parent.mkdir(parents=True, exist_ok=True)
                try:
                    tmp.rename(final_dir)
                except OSError:
                    shutil.rmtree(tmp, ignore_errors=True)
                    skipped.append(key_hex)
                    continue
                store.upsert(
                    key=spec.cache_key,
                    transform_key=spec.transform_key,
                    payload=manifest_bytes,
                    output_root=str(final_dir.relative_to(cache_root)),
                    size_bytes=total_bytes,
                    origin="lineage",
                )
                promoted.append(key_hex)
                _append_invocation_event_v2(
                    workspace,
                    session_id=_read_session_id(workspace),
                    spec=spec,
                    status="promoted",
                    cache_key_hex=key_hex,
                    files_meta=files_meta,
                )
            finally:
                _release_lock(lock)
        return {
            "promoted": promoted,
            "skipped": skipped,
            "orphan_recovery": recover_orphan_tmp_dirs(cache_root, owned_keys),
        }
    finally:
        store.close()
