"""Post-execution promote pass (S5).

After Nextflow finishes, walk every step's `workflow.step_N.meta` to
locate its cache_key and out_identities, find the actual output files
in `<workspace>/nxf_work/step_NN/...` (or already-published files in
`<cache_root>/<key>.tmp/` if Nextflow's publishDir directive ran), and
deposit them into the cache atomically.

Per the plan:
- A lock file at `<cache_root>/<key>.lock` (PID + hostname + monotonic
  ts) is acquired with O_EXCL|O_CREAT before any write.
- The on-disk layout is `<cache_root>/<key_prefix>/<key>/out/<files>`
  with a sidecar `<cache_root>/<key_prefix>/<key>/manifest.cbor`.
- The promote writes to `<key_prefix>/<key>.tmp/` first, then atomic
  rename. Loser-of-race deletes its own .tmp.
- A SQLite row is inserted into `CacheStore.entries` referencing the
  output_root directory.

Orphan recovery: stale `<key>.tmp/` from a prior interrupted run are
detected by the absence of manifest.cbor; they're deleted. Stale dirs
that DO have a manifest.cbor are promoted on the next pass.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import time
from dataclasses import dataclass, field
from pathlib import Path

from .keys import canonical_cbor
from .store import CacheStore, encode_manifest


@dataclass(frozen=True)
class StepPromoteSpec:
    order: int
    cache_key: bytes
    cacheable: bool
    transform_key: str
    signature: str
    out_identities: dict[str, str]  # "{slot}::{branch}" -> instance_id hex
    dep_out: list[dict]  # parsed `dot` line; per-branch dep_key -> [ids]
    # S4a (Bug E): persisted from compile-time `step.transform.name` so
    # the promote route can populate InvocationEvent.step_name to match
    # the cache-hit route's emission schema.
    step_name: str = ""
    # C0: per-slot input ids in the same shape the cache-hit route uses
    # (workflow.py:1419-1422). Each tuple is (slot_key, list[slot_id_hex]).
    # Built at compile time (workflow.py:1279-1290), persisted via the
    # `sorted_inputs` line in step_N.meta, and consumed by
    # `_emit_promote_event` to populate InvocationEvent.consumes.
    sorted_inputs: list = field(default_factory=list)
    # C0.5: per-output-slot file naming info. Each entry is
    #   {"dtype_key", "ext", "branch_idx", "slot_id"}
    # where dtype_key is the DataInstance.dtype.key embedded in the
    # canonical filename (bootstrap.py:196), ext is the preferred
    # extension (e.g. ".bam", ".tar.gz"), branch_idx is the produces-
    # branch index, and slot_id is the channel-level identity. Used to
    # match output files in promote_run unambiguously. Empty list
    # signals a legacy step_N.meta — the promote path falls back to
    # per-slot degenerate emission with a Log.Warn.
    slot_files: list = field(default_factory=list)
    # S3: per-batch decomposition mirroring the compile-time batching
    # algorithm (virtual_runtime._select_instances). Each entry is
    #   {"batch_idx", "start", "end",
    #    "sorted_inputs": [[slot_key, [iid_hex, ...]], ...]}
    # Used by S4 emission to produce one InvocationEvent per batch
    # (= per task) instead of one per step. Empty list signals legacy
    # step_N.meta; emission falls back to step-aggregated path.
    batches: list = field(default_factory=list)


def _shard_dir(cache_root: Path, key_hex: str) -> Path:
    """`<cache_root>/<first 2 hex>/<rest>` — bounded directory fanout."""
    return cache_root / key_hex[:2] / key_hex[2:]


def _read_step_meta(meta_path: Path) -> StepPromoteSpec | None:
    """Parse a workflow.step_N.meta file into a promote spec.

    Returns None when the file lacks cache fields (e.g. legacy or pre-S3
    runs, kill-switch active). Order is taken from the filename.
    """
    name = meta_path.stem  # workflow.step_N
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
            # C0-amend: tolerate two shapes:
            #   - new: [[slot_key, [iid_hex, ...]], ...]
            #   - legacy (C0 f0725e6): [[slot_key, "iid_hex+iid_hex+..."], ...]
            # The legacy form was a +-joined ASCII string of hexes; splitting
            # on "+" recovers the list (hex chars never contain +). Empty
            # string → empty list, not [""].
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
            # S3: list of per-batch dicts with sorted_inputs slice.
            try:
                raw = json.loads(rest)
            except json.JSONDecodeError:
                raw = []
            if isinstance(raw, list):
                # Normalize sorted_inputs entries to (slot_key, [hex,...])
                # tuples so downstream code matches the StepPromoteSpec
                # field shape.
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
    """O_EXCL acquire of `<cache_root>/<key>.lock`. Returns path on success.

    Stale lock detection: same host + dead PID → reclaim; different
    host + ts > 1 hour → reclaim.
    """
    lock = cache_root / f"{key_hex}.lock"
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
    # Maybe stale — inspect.
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
    """Locate output files produced by step <step_order> in nxf_work.

    The virtual_runtime synthesizes files under
        nxf_work/step_NN/batch_XXXX_YYYY/1-1-{branch}.{lin_hash}-{dtype_key}{ext}
    Real Nextflow writes them under task work dirs but publishDir already
    deposits them into the cache.tmp path; this function is the fallback
    for runtimes that didn't run publishDir. We scan only nxf_work and
    return absolute paths to files matching the canonical name shape
    (excluding command/log files starting with `.command`).
    """
    out: list[Path] = []
    step_dir = workspace / "nxf_work" / f"step_{step_order:02}"
    if not step_dir.exists():
        return out
    for fp in sorted(step_dir.rglob("*")):
        if not fp.is_file():
            continue
        if fp.name.startswith(".command"):
            continue
        if fp.name == "META":
            continue
        out.append(fp)
    return out


def _find_step_logs(workspace: Path, step_order: int) -> list[Path]:
    """Locate `.command.{sh,out,err,log}` files for a completed step.

    C8 / G6 — promote captures these into `<shard>/logs/` so
    `DataInstanceLibrary.get_logs_of(any_output).stdout` resolves after
    `rm -rf work/` + resume. Same scan root as `_find_step_outputs`;
    different filter.
    """
    out: list[Path] = []
    step_dir = workspace / "nxf_work" / f"step_{step_order:02}"
    if not step_dir.exists():
        return out
    for fp in sorted(step_dir.rglob(".command.*")):
        if not fp.is_file():
            continue
        out.append(fp)
    return out


def recover_orphan_tmp_dirs(cache_root: Path) -> dict[str, str]:
    """Walk `<cache_root>/*.tmp/`: promote those with manifest.cbor, else rm.

    Returns a {key_hex: 'promoted'|'deleted'} mapping for telemetry.
    """
    actions: dict[str, str] = {}
    if not cache_root.exists():
        return actions
    for tmp in cache_root.glob("*.tmp"):
        if not tmp.is_dir():
            continue
        key_hex = tmp.name[: -len(".tmp")]
        if (tmp / "manifest.cbor").exists():
            final = _shard_dir(cache_root, key_hex)
            final.parent.mkdir(parents=True, exist_ok=True)
            if final.exists():
                shutil.rmtree(tmp)
                actions[key_hex] = "raced"
            else:
                tmp.rename(final)
                actions[key_hex] = "promoted"
        else:
            shutil.rmtree(tmp, ignore_errors=True)
            actions[key_hex] = "deleted"
    return actions


def _read_session_id(workspace: Path) -> int:
    """Recover the active session_id from the SessionStart sentinel.

    Workflow.py:_compute_cache_decisions writes the sentinel as the
    first line of every fresh trace.jsonl (C7); promote_run reads it
    so its emitted InvocationEvents carry the same session_id. Returns
    0 if the file or sentinel is missing — every InvocationEvent still
    parses, just with an uncorrelated session_id.
    """
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


def _append_invocation_event_v2(
    workspace: Path,
    *,
    session_id: int,
    spec: "StepPromoteSpec",
    status: str,
    cache_key_hex: str,
    files_meta: list[dict] | None = None,
) -> None:
    """Append a v2 InvocationEvent row to <workspace>/_metasmith/trace.jsonl.

    C0.5: when `files_meta` carries per-file `slot_id`/`dtype_key`
    entries (post-C0.5 promote), emit one ProducedFile per file with
    `file_instance_id = LinPayload.mint_file_id(slot_id, relpath)`.
    Legacy callers (and miss/fail emissions that have no files_meta yet)
    fall back to the per-slot degenerate emission with `path=""`.
    """
    from ..models.lineage import (
        InvocationEvent,
        LinPayload,
        ProducedFile,
        append_invocation_event,
    )

    trace_dir = workspace / "_metasmith"
    trace_dir.mkdir(parents=True, exist_ok=True)
    trace_path = trace_dir / "trace.jsonl"

    produces: list[ProducedFile] = []
    # C0.5: prefer per-file emission when files_meta carries slot info.
    have_per_file = files_meta is not None and any(
        f.get("slot_id") and not f.get("unmatched")
        for f in (files_meta or [])
    )
    if have_per_file:
        for f in sorted(files_meta or [], key=lambda d: d.get("relpath", "")):
            if f.get("unmatched"):
                # G6: file copied to cache but doesn't belong to any
                # declared slot. Skip — emitting with empty slot_id would
                # pollute TraceIndex.by_slot.
                continue
            sid = f.get("slot_id", "")
            rel = f.get("relpath", "")
            dk = f.get("dtype_key", "")
            if not sid:
                continue
            produces.append(
                ProducedFile(
                    file_instance_id=LinPayload.mint_file_id(
                        slot_id=sid, relative_path=rel
                    ),
                    slot_id=sid,
                    path=rel,
                    dtype_key=dk,
                )
            )
    else:
        # Legacy fallback (miss-without-promote, or pre-C0.5 caller).
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
    # C0-amend: spec.sorted_inputs is list[tuple[str, list[str]]].
    # Mirrors workflow.py:1419-1422 exactly — both routes produce
    # byte-identical consumes dicts of {slot_key: list[slot_id_hex]}.
    # The hex strings ARE the slot_ids that TraceIndex.by_slot indexes
    # on, so walk_ancestors can route by them.
    consumes = {slot_key: list(ids) for slot_key, ids in spec.sorted_inputs}
    event = InvocationEvent(
        task_hash=cache_key_hex,
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


def promote_run(
    *,
    workspace: Path,
    cache_root: Path,
    log: list | None = None,
) -> dict:
    """Promote every step's outputs from a completed run into the cache.

    Returns a summary dict with counts for telemetry; the same data
    feeds `<run_dir>/_metasmith/trace.jsonl` post-exec source: run rows.
    """
    log = log if log is not None else []
    cache_root.mkdir(parents=True, exist_ok=True)
    store = CacheStore.open(cache_root)
    try:
        promoted: list[str] = []
        skipped: list[str] = []
        for meta_path in sorted(workspace.glob("workflow.step_*.meta")):
            spec = _read_step_meta(meta_path)
            if spec is None or not spec.cacheable:
                continue
            key_hex = spec.cache_key.hex()
            final_dir = _shard_dir(cache_root, key_hex)
            if final_dir.exists():
                # Already in cache (cross-workspace import or prior promote).
                store.touch(spec.cache_key)
                skipped.append(key_hex)
                continue
            lock = _acquire_lock(cache_root, key_hex)
            if lock is None:
                skipped.append(key_hex)
                continue
            try:
                outputs = _find_step_outputs(workspace, spec.order)
                if not outputs:
                    skipped.append(key_hex)
                    continue
                tmp = cache_root / f"{key_hex}.tmp"
                tmp.mkdir(parents=True, exist_ok=True)
                out_dir = tmp / "out"
                out_dir.mkdir(parents=True, exist_ok=True)
                # C0.5: enriched files_meta with (slot_id, dtype_key, branch_idx)
                # per file so cache-hit emission can read paths back without
                # rescanning the filesystem, and so per-file `file_instance_id`
                # mints deterministically via LinPayload.mint_file_id.
                #
                # spec.slot_files holds the compile-time-known declaration:
                # one entry per produced slot with the DataInstance's
                # `dtype.key` (which is what the filename embeds, NOT the
                # Dependency.key), preferred extension, branch_idx, and
                # slot_id. Empty list = legacy step_N.meta; matcher emits
                # nothing matched and `_append_invocation_event_v2` falls
                # back to per-slot degenerate emission.
                #
                # Canonical filename (bootstrap.py:196, virtual_runtime.py:651/665):
                #   "{batch+1}-{i+1}-{branch+1}.{_hash}-{dtype.key}{ext}"
                # We parse branch_idx from the leading prefix and match the
                # filename tail against each declared (dtype_key, ext, branch_idx)
                # triple via endswith — robust to multi-segment extensions.
                files_meta: list[dict] = []
                total_bytes = 0
                unmatched: list[str] = []
                for src in outputs:
                    dest = out_dir / src.name
                    if src.resolve() != dest.resolve():
                        shutil.copy2(src, dest)
                    relpath = str(dest.relative_to(tmp))
                    total_bytes += dest.stat().st_size
                    name = src.name
                    prefix, _dot, rest = name.partition(".")
                    tokens = prefix.split("-")
                    branch_idx = -1
                    if len(tokens) >= 3 and rest:
                        try:
                            branch_idx = int(tokens[2]) - 1
                        except ValueError:
                            branch_idx = -1
                    matched: dict | None = None
                    if branch_idx >= 0:
                        # Prefer longest dtype_key first to avoid prefix
                        # collisions (e.g. "step_a" vs "step_a_legacy").
                        candidates = sorted(
                            spec.slot_files,
                            key=lambda d: len(str(d.get("dtype_key", ""))),
                            reverse=True,
                        )
                        for sf in candidates:
                            if sf.get("branch_idx") != branch_idx:
                                continue
                            dk = sf.get("dtype_key", "")
                            ext = sf.get("ext", "")
                            if not dk:
                                continue
                            if name.endswith(f"-{dk}{ext}"):
                                matched = sf
                                break
                    if matched is not None:
                        files_meta.append({
                            "relpath": relpath,
                            "slot_id": matched.get("slot_id", ""),
                            "dtype_key": matched.get("dtype_key", ""),
                            "branch_idx": matched.get("branch_idx", 0),
                        })
                    else:
                        # G6: still copy to cache so the file isn't lost,
                        # but mark as unmatched so cache-hit emission skips
                        # it (no synthetic ProducedFile with empty slot_id).
                        files_meta.append({
                            "relpath": relpath,
                            "unmatched": True,
                        })
                        unmatched.append(src.name)
                if unmatched and spec.slot_files:
                    # Only warn when we DID have a slot declaration to match
                    # against — legacy step_N.meta files have empty slot_files
                    # and degrade gracefully via _append_invocation_event_v2.
                    log.append((
                        "warn",
                        f"promote {key_hex[:8]}: "
                        f"{len(unmatched)} unmatched output(s): {unmatched}",
                    ))
                # C8 / G6 — capture .command.{sh,out,err,log} into
                # `<shard>/logs/` so get_logs_of resolves after `rm -rf
                # work/` + resume. Best-effort: logs are nice-to-have,
                # never fail the promote on a missing/unreadable .command.*.
                log_srcs = _find_step_logs(workspace, spec.order)
                if log_srcs:
                    logs_dir = tmp / "logs"
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
                    index_payload=[],
                )
                (tmp / "manifest.cbor").write_bytes(manifest_bytes)
                final_dir.parent.mkdir(parents=True, exist_ok=True)
                try:
                    tmp.rename(final_dir)
                except OSError:
                    # Race lost: another writer claimed the final dir.
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
            "orphan_recovery": recover_orphan_tmp_dirs(cache_root),
        }
    finally:
        store.close()
