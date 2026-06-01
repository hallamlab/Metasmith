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


def _parse_batch_idx_from_relpath(relpath: str) -> int:
    """Extract `batch_idx` (0-indexed) from a canonical output filename.

    Filename shape (bootstrap.py:196, virtual_runtime.py:651/665):
      `{batch+1}-{i+1}-{branch+1}.{_hash}-{dtype.key}{ext}`
    The file may live under `out/` so we strip path components first.
    Returns -1 when the filename doesn't match (e.g. host log files).
    """
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
    """Append v2 InvocationEvent row(s) to <workspace>/_metasmith/trace.jsonl.

    S4b: emit ONE event per batch (= per task) instead of one per step.
    Filename `batch_idx` parsed from each matched file's relpath; events
    grouped by batch_idx. For multi-batch steps, task_hash is suffixed
    with `:{batch_idx}` so events are distinct in TraceIndex.by_task_hash.

    Per-batch consumes is read from `spec.batches` when available with
    > 1 entries (compile-time per-batch decomp from S3, accurate for
    step 1 where dependency_map carries the full N-sample arity). For
    intermediate steps (compile-time archetype arity = 1, runtime arity
    = N), `spec.batches` has a single entry; we fall back to the
    aggregate `spec.sorted_inputs` for all runtime batches. That's
    still step-aggregated for intermediate steps — full per-batch
    consumes on intermediates requires runtime parent capture (a
    follow-up for parallel_then_group / group_by topologies).

    C0.5 / S4a: per-file emission with mint_file_id(slot_id, relpath)
    + populated path / dtype_key remains. Legacy fallback (no
    files_meta) emits a single per-slot degenerate event with
    `path=""`.
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

    # Group matched files by parsed batch_idx; unmatched files are
    # carried under batch_idx=-1 and dropped from emission below.
    batched: dict[int, list[dict]] = {}
    have_per_file = files_meta is not None and any(
        f.get("slot_id") and not f.get("unmatched")
        for f in (files_meta or [])
    )
    if have_per_file:
        for f in (files_meta or []):
            if f.get("unmatched") or not f.get("slot_id"):
                continue
            # S4b: prefer the explicit `batch_idx` field set by
            # promote_run from the source `batch_XXXX_YYYY` parent dir.
            # Fall back to filename prefix parse for legacy callers.
            bi = f.get("batch_idx", None)
            if bi is None or bi < 0:
                bi = _parse_batch_idx_from_relpath(f.get("relpath", ""))
            batched.setdefault(int(bi), []).append(f)

    # Per-batch consumes: spec.batches[i].sorted_inputs when len > 1,
    # else aggregate. `consumes_for_batch` is keyed by batch_idx.
    # S5: when files arrived flat in cache_tmp (real Nextflow
    # publishDir, batch_idx synthesized per-file by promote_run), the
    # compile-time spec.batches doesn't reflect runtime fan-out and
    # neither aggregate nor per-batch consumes maps cleanly. Emit
    # consumes={} so the trace events register output files (for I8)
    # without injecting cross-sample parent edges that would contaminate
    # the BFS in agents.py. Pre-S6, `_manifests/*.json` remains the
    # authoritative parent source for these cases; S6 will fold full
    # per-file consumes capture from runtime Nextflow .command.in.
    is_flat_cache_tmp = (
        have_per_file
        and len(batched) > 1
        and len(spec.batches) <= 1
        and all(
            not (workspace / "nxf_work" / f"step_{spec.order:02}").exists()
            for _ in [None]
        )
    )
    aggregate_consumes = (
        {}
        if is_flat_cache_tmp
        else {slot_key: list(ids) for slot_key, ids in spec.sorted_inputs}
    )
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
                    )
                )
            if produces:
                _emit(batch_idx, produces)
    else:
        # Legacy fallback (miss-without-promote, or pre-C0.5 caller).
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
                # S5: when real-Nextflow publishDir has already staged
                # outputs at `<cache_root>/<key>.tmp/<file>` (files-at-root,
                # see workflow.py:1665-1683), pick those up directly. The
                # virtual_runtime path keeps using `_find_step_outputs`
                # (nxf_work/step_NN/batch_*/file layout). cache_tmp can
                # also have a pre-existing `out/` from a re-run we should
                # not double-process.
                tmp = cache_root / f"{key_hex}.tmp"
                outputs = _find_step_outputs(workspace, spec.order)
                if not outputs and tmp.exists():
                    # files-at-root in cache_tmp (real Nextflow publishDir)
                    skip_names = {"out", "logs", "manifest.cbor"}
                    outputs = [
                        p for p in tmp.iterdir()
                        if p.is_file()
                        and p.name not in skip_names
                        and not p.name.startswith(".command")
                    ]
                if not outputs:
                    skipped.append(key_hex)
                    continue
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
                # S5: files-at-root in cache_tmp (real-Nextflow publishDir
                # path) loses the per-batch parent-dir signal. Each file
                # there came from a separate Nextflow task invocation,
                # so assign a sequential batch_idx per (slot_id,
                # filename-order) so per-batch event emission produces
                # one event per file.
                files_have_batch_dir = any(
                    p.parent.name.startswith("batch_") for p in outputs
                )
                fallback_batch_counter = 0
                for src in outputs:
                    # S4b: resolve batch_idx from `batch_XXXX_YYYY` parent
                    # directory. For files-at-root in cache_tmp,
                    # synthesize a sequential batch_idx per file so
                    # downstream per-batch event emission distinguishes
                    # them.
                    parent = src.parent
                    batch_dir_idx = -1
                    if parent.name.startswith("batch_"):
                        try:
                            batch_dir_idx = int(parent.name.split("_")[1])
                        except (IndexError, ValueError):
                            batch_dir_idx = -1
                    elif not files_have_batch_dir:
                        batch_dir_idx = fallback_batch_counter
                        fallback_batch_counter += 1
                    dest = out_dir / src.name
                    # S5: when src is inside cache_tmp (real-Nextflow
                    # publishDir already staged here), move instead of
                    # copy to avoid duplicating files.
                    try:
                        src_in_tmp = src.parent.resolve() == tmp.resolve()
                    except (OSError, RuntimeError):
                        src_in_tmp = False
                    if src.resolve() != dest.resolve():
                        if src_in_tmp:
                            shutil.move(str(src), str(dest))
                        else:
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
                            # S4b: persist resolved batch_idx for per-batch
                            # event emission. -1 when src wasn't under
                            # batch_XXXX_YYYY (treated as single-batch).
                            "batch_idx": batch_dir_idx,
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
