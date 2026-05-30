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
from dataclasses import dataclass
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
    signature = ""
    dep_out: list[dict] = []

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
    if cache_key_hex is None:
        return None
    return StepPromoteSpec(
        order=order,
        cache_key=bytes.fromhex(cache_key_hex),
        cacheable=cacheable,
        transform_key=transform_key,
        signature=signature,
        out_identities=out_identities,
        dep_out=dep_out,
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
                files_meta: list[dict] = []
                total_bytes = 0
                for src in outputs:
                    dest = out_dir / src.name
                    if src.resolve() != dest.resolve():
                        shutil.copy2(src, dest)
                    files_meta.append({"relpath": str(dest.relative_to(tmp))})
                    total_bytes += dest.stat().st_size
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
            finally:
                _release_lock(lock)
        return {
            "promoted": promoted,
            "skipped": skipped,
            "orphan_recovery": recover_orphan_tmp_dirs(cache_root),
        }
    finally:
        store.close()
