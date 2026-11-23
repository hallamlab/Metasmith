"""Which deleted products a run can do without.

A run deletes every product no target names once each step reading it has
succeeded (`Orchestrator._maybeDelete`), so its producer's shard keeps the
manifest and loses the bytes: a ghost. A rerun must not remake a ghost whose
every reader is served from the cache, or deletion would cost a recompute on
every run. Whether a reader hits depends on its key, which depends on what
arrives on the channel, so only the run's own data flow can answer it.

The answer therefore takes two passes of the same workflow. The probe pass
(`--msm_probe true`) runs no process: every member's outputs are read off its
shard's manifest, a member with no shard emits one stand-in per output so its
readers still form, and the orchestrator logs each member's key, verdict and
inputs. `plan_skips` then marks a ghost as needed when a member that will run
reads one of its absent files, or when a target names one, to a fixed point.
Every other ghost is skipped in the real pass: its outputs flow as placeholders
that only cache hits read, since a hit's twin never stages its inputs.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from .invocation import consumed_of, read_manifest
from .layout import GHOSTS_MARK, shard_dir

PROBE_LOG = Path("_metasmith") / "cache_probe.jsonl"
SKIP_FILE = Path("_metasmith") / "cache_skip.json"
DELETED_LOG = Path("_metasmith") / "deleted.jsonl"
FOOTPRINT_FILE = "workflow.footprint.json"


def has_ghosts(cache_root: Path) -> bool:
    return (Path(cache_root) / GHOSTS_MARK).exists()


def reset(workspace: Path) -> None:
    for p in (SKIP_FILE, PROBE_LOG):
        (Path(workspace) / p).unlink(missing_ok=True)


def plan_skips(workspace: Path, cache_root: Path) -> dict:
    from ..models.lineage import LinPayload
    from .promote import read_step_meta

    workspace = Path(workspace)
    published = set(json.loads((workspace / FOOTPRINT_FILE).read_text()).get("published", []))
    channels = {
        m.order: m.channels for m in (
            read_step_meta(p) for p in workspace.glob("workflow.step_*.meta")
        ) if m is not None
    }

    reads: list[tuple[str, str, set[str]]] = []
    absent_of: dict[str, set[str]] = {}
    maker: dict[str, str] = {}
    needed: set[str] = set()
    for line in (workspace / PROBE_LOG).read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        key, verdict = str(row["key"]), str(row["verdict"])
        consumed = consumed_of(row.get("entry") or {}, channels.get(int(row["step"]), []))
        reads.append((key, verdict, {i for ids in (consumed or {}).values() for i in ids}))
        if verdict != "ghost" or key in absent_of:
            continue
        absent_of[key] = set()
        shard = shard_dir(Path(cache_root), key)
        for f in (read_manifest(shard) or {}).get("files", []):
            if os.path.lexists(shard / f["relpath"]):
                continue
            fid = LinPayload.mint_file_id(f.get("slot_id", ""), f["relpath"])
            absent_of[key].add(fid)
            maker[fid] = key
            if f.get("slot_id") in published:
                needed.add(key)

    changed = True
    while changed:
        changed = False
        for key, verdict, ids in reads:
            if verdict == "hit" or (verdict == "ghost" and key not in needed):
                continue
            for i in ids:
                g = maker.get(i)
                if g is not None and g not in needed:
                    needed.add(g)
                    changed = True

    skip = sorted(set(absent_of) - needed)
    doc = {
        "skip": skip,
        "absent_ids": sorted(i for k in skip for i in absent_of[k]),
        "rerun": sorted(needed),
    }
    (workspace / SKIP_FILE).parent.mkdir(parents=True, exist_ok=True)
    (workspace / SKIP_FILE).write_text(json.dumps(doc))
    return doc
