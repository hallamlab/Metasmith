"""Stamp structural identities onto the plan and open the run's trace.

The cache unit is one group member's invocation, and its key is minted on the
channel once the member's inputs are known (`caching.invocation`, called by
`Orchestrator._route`). Nothing about a hit is decided here. What compile time
owns is identity: every produced slot gets a structural id that the consumer's
`din` and the producer's `slot_files` both name, so a product's file id --
`mint_file_id(slot_id, name)` -- means the same thing in every run.
"""

from __future__ import annotations

import json
import os

from ...logging import Log
from .nextflow_codegen import CACHE_HITS_LOG, NextflowGenContext


def cache_enabled() -> bool:
    return os.environ.get("METASMITH_CACHE", "1").lower() not in {
        "0", "false", "off", "no"
    }


def compute_cache_decisions(task, context: NextflowGenContext) -> dict[int, dict]:
    from ...caching.invocation import structural_slot_id

    enabled = context.cache_root is not None and cache_enabled()
    decisions: dict[int, dict] = {}
    # A step's produce instances and the downstream step's require instances are
    # distinct objects that both start life carrying the transform archetype's
    # id. Stamping only the producer leaves the consumer naming the archetype, so
    # `produces` and `consumes` land in different identity spaces and nothing can
    # join a result to the step that made it.
    slot_id_by_archetype: dict[str, str] = {}

    for step in task.plan.steps:
        transform_key = step.transform.GetKey() or step.transform.name or ""
        protocol_sig = getattr(step.transform, "_protocol_source_hash", "") or ""
        signature = f"{step.transform._hash}:{protocol_sig}"

        upstream: list[str] = []
        for dep in step.transform.model.requires:
            for inst in step.dependency_map.get(dep, []):
                slot_id = slot_id_by_archetype.get(inst.instance_id)
                if slot_id is None:
                    # A given leaf. Its id names a sample, and a slot id that
                    # folded it would move with the sample set; the dtype is
                    # what the structure knows about it.
                    upstream.append(f"given:{inst.dtype.key}")
                    continue
                upstream.append(slot_id)
                inst.instance_id = slot_id
                inst.origin = "lineage"
                inst._refresh_derived_keys()
        step.RefreshViews()

        out_slot_ids: dict[tuple[str, int], str] = {}
        for branch_idx, dep_group in enumerate(step.transform.model.produces):
            for dep in dep_group:
                slot_id = structural_slot_id(
                    transform_key, signature, dep.key, branch_idx, upstream
                )
                out_slot_ids[(dep.key, branch_idx)] = slot_id
                for inst in step.dependency_map.get(dep, []):
                    slot_id_by_archetype[inst.instance_id] = slot_id
                    inst.instance_id = slot_id
                    inst.origin = "lineage"
                    inst._refresh_derived_keys()
        step.RefreshViews()

        decisions[step.order] = {
            "transform_key": transform_key,
            "signature": signature,
            "out_instance_ids": out_slot_ids,
            "cacheable": enabled and bool(getattr(step.transform, "cacheable", True)),
            "step_name": step.transform.name or "",
        }

    session_id = _open_trace(context)
    for d in decisions.values():
        d["session"] = session_id
    return decisions


def _open_trace(context: NextflowGenContext) -> int:
    from ..lineage import INVOCATION_EVENT_SCHEMA_VERSION, SessionStart
    from ...constants import VERSION

    trace_dir = context.work_dir / "_metasmith"
    trace_dir.mkdir(parents=True, exist_ok=True)
    trace_path = trace_dir / "trace.jsonl"

    prev_session_id = 0
    if trace_path.exists():
        try:
            first_line = next(
                (l for l in trace_path.read_text().splitlines() if l.strip()), "",
            )
            if first_line:
                head = json.loads(first_line)
                if head.get("event") == SessionStart.EVENT_NAME:
                    prev_session_id = int(head.get("session_id", 0))
        except Exception:
            prev_session_id = 0
        try:
            trace_path.rename(trace_dir / f"trace.{prev_session_id}.jsonl")
        except OSError as e:
            Log.Warn(f"could not rotate the previous trace: {e}")

    hits_log = context.work_dir / CACHE_HITS_LOG
    if hits_log.exists():
        try:
            hits_log.rename(hits_log.with_name(f"cache_hits.{prev_session_id}.jsonl"))
        except OSError as e:
            Log.Warn(f"could not rotate the previous hit log: {e}")

    sentinel = SessionStart(
        session_id=prev_session_id + 1,
        compile_started_at="",
        metasmith_version=VERSION,
        schema_version=INVOCATION_EVENT_SCHEMA_VERSION,
    )
    with open(trace_path, "w", encoding="utf-8") as f:
        f.write(sentinel.to_jsonl() + "\n")
    return prev_session_id + 1
