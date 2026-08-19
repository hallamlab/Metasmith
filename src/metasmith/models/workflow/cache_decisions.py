from __future__ import annotations

import json
import os

from ...logging import Log
from .grouping import select_for_key
from .nextflow_codegen import NextflowGenContext

def compute_cache_decisions(
    task, context: NextflowGenContext
) -> dict[int, dict]:
    if context.cache_root is None:
        return {}
    if os.environ.get("METASMITH_CACHE", "1").lower() in {
        "0", "false", "off", "no"
    }:
        return {}

    from ...caching.keys import (
        KEY_PREFIX,
        canonical_cbor,
        lineage_key,
        multihash_key,
    )
    from ...caching.store import CacheStore

    store = None
    if context.cache_root.exists():
        try:
            store = CacheStore.open(context.cache_root)
        except Exception:
            store = None

    decisions: dict[int, dict] = {}
    out_id_by_producer: dict[tuple[int, str, int], str] = {}
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

        for dep in step.transform.model.requires:
            for inst in step.dependency_map.get(dep, []):
                slot_id = slot_id_by_archetype.get(inst.instance_id)
                if slot_id is None:
                    continue
                inst.instance_id = slot_id
                inst.origin = "lineage"
                inst._refresh_derived_keys()
        step.RefreshViews()

        sorted_inputs: list[tuple[str, list[str]]] = []
        for dep in step.transform.model.requires:
            insts = step.dependency_map.get(dep, [])
            if not insts:
                continue
            slot_ids = sorted(i.instance_id for i in insts)
            sorted_inputs.append((dep.key, slot_ids))
        sorted_inputs.sort(key=lambda kv: kv[0])

        cache_key = lineage_key(
            transform_key,
            signature,
            [(k, "+".join(ids).encode()) for k, ids in sorted_inputs],
        )

        out_slot_ids: dict[tuple[str, int], str] = {}
        for branch_idx, dep_group in enumerate(step.transform.model.produces):
            for dep in dep_group:
                slot_id_bytes = multihash_key(
                    canonical_cbor(
                        {"ck": cache_key, "s": dep.key, "b": branch_idx}
                    )
                )
                slot_id = slot_id_bytes.hex()
                out_slot_ids[(dep.key, branch_idx)] = slot_id
                out_id_by_producer[(step.order, dep.key, branch_idx)] = slot_id
                for inst in step.dependency_map.get(dep, []):
                    slot_id_by_archetype[inst.instance_id] = slot_id
                    inst.instance_id = slot_id
                    inst.origin = "lineage"
                    inst._refresh_derived_keys()
        step.RefreshViews()

        entry = None
        hit = False
        out_indexes: dict[str, dict] = {}
        if store is not None:
            entry = store.probe(cache_key)
            if entry is not None and store.files_exist(entry):
                hit = True

        if hit:
            from ...caching.store import decode_manifest

            manifest: dict = {}
            if getattr(entry, "payload", None):
                try:
                    manifest = decode_manifest(entry.payload)
                except Exception as e:
                    Log.Warn(
                        f"cache-hit decode_manifest failed for "
                        f"{cache_key.hex()[:8]}: {e}"
                    )
            out_indexes = {
                name: index
                for name, index in (
                    (
                        str(row.get("relpath", "")).rsplit("/", 1)[-1],
                        dict(row.get("index") or {}),
                    )
                    for row in (manifest.get("index") or [])
                    if row.get("relpath")
                )
                if index
            }
            need = {
                str(f.get("relpath", "")).rsplit("/", 1)[-1]
                for f in (manifest.get("files") or [])
                if not f.get("unmatched") and f.get("relpath")
            }
            missing = need - set(out_indexes)
            if not need or missing:
                Log.Warn(
                    f"cache shard {cache_key.hex()[:8]} carries no on-channel "
                    f"index for {len(missing) or 'any'} output(s); demoting "
                    "the hit so the step re-runs rather than emitting a tuple "
                    "the orchestrator would drop"
                )
                hit = False
                out_indexes = {}

        group_total = max(1, len(step.group_by_instances))
        batch_size = max(1, int(getattr(step.transform, "batch_size", 1) or 1))
        key_insts = list(step.group_by_instances)
        batches: list[dict] = []
        for batch_idx, start in enumerate(range(0, group_total, batch_size)):
            end = min(group_total, start + batch_size)
            batch_sorted: list[tuple[str, list[str]]] = []
            for dep in step.transform.model.requires:
                dep_insts = list(step.dependency_map.get(dep, []))
                if not dep_insts:
                    continue
                selected: list = []
                seen_ids: set[int] = set()
                for key_idx in range(start, end):
                    key_inst = (
                        key_insts[key_idx] if key_idx < len(key_insts) else None
                    )
                    for inst in select_for_key(dep_insts, key_inst, key_idx):
                        if id(inst) in seen_ids:
                            continue
                        seen_ids.add(id(inst))
                        selected.append(inst)
                slot_ids = sorted(i.instance_id for i in selected)
                batch_sorted.append((dep.key, slot_ids))
            batch_sorted.sort(key=lambda kv: kv[0])
            batches.append({
                "batch_idx": batch_idx,
                "start": start,
                "end": end,
                "sorted_inputs": batch_sorted,
            })

        decisions[step.order] = {
            "cache_key": cache_key,
            "transform_key": transform_key,
            "signature": signature,
            "sorted_inputs": sorted_inputs,
            "out_instance_ids": out_slot_ids,
            "hit": hit,
            "entry": entry,
            "out_indexes": out_indexes,
            "cacheable": getattr(step.transform, "cacheable", True),
            "batches": batches,
        }

    from ..lineage import (
        INVOCATION_EVENT_SCHEMA_VERSION,
        InvocationEvent,
        LinPayload,
        ProducedFile,
        SessionStart,
        append_invocation_event,
    )
    from ...constants import VERSION

    trace_dir = context.work_dir / "_metasmith"
    trace_dir.mkdir(parents=True, exist_ok=True)
    trace_path = trace_dir / "trace.jsonl"

    prev_session_id = 0
    if trace_path.exists():
        try:
            first_line = next(
                (l for l in trace_path.read_text().splitlines() if l.strip()),
                "",
            )
            if first_line:
                head = json.loads(first_line)
                if head.get("event") == SessionStart.EVENT_NAME:
                    prev_session_id = int(head.get("session_id", 0))
        except Exception:
            prev_session_id = 0
        rotated = trace_dir / f"trace.{prev_session_id}.jsonl"
        try:
            trace_path.rename(rotated)
        except OSError:
            pass

    if store is not None:
        session_id = store.allocate_session_id()
    else:
        session_id = prev_session_id + 1

    sentinel = SessionStart(
        session_id=session_id,
        compile_started_at="",
        metasmith_version=VERSION,
        schema_version=INVOCATION_EVENT_SCHEMA_VERSION,
    )
    with open(trace_path, "w", encoding="utf-8") as f:
        f.write(sentinel.to_jsonl() + "\n")

    for order, decision in sorted(decisions.items()):
        if not decision["hit"]:
            continue
        step_name = ""
        for step in task.plan.steps:
            if step.order == order:
                step_name = step.transform.name or ""
                break
        from ...caching.store import decode_manifest
        entry = decision["entry"]
        files: list[dict] = []
        if entry is not None and getattr(entry, "payload", None):
            try:
                manifest = decode_manifest(entry.payload)
                files = manifest.get("files", []) or []
            except Exception as e:
                Log.Warn(
                    f"cache-hit decode_manifest failed for "
                    f"{decision['cache_key'].hex()[:8]}: {e}"
                )
        matched_files = [f for f in files if not f.get("unmatched")]
        legacy = (not matched_files) or any(
            "slot_id" not in f for f in matched_files
        )
        produces: list[ProducedFile] = []
        if legacy:
            if entry is not None:
                Log.Warn(
                    f"cache-hit: legacy manifest for "
                    f"{decision['cache_key'].hex()[:8]}; emitting "
                    "per-slot degenerate ProducedFile rows"
                )
            for (slot_key, branch_idx), slot_id in decision[
                "out_instance_ids"
            ].items():
                produces.append(
                    ProducedFile(
                        file_instance_id=slot_id,
                        slot_id=slot_id,
                        path="",
                        dtype_key=slot_key,
                    )
                )
        else:
            for f in sorted(files, key=lambda d: d.get("relpath", "")):
                if f.get("unmatched"):
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
                        parents=list(f.get("parents") or []),
                    )
                )
        consumes = {
            slot_key: list(ids)
            for slot_key, ids in decision["sorted_inputs"]
        }
        event = InvocationEvent(
            task_hash=decision["cache_key"].hex(),
            transform_key=decision["transform_key"],
            status="hit",
            consumes=consumes,
            produces=produces,
            session_id=session_id,
            step_order=order,
            step_name=step_name,
            cache_key=decision["cache_key"].hex(),
        )
        append_invocation_event(trace_path, event)

    if store is not None:
        store.close()
    hits = sum(1 for d in decisions.values() if d["hit"])
    if hits:
        Log.Info(
            f"cache probe matched {hits}/{len(decisions)} step(s); "
            f"will short-circuit via synthetic Channel.of(...) emission"
        )
    return decisions
