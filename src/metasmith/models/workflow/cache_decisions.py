"""Deciding, before anything runs, which steps are already done.

A step's `cache_key` is the transform key plus its sorted input `instance_id`s,
canonical-CBOR encoded and blake3-32 multihashed. Nothing about the output
participates -- cache identity is provenance, not bytes.

This runs at compile time and its output rewrites codegen: a hit turns that
step's emission into a synthetic channel instead of a process. That is why it
must compute output instance ids for hit steps too, since the *next* step's key
is a function of them -- a chain of hits has to keep resolving.

Two version constants stay deliberately separate. `CACHE_KEY_VERSION` is the
cache-key epoch; `LIN_PAYLOAD_VERSION` is the on-wire envelope the Groovy side
parses. They were one constant once, and bumping it for a key-epoch reason
desynced the emitter and failed every containerized step with a masked exit 1
that no fast test could see.
"""

from __future__ import annotations

import json
import os

from ...logging import Log
from .grouping import select_for_key
from .nextflow_codegen import NextflowGenContext

def compute_cache_decisions(
    task, context: NextflowGenContext
) -> dict[int, dict]:
    """Compute per-step cache keys + probe results.

    Returns a dict[step.order, {cache_key, hit, entry?, transform_key,
    signature, sorted_input_ids, out_instance_ids}]. The OUTPUT
    instance_ids are needed by downstream steps as their input
    identities, so the walk runs in topological (step.order) order.

    Cache integration is skipped (returns {} effectively) when:
    - context.cache_root is None
    - env METASMITH_CACHE is set to "0" / "false" / "off"
    """
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

    # Cache STORE is optional — probe-only flow doesn't require the
    # SQLite db to exist. Only open if the cache_root already exists
    # on disk (this matches "fresh workspace -> nothing to probe"
    # and avoids materializing an empty task_cache/ dir during the
    # first ever run).
    store = None
    if context.cache_root.exists():
        try:
            store = CacheStore.open(context.cache_root)
        except Exception:
            store = None

    decisions: dict[int, dict] = {}
    # Per-(step.order, slot_key, branch_idx) → output instance_id (hex).
    # Downstream steps use this to look up their inputs' ids when the
    # input came from an upstream step's output (not a given leaf).
    out_id_by_producer: dict[tuple[int, str, int], str] = {}

    # S4a (Bug A fix): use inst.instance_id directly. The old code
    # called `inst.instance_id.encode("utf-8").hex()` which produces
    # hex-of-ASCII-hex (double-encoded). The audit
    # (plans/lineage-quadrant-audit.md, I1) confirmed this leaked
    # into InvocationEvent.consumes as e.g.
    # "3165323035396234..." (decodes to the actual hex
    # "1e2059b4..."). The fix is to just keep the hex string. Cache
    # sharding changes — existing dev caches need rebuild — but the
    # consumes dict now carries plain 32-byte hex slot_ids that
    # TraceIndex.by_slot can actually look up.

    for step in task.plan.steps:
        transform_key = step.transform.GetKey() or step.transform.name or ""
        # R5 (F1 fix): the lineage signature captures BOTH the I/O type
        # topology (_hash = model.hash) AND the transform's protocol-body
        # identity (_protocol_source_hash = digest of the definition-file
        # bytes). Topology alone let a protocol edit — or a different tool
        # with the same in/out types — false-hit the cache with stale
        # output. Folding the body digest in makes such an edit bust the
        # cache. Computed once here; promote reads the resulting cache_key
        # back from workflow.step_N.meta, so probe/promote stay symmetric.
        protocol_sig = getattr(step.transform, "_protocol_source_hash", "") or ""
        signature = f"{step.transform._hash}:{protocol_sig}"

        sorted_inputs: list[tuple[str, list[str]]] = []
        for dep in step.transform.model.requires:
            insts = step.dependency_map.get(dep, [])
            if not insts:
                continue
            # Aggregate every instance feeding this slot. Sort the
            # ids to remove ordering noise from the input set.
            # S4a (Bug A): store inst.instance_id directly (hex string).
            slot_ids = sorted(i.instance_id for i in insts)
            sorted_inputs.append((dep.key, slot_ids))
        sorted_inputs.sort(key=lambda kv: kv[0])

        cache_key = lineage_key(
            transform_key,
            signature,
            [(k, "+".join(ids).encode()) for k, ids in sorted_inputs],
        )

        # Compute per-output slot_ids (cache_key + dep.key + branch_idx).
        # G1 (C4): these `derived_hex` values ARE the slot_ids — the
        # production-channel identity for the (transform, slot, branch)
        # triple. They're stored on each produced DataInstance's
        # `instance_id` field so downstream steps see slot-identity on
        # their input sides. File-level identity (file_instance_id) is
        # minted post-facto by CollectResults (C6) over (slot_id, path)
        # and never travels on the Nextflow channel.
        #
        # dependency_map shares the same DataInstance object reference
        # between the producing step's produced dep and the consuming
        # step's required dep (via canonical get_or_create in
        # WorkflowPlan.Generate), so a single mutation propagates. We
        # run topologically, so each consumer iteration above sees ids
        # already rewritten.
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

        # A hit replays the shard's files onto the channel without running
        # the step, so it has to replay the on-channel lineage index they
        # travelled with (captured at promote time, manifest `index`).
        # Without it the synthetic tuple carries no ancestry and a downstream
        # `o.group` keyed on an ancestor drops it — the warm run loses what
        # the cold run computes. A shard that cannot supply an index for
        # every matched file is DEMOTED to a miss: re-running is slower, a
        # silent drop is wrong.
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
                str(row.get("relpath", "")).rsplit("/", 1)[-1]: dict(
                    row.get("index", {})
                )
                for row in (manifest.get("index") or [])
                if row.get("relpath")
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

        # S3: per-batch decomposition. The compile-time `sorted_inputs`
        # above is the *aggregate* (step-level) view used for cache_key
        # byte-identity; `batches` is emission-only metadata naming each
        # task's specific inputs. Cache sharding stays one-shard-per-step.
        #
        # `batch_size` folds whole group_by KEYS into one task, so a batch is
        # the union of its keys' slices — and which instances belong to a key
        # is a lineage question (`grouping.select_for_key`), the same one the
        # Nextflow runtime and virtual_runtime now answer. It used to be a
        # positional `insts[start:end]` here, which lines up only for a
        # dependency that fans out ALONGSIDE the key; for one that COLLECTS
        # into it, N instances descend from a single key and the slice named
        # one of them.
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
                # S4a (Bug A): use inst.instance_id directly.
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
            # basename -> the on-channel index that file rode in on; empty
            # unless `hit`.
            "out_indexes": out_indexes,
            "cacheable": getattr(step.transform, "cacheable", True),
            "batches": batches,
        }

    # C7 — emit the per-run trace.jsonl at compile time as v2
    # InvocationEvent rows. On each compile: if a prior trace.jsonl
    # exists, rotate it to `trace.<prev_session_id>.jsonl` (the
    # session_id read from its SessionStart sentinel, or 0 fallback);
    # then allocate a fresh session_id via the cache sqlite counter
    # and open a clean file headed by a SessionStart sentinel. All
    # subsequent emits in this compile carry the new session_id.
    # Post-exec promote (promote.py) appends miss/promoted/fail rows
    # carrying the same session_id, rediscovered from the sentinel.
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
            # Falling back to truncate-overwrite is non-fatal: the
            # archived rows are lost but the fresh session proceeds.
            pass

    if store is not None:
        session_id = store.allocate_session_id()
    else:
        session_id = prev_session_id + 1

    sentinel = SessionStart(
        session_id=session_id,
        compile_started_at="",  # Date.now() omitted — set at writer
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
        # C0.5: read per-file (slot_id, dtype_key, relpath) from the
        # cache entry's manifest.cbor and emit one ProducedFile per
        # file. Pre-C0.5 manifests carry only {"relpath"} per file —
        # detected by missing "slot_id" — and we fall back to the
        # legacy per-slot degenerate emission with a Log.Warn. The
        # legacy path also covers the (defensive) empty-files case.
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
        # S4a (Bug B/C/D fix): ignore `unmatched` files when deciding
        # legacy fallback. Unmatched files (e.g. virt-host.log,
        # virt-host.stop) are copied into the cache shard without
        # slot_id annotation; their presence shouldn't force the
        # whole event into the legacy degenerate emission (which
        # collapses slot_id == file_instance_id, drops path, and
        # uses the consumer's dep_key as dtype_key).
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
                    )
                )
        # C0-amend: decision["sorted_inputs"] is now
        # list[tuple[str, list[str]]] — the slot_ids are already
        # hex strings, no byte-encoding gymnastics. This is the
        # shape `walk_ancestors` expects to look up `by_slot`.
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
