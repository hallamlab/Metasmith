"""Reassembling a finished run's outputs, and the lineage that explains them.

`CollectResults` walks the lin envelopes every task echoed onto the Nextflow
channel and rebuilds the produced-file graph: which instance came from which
transform application, over which inputs. The output is the result library plus
the manifests a later run reads to decide what it can skip.

The delicate part is `_build_transitive_lind`, which takes a per-file root
rather than a per-event one. Sibling files from a multi-slot transform are
co-emitted, not each other's ancestors; including them makes the walk wait on
itself.

Its own module rather than part of `runner` because it is the one agent-side
entry point that shares no name with an `Agent` method, and at 400 lines it is
the single largest thing the agent does.
"""

from __future__ import annotations

import json
import re
from dataclasses import field
from hashlib import md5
from pathlib import Path

import pandas as pd

from ..logging import Log
from ..models.libraries import DataInstance, DataInstanceLibrary, DataTypeLibrary
from ..models.workflow import WorkflowTask

def CollectResults(
    task: WorkflowTask,
    output_path: Path,
    inputs_dir: Path,
) -> DataInstanceLibrary:
    """Compile Nextflow outputs into a DataInstanceLibrary with lineage.

    Walks `<workspace>/_metasmith/trace.jsonl` for per-batch
    InvocationEvent rows (produced by promote_run / cache-hit emission)
    and reconstructs parent-child relationships from `consumes` /
    `produces`. The legacy `_manifests/*.json` sidecar route is gone
    as of S6.

    Args:
        task: The workflow task that was executed.
        output_path: Path to the results directory (where outputs live).
        inputs_dir: Path to the inputs/ directory with input CSVs.

    Returns:
        DataInstanceLibrary with all outputs and their lineage.
    """
    output = DataInstanceLibrary(output_path)
    tlibs: dict[str, DataTypeLibrary] = {}
    path2inst: dict[Path, DataInstance] = {}
    for lib in task.transform_libraries:
        for namespace, tlib in lib.types.items():
            tlibs[namespace] = tlib
    for lib in task.data_libraries:
        for namespace, tlib in lib.types.items():
            if namespace in tlibs:
                _lib = tlibs[namespace]
                for k, e in tlib.types.items():
                    if k in _lib: continue
                    _lib[k] = e
            else:
                _lib = tlib
            tlibs[namespace] = _lib
        for path, name, model in lib.Iterate():
            inst = lib.Get(path)
            path2inst[inst.ResolvePath()] = inst
    for namespace, tlib in tlibs.items():
        output.AddTypeLibrary(namespace=namespace, lib=tlib)
    inst_id2inst: dict[str, DataInstance] = {}
    # path2iid replaces the old input_ids/ sidecar: the given DataInstances are
    # themselves the record of <path> -> <instance_id> (the input CSVs write
    # x.ResolvePath(); this maps the same path back to x.instance_id). Sourcing
    # it from task.plan.given (+ step deps) removes the sidecar's separate copy.
    path2iid: dict[str, str] = {}
    for inst in task.plan.given:
        inst_id2inst[inst.instance_id] = inst
        path2iid[str(inst.ResolvePath())] = inst.instance_id
    for step in task.plan.steps:
        for insts in step.dependency_map.values():
            for inst in insts:
                inst_id2inst[inst.instance_id] = inst
                path2iid[str(inst.ResolvePath())] = inst.instance_id

    def _resolve_instance(dtype_key: str, instance_id: str | None = None):
        """Direct lookup by instance_id (G2). No dtype_key fallback.

        The input-CSV writer at workflow.py emits `<path>\\t<instance_id>`
        rows; manifest filenames carry slot_id in the `inst_id` field.
        Both routes populate `instance_id` end-to-end, so the legacy
        collision fallback (multiple candidates → deterministic first)
        is no longer needed.
        """
        if instance_id is None:
            raise KeyError(
                f"_resolve_instance called without instance_id for [{dtype_key}]; "
                f"input CSV writer should always emit <path>\\t<instance_id>"
            )
        try:
            return inst_id2inst[instance_id]
        except KeyError:
            raise KeyError(
                f"missing DataInstance for instance_id [{instance_id}] (dtype={dtype_key})"
            )
    # Mapping of (dtype_key, hash15(abs_path)) → (path, lineage, slot/csv id, file_instance_id).
    # The fourth element is the trace's ProducedFile.file_instance_id (G3); it's
    # None for input entries (which only carry the csv-routed slot identity) and
    # is the bridge between the trace's per-file identity and the published-results
    # manifest's instance_id. Consumed by SetLineageInstance(...) after AddItem
    # below so `_resolve_instance_meta` on reload no longer falls back to the
    # legacy path+dtype derivation for promoted outputs.
    kv2path: dict[tuple[str, int], tuple[Path, dict, str|None, str|None]] = {}
    # The input CSVs are path-only (Nextflow's Channel.splitCsv consumes them);
    # the instance_id that routes each path to its DataInstance comes from
    # path2iid (built above from the given record), not a separate sidecar file.
    for in_manifest in inputs_dir.iterdir():
        k = in_manifest.name
        with open(in_manifest) as f:
            for l in f:
                p = Path(l[:-1])
                _hash = md5(str(p).encode()).hexdigest()
                _hash = int(_hash[:15], 16) # 15 is important as it allows us to disregard the sign of a long and match with java
                kv2path[(k, _hash)] = p, {}, path2iid.get(str(p)), None
    # C1: BFS over `_metasmith/trace.jsonl` populates the output side of
    # `kv2path` directly from `InvocationEvent.consumes`, replacing the
    # legacy `_manifests/*.json` glob. The trace is authoritative post
    # C0/C0.5: every promote/hit event carries `consumes` (slot-keyed
    # parent ids) and per-file `ProducedFile.path` + `file_instance_id`.
    # The legacy `_manifests/` publishDir route is fully removed (S6);
    # the trace is the sole reconstruction source.
    from ..telemetry import TraceIndex
    trace_idx = TraceIndex.read(output_path.parent / "_metasmith" / "trace.jsonl")
    # trace.jsonl carries slot_ids (assigned by _compute_cache_decisions
    # at PrepareNextflow time); the on-disk task loaded above still
    # holds pre-refresh short instance_ids because PrepareNextflow is
    # not rerun in RunWorkflow. Bridge the two so `_resolve_instance`
    # can map a slot_id back to a DataInstance via inst_id2inst — both
    # ids end up pointing at the same in-memory DataInstance whose
    # dtype_name we ultimately need at `output.AddItem`.
    for _ev in trace_idx.events:
        if not _ev.step_order:
            continue
        _si = _ev.step_order - 1
        if not (0 <= _si < len(task.plan.steps)):
            continue
        _step = task.plan.steps[_si]
        _dtype_to_insts: dict[str, list[DataInstance]] = {}
        for _dep_group in _step.transform.model.produces:
            for _dep in _dep_group:
                for _inst in _step.dependency_map.get(_dep, []):
                    _dtype_to_insts.setdefault(_inst.dtype.key, []).append(_inst)
        for _pf in _ev.produces:
            _cands = _dtype_to_insts.get(_pf.dtype_key, [])
            if _cands:
                if _pf.slot_id:
                    inst_id2inst.setdefault(_pf.slot_id, _cands[0])
                if _pf.file_instance_id:
                    inst_id2inst.setdefault(_pf.file_instance_id, _cands[0])
    # consumes values are `hex(utf8(instance_id))` (workflow.py:1287),
    # while TraceIndex.by_slot / by_file key on the raw instance_id —
    # decode once to bridge. Empty for fixtures with no upstream chain
    # (every input is a leaf given), and `_try_decode` falls back to
    # the raw form for trace shapes that ever emit it directly.
    def _try_decode(piid: str) -> str | None:
        try:
            return bytes.fromhex(piid).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            return None
    # Map every produced slot_id / file_instance_id to ALL events that
    # produced it. A slot_id is shared across every batch of a step (the
    # intermediate bam of a 3-sample diamond has one slot_id but three
    # producer events, one per sample), so a single-winner map collapses
    # multi-batch producers to the first event — the root of Bug L/I11: a
    # downstream task that references the bam by slot_id would always walk
    # back into sample 0's alignment. Keep the full producer list and
    # disambiguate per walk-branch by shared given-ancestry (below).
    _slot_to_events: dict[str, list] = {}
    def _add_producer(key, ev):
        if not key:
            return
        lst = _slot_to_events.setdefault(key, [])
        if ev not in lst:
            lst.append(ev)
    for _ev in trace_idx.events:
        for _pf in _ev.produces:
            _add_producer(_pf.slot_id, _ev)
            _add_producer(_pf.file_instance_id, _ev)

    def _event_given_ids(ev) -> set[str]:
        """Given-leaf instance_ids directly referenced by `ev.consumes`.

        A "given" here is a consumes id that resolves to a DataInstance
        but is produced by no event (a true leaf) — that set is the
        event's *sample identity*. The diamond's alignment:i and
        binner:i both reference assembly_i directly, so intersecting
        these sets tells us which bam producer feeds which binner
        without any runtime capture. Bridged slot/file ids (added to
        inst_id2inst above) are produced, so they're excluded.
        """
        out: set[str] = set()
        for _vals in ev.consumes.values():
            for _piid in _vals:
                for _key in (_try_decode(_piid), _piid):
                    if _key and _key in inst_id2inst and _key not in _slot_to_events:
                        out.add(_key)
                        break
        return out

    def _producers_for(piid, root_givens):
        """Producer events for a consumes id, disambiguated by sample.

        When a slot_id names multiple producers (one per batch), keep
        only those sharing a given ancestor with the walk root. If none
        share one — a wildcard/cartesian join, or a root with no direct
        given to key on — fall back to the first producer (legacy
        single-winner behaviour), so already-correct walks don't shift.
        Returns None when the id names no producer (it's a given leaf).
        """
        for _key in (_try_decode(piid), piid):
            if _key and _key in _slot_to_events:
                cands = _slot_to_events[_key]
                if len(cands) <= 1:
                    return list(cands)
                if root_givens:
                    filtered = [e for e in cands if _event_given_ids(e) & root_givens]
                    if filtered:
                        return filtered
                return [cands[0]]
        return None

    def _seed_given_lineage(inst, lind: dict[str, list[int]]) -> None:
        """Walk `parent_lib.parents` transitively from a given DataInstance.

        Mirrors virtual_runtime._seed_lineage (virtual_runtime.py:308):
        the legacy `_manifests/*.json` rows carried this full ancestry
        chain inline, not just direct parents. Reproducing it here keeps
        downstream parent-walk lookups (the `lineage.items()` loop
        below) able to find given-side `(dtype, hash15)` entries from
        kv2path's input-CSV side.
        """
        stack = [inst]
        seen: set[tuple[str, str]] = set()
        while stack:
            curr = stack.pop()
            pl = getattr(curr, "parent_lib", None)
            mark = (pl.GetKey() if pl is not None else "", str(curr.path))
            if mark in seen:
                continue
            seen.add(mark)
            p = curr.ResolvePath()
            lind.setdefault(curr.dtype.key, []).append(
                int(md5(str(p).encode()).hexdigest()[:15], 16)
            )
            if pl is None:
                continue
            for pm in pl.parents.get(curr.path, []):
                if pm.path in pl.manifest:
                    stack.append(pl.Get(pm.path))

    def _build_transitive_lind(root_ev, root_pf):
        """BFS over `consumes`; returns {dtype_key: sorted [hash15(abs_path)]}.

        Reproduces post-hoc what virtual_runtime's `_merge_lineage`
        built at channel-merge time. Each reached event contributes its
        own produces' (dtype_key, hash15(abs_path)) to the accumulator.
        Frontier branches terminating at a given (no producer event)
        seed via `_seed_given_lineage` so the ancestry chain on the
        input side of the library reaches the kv2path lookup table.

        Multi-slot events emit multiple ProducedFile siblings under a
        single event; siblings are co-produced, not each other's
        ancestors. For the ROOT event, include only `root_pf` (the file
        whose lineage is being computed) so the downstream resolver in
        CollectResults does not treat sibling outputs as parents. All
        ancestor events still contribute their full produces.
        """
        lind: dict[str, list[int]] = {}
        if root_pf.path and root_pf.dtype_key:
            rel = Path(root_pf.path)
            abs_p = output_path / rel if not rel.is_absolute() else rel
            lind.setdefault(root_pf.dtype_key, []).append(
                int(md5(str(abs_p).encode()).hexdigest()[:15], 16)
            )
        # The walk root's sample identity: the given leaves its producing
        # event directly consumes. Every ancestor of root_pf shares it,
        # so it disambiguates multi-producer slot references (Bug L/I11).
        root_givens = _event_given_ids(root_ev)
        seen_evs: set[str] = {root_ev.task_hash}
        frontier: list = []
        # Seed frontier from root_ev's consumes (without re-adding its
        # own produces — those are the file we're computing lineage FOR
        # plus its siblings).
        for parent_iids in root_ev.consumes.values():
            for piid in parent_iids:
                parent_evs = _producers_for(piid, root_givens)
                if parent_evs:
                    frontier.extend(parent_evs)
                    continue
                given = None
                for key in (_try_decode(piid), piid):
                    if key and key in inst_id2inst:
                        given = inst_id2inst[key]
                        break
                if given is not None:
                    _seed_given_lineage(given, lind)
        while frontier:
            nxt = []
            for ev in frontier:
                if ev.task_hash in seen_evs:
                    continue
                seen_evs.add(ev.task_hash)
                for pf in ev.produces:
                    if not pf.path or not pf.dtype_key:
                        continue
                    rel = Path(pf.path)
                    abs_p = output_path / rel if not rel.is_absolute() else rel
                    lind.setdefault(pf.dtype_key, []).append(
                        int(md5(str(abs_p).encode()).hexdigest()[:15], 16)
                    )
                for parent_iids in ev.consumes.values():
                    for piid in parent_iids:
                        parent_evs = _producers_for(piid, root_givens)
                        if parent_evs:
                            for pe in parent_evs:
                                if pe.task_hash not in seen_evs:
                                    nxt.append(pe)
                            continue
                        given = None
                        for key in (_try_decode(piid), piid):
                            if key and key in inst_id2inst:
                                given = inst_id2inst[key]
                                break
                        if given is not None:
                            _seed_given_lineage(given, lind)
            frontier = nxt
        return {k: sorted(set(v)) for k, v in lind.items()}

    for ev in trace_idx.events:
        for pf in ev.produces:
            if not pf.path or not pf.dtype_key:
                continue
            rel_path = Path(pf.path)
            abs_path = output_path / rel_path if not rel_path.is_absolute() else rel_path
            _hash = int(md5(str(abs_path).encode()).hexdigest()[:15], 16)
            kv = pf.dtype_key, _hash
            try:
                lind = _build_transitive_lind(ev, pf)
            except Exception as e:
                Log.Error(e)
                continue
            # Preserve CSV-side instance_id if a prior input entry
            # already claimed this kv (G2 routing identity); otherwise
            # fall back to the slot_id from the trace event.
            prior = kv2path.get(kv)
            csv_inst_id = prior[2] if prior else None
            kv2path[kv] = (
                abs_path,
                lind,
                (csv_inst_id or pf.slot_id or None),
                pf.file_instance_id,
            )
    relavent_k = {k for k, v in kv2path}
    given_manifest = []
    todo = dict(enumerate(kv2path.items()))
    prev_len = len(todo) + 1
    while len(todo)>0:
        if len(todo) == prev_len:
            for i, ((ck, cv), (path, lineage, cinst_id, file_inst_id)) in todo.items():
                Log.Warn(f"dropping entry with unresolvable lineage: [{ck}] path=[{path}]")
            break
        prev_len = len(todo)
        to_del = []
        for i, ((ck, cv), (path, lineage, cinst_id, file_inst_id)) in todo.items():
            cinst = _resolve_instance(ck, cinst_id)
            if path.is_relative_to(output_path): # is output
                parents = []
                ok = True
                for pk, pvs in lineage.items():
                    if pk not in relavent_k: continue
                    if pk == ck: continue
                    for pv in pvs:
                        k = (pk, pv)
                        if k not in kv2path: continue # likely due to a merge between branches
                        ppath, _, _, _ = kv2path[k]
                        if ppath not in path2inst:
                            ok = False
                            break
                        _inst = path2inst[ppath]
                        _path = _inst.ResolvePath()
                        parents.append((_path, _inst.dtype_name))
                        if _path in output: continue
                    if not ok: break
                if not ok: continue
                _parents = []
                for _path, _name in parents:
                    if _path not in output.manifest:
                        output.AddItem(path=_path, dtype=_name)
                    _parents.append(_path)
                _path = path.relative_to(output_path)
                _path = output.AddItem(
                    path=_path,
                    dtype=cinst.dtype_name,
                    parents=_parents,
                )
                # G3: persist the trace's file_instance_id into the published
                # manifest so DataInstanceLibrary.Load on a downstream consumer
                # sees `instance_id == ProducedFile.file_instance_id` instead of
                # the legacy (path + dtype + lib_key) fallback. Bridges
                # walk_ancestors / get_lineage_of into the trace index.
                if file_inst_id is not None:
                    output.SetLineageInstance(
                        path=_path,
                        instance_id=file_inst_id,
                        lineage_payload=b"",
                        origin="lineage",
                    )
                _inst = output.Get(_path)
                _path = _inst.ResolvePath()
                path2inst[_path] = _inst
            else:
                given_manifest.append((cinst.instance_id, cinst.dtype.key, str(path), cinst.origin))
            to_del.append(i)
        assert len(to_del)>0
        for i in to_del:
            del todo[i]
    output.PruneTypes(save=False)
    output.Save()

    _df = pd.DataFrame(given_manifest, columns=["instance_id", "dtype_key", "path", "origin"])
    _df.to_csv(output_path/"given.csv", index=False)
    return output
