from __future__ import annotations

import json
import os
import re
from dataclasses import field
from hashlib import md5
from pathlib import Path

import pandas as pd

from ..logging import Log
from ..models.libraries import DataInstance, DataInstanceLibrary, DataTypeLibrary
from ..models.workflow import WorkflowTask

def _published_index(output_path: Path) -> dict[str, Path]:
    index: dict[str, Path] = {}
    for here, dirs, files in os.walk(output_path, followlinks=False):
        rel_dir = Path(here).relative_to(output_path)
        if rel_dir.parts and rel_dir.parts[0] == "_metadata":
            dirs[:] = []
            continue
        for name in files:
            index.setdefault(name, rel_dir/name)
    return index


def _published_path(path: Path, output_path: Path, index: dict[str, Path]) -> Path:
    rel = path.relative_to(output_path)
    if (output_path/rel).exists():
        return rel
    found = index.get(rel.name)
    if found is None:
        Log.Warn(f"produced file [{rel}] is not in the results directory")
        return rel
    return found


def CollectResults(
    task: WorkflowTask,
    output_path: Path,
    inputs_dir: Path,
) -> DataInstanceLibrary:
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
    kv2path: dict[tuple[str, int], tuple[Path, dict, str|None, str|None]] = {}
    for in_manifest in inputs_dir.iterdir():
        k = in_manifest.name
        with open(in_manifest) as f:
            for l in f:
                p = Path(l[:-1])
                _hash = md5(str(p).encode()).hexdigest()
                _hash = int(_hash[:15], 16)
                kv2path[(k, _hash)] = p, {}, path2iid.get(str(p)), None
    from ..telemetry import TraceIndex
    trace_idx = TraceIndex.read(output_path.parent / "_metasmith" / "trace.jsonl")
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
    def _try_decode(piid: str) -> str | None:
        try:
            return bytes.fromhex(piid).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            return None
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
        out: set[str] = set()
        for _vals in ev.consumes.values():
            for _piid in _vals:
                for _key in (_try_decode(_piid), _piid):
                    if _key and _key in inst_id2inst and _key not in _slot_to_events:
                        out.add(_key)
                        break
        return out

    def _producers_for(piid, root_givens):
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
        lind: dict[str, list[int]] = {}
        if root_pf.path and root_pf.dtype_key:
            rel = Path(root_pf.path)
            abs_p = output_path / rel if not rel.is_absolute() else rel
            lind.setdefault(root_pf.dtype_key, []).append(
                int(md5(str(abs_p).encode()).hexdigest()[:15], 16)
            )
        root_givens = _event_given_ids(root_ev)
        seen_evs: set[str] = {root_ev.task_hash}
        frontier: list = []
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
    published = _published_index(output_path)
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
            if path.is_relative_to(output_path):
                parents = []
                ok = True
                for pk, pvs in lineage.items():
                    if pk not in relavent_k: continue
                    if pk == ck: continue
                    for pv in pvs:
                        k = (pk, pv)
                        if k not in kv2path: continue
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
                _path = _published_path(path, output_path, published)
                _path = output.AddItem(
                    path=_path,
                    dtype=cinst.dtype_name,
                    parents=_parents,
                )
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
