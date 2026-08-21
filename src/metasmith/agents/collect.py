from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from ..logging import Log
from ..models.libraries import DataInstance, DataInstanceLibrary, DataTypeLibrary
from ..models.lineage import ProducedFile
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


def _published_path(
    path: Path, output_path: Path, index: dict[str, Path], warn: bool = True,
) -> Path:
    rel = path.relative_to(output_path)
    if (output_path/rel).exists():
        return rel
    found = index.get(rel.name)
    if found is None:
        # An unpublished intermediate is still registered: the entry is the only
        # thing that lets a target name it as an ancestor. It just has no file.
        if warn:
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

    # Every produced file in the run, keyed by the identity its producer minted
    # for it. The trace is a record, not a reconstruction: a row already names
    # what the file is, where it landed and which files its task read.
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
            if not _cands:
                continue
            if _pf.slot_id:
                inst_id2inst.setdefault(_pf.slot_id, _cands[0])
            if _pf.file_instance_id:
                inst_id2inst.setdefault(_pf.file_instance_id, _cands[0])

    produced: dict[str, ProducedFile] = {}
    for ev in trace_idx.events:
        for pf in ev.produces:
            if not pf.file_instance_id or not pf.path or not pf.dtype_key:
                continue
            produced[pf.file_instance_id] = pf

    published = _published_index(output_path)
    registered: dict[str, Path] = {}
    resolving: list[str] = []

    def _register(fid: str) -> Path:
        if fid in registered:
            return registered[fid]
        if fid in resolving:
            raise ValueError(
                f"lineage cycle through instance [{fid}]: "
                f"{' -> '.join(resolving[resolving.index(fid):])} -> {fid}"
            )
        pf = produced.get(fid)
        if pf is None:
            inst = inst_id2inst.get(fid)
            if inst is None:
                raise KeyError(
                    f"parent instance [{fid}] is neither a file this run "
                    "produced nor an input it was given; the trace names a "
                    "file nothing accounts for"
                )
            path = output.AddItem(path=inst.ResolvePath(), dtype=inst.dtype_name)
            registered[fid] = path
            return path

        resolving.append(fid)
        try:
            parents = sorted({_register(parent) for parent in pf.parents})
        finally:
            resolving.pop()

        rel = Path(pf.path)
        abs_path = rel if rel.is_absolute() else output_path / rel
        cinst = _resolve_instance(pf.dtype_key, pf.slot_id or fid)
        path = output.AddItem(
            path=_published_path(
                abs_path, output_path, published,
                warn=task.plan.publish_intermediates,
            ),
            dtype=cinst.dtype_name,
            parents=parents,
        )
        output.SetLineageInstance(
            path=path,
            instance_id=fid,
            lineage_payload=b"",
            origin="lineage",
        )
        registered[fid] = path
        return path

    for fid in produced:
        _register(fid)

    # given.csv is the run's record of what it was handed, in the spelling the
    # inputs manifests used -- absolute host paths, not anything relativised.
    given_manifest = []
    for in_manifest in sorted(inputs_dir.iterdir()):
        dtype_key = in_manifest.name
        with open(in_manifest) as f:
            for line in f:
                p = line.strip()
                if not p:
                    continue
                inst = _resolve_instance(dtype_key, path2iid.get(p))
                given_manifest.append(
                    (inst.instance_id, inst.dtype.key, p, inst.origin)
                )

    output.PruneTypes(save=False)
    output.Save()

    _df = pd.DataFrame(given_manifest, columns=["instance_id", "dtype_key", "path", "origin"])
    _df.to_csv(output_path/"given.csv", index=False)
    return output
