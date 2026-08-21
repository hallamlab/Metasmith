from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pandas as pd

from ..caching.promote import CANONICAL_OUTPUT_PREFIX
from ..constants import AgentPaths
from ..logging import Log
from ..models.libraries import DataInstance, DataInstanceLibrary, DataTypeLibrary
from ..models.lineage import ProducedFile
from ..models.workflow import WorkflowTask

def _place(src: Path, dest: Path, strategy: str) -> None:
    if src.is_dir():
        # publishDir's own shape for a directory: a real directory whose
        # leaves are links, so the shard's bytes are never duplicated.
        dest.mkdir(parents=True, exist_ok=True)
        for child in src.iterdir():
            _place(child, dest/child.name, strategy)
        return
    if strategy == "link":
        try:
            os.link(src, dest)
            return
        except OSError:
            pass
    shutil.copy2(src, dest)


def PublishCachedProducts(workspace: Path, output_path: Path) -> int:
    """Put every cache-hit product into the results directory.

    Nextflow adds a path to the publish set only when it resolves under the
    session's own work directory (`PublishOp.collectFiles` -> `getTaskDir`),
    and a cache shard lives outside it. The path is dropped with no log and no
    error, so nothing the emitter puts on a channel can reach `results/` from a
    shard -- the driver has to place them once nextflow has exited.
    """
    manifest = workspace/AgentPaths.CACHE_PUBLISH_MANIFEST
    if not manifest.exists():
        return 0
    try:
        plan = json.loads(manifest.read_text())
    except (OSError, json.JSONDecodeError) as e:
        Log.Warn(f"unreadable cache publish manifest [{manifest}]: {e}")
        return 0
    strategy = plan.get("strategy", "link")
    placed = 0
    for entry in plan.get("publish", []):
        dest_dir = output_path/entry.get("path", "")
        dest_dir.mkdir(parents=True, exist_ok=True)
        for raw in entry.get("files", []):
            src = Path(raw)
            dest = dest_dir/src.name
            if dest.exists():
                continue
            if not src.exists():
                Log.Warn(
                    f"cache-hit product [{src}] is gone from the shard; "
                    f"[{entry.get('path')}] will be missing it"
                )
                continue
            _place(src, dest, strategy)
            placed += 1
    if placed:
        Log.Info(f"published [{placed}] product(s) from cache shards")
    return placed


def _published_index(output_path: Path) -> dict[str, Path]:
    index: dict[str, Path] = {}
    for here, dirs, files in os.walk(output_path, followlinks=False):
        rel_dir = Path(here).relative_to(output_path)
        if rel_dir.parts and rel_dir.parts[0] == "_metadata":
            dirs[:] = []
            continue
        # A directory named in the canonical output spelling is one product,
        # not a folder of them: index it, and do not index what is inside it.
        products = [d for d in dirs if CANONICAL_OUTPUT_PREFIX.match(d)]
        dirs[:] = [d for d in dirs if d not in set(products)]
        for name in products + files:
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
    # Staged libraries are pruned per-transform (PruneTypes), so two libraries
    # in the same namespace can carry disjoint types -- union them instead of
    # last-one-wins, and copy rather than mutate the incoming library's own
    # DataTypeLibrary object.
    for lib in list(task.transform_libraries) + list(task.data_libraries):
        for namespace, tlib in lib.types.items():
            if namespace in tlibs:
                _lib = tlibs[namespace]
            else:
                _lib = DataTypeLibrary.Unpack(tlib.Pack())
                _lib.types = {}
                tlibs[namespace] = _lib
            for k, e in tlib.types.items():
                if k in _lib.types: continue
                _lib.types[k] = e
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
