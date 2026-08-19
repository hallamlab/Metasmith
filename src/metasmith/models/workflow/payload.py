from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, Iterable, Sequence

from ...hashing import KeyGenerator
from ..lineage import LinPayload
from ..libraries import DataInstance
from ..solver import Endpoint


# The on-channel index of one staged file: its own identity under its dtype key,
# plus the identities it inherited. Orchestrator.groovy builds these incrementally
# on the Nextflow channel (`postIn` seeds a leaf, `_post` stamps a produced file,
# `combineIndexes` unions them). Nothing outside Nextflow has a channel, so the
# runtimes that stand in for it build the same maps here.
Index = dict[str, list[str]]


def merge_indexes(maps: Iterable[Index]) -> Index:
    merged: Index = {}
    for m in maps:
        for k, ids in m.items():
            merged[k] = merged.get(k, []) + list(ids)
    return {k: sorted(set(v)) for k, v in merged.items()}


def given_index(
    inst: DataInstance,
    given_by_path: dict[Path, DataInstance],
) -> Index:
    # Mirrors the seed nextflow_codegen writes to workflow.lineage_of_given.json:
    # the leaf's own instance_id, plus one hop of parents that are themselves
    # given to this workflow. Not the transitive closure — an ancestor outside
    # the given set has no channel and therefore no identity here.
    index: Index = {inst.dtype.key: [inst.instance_id]}
    for pm in inst.parent_lib.parents.get(inst.path, []):
        if pm.path not in inst.parent_lib.manifest:
            continue
        parent = given_by_path.get(inst.parent_lib.Get(pm.path).ResolvePath())
        if parent is None:
            continue
        index[parent.dtype.key] = index.get(parent.dtype.key, []) + [
            parent.instance_id
        ]
    return {k: sorted(set(v)) for k, v in index.items()}


def output_index(inputs: Index, dtype_key: str, file_id: str) -> Index:
    # Orchestrator._post: copy the incoming index, stamp this file's identity
    # under its own channel name. The copy is not optional — one index object is
    # shared across every output of a multi-output process.
    index = {k: list(v) for k, v in inputs.items() if k not in LinPayload.RESERVED_KEYS}
    index[dtype_key] = [file_id]
    return index


def output_file_id(slot_id: str, path: str | Path) -> str:
    # The `sid` Orchestrator._post hashes with is the compile-time slot id the
    # step meta records, not anything the runtime derives for itself.
    return LinPayload.mint_file_id(slot_id, path)


def build_entry(
    slots: Sequence[tuple[str, Sequence[tuple[Path, Index]]]],
) -> dict:
    """One `lin` entry: the merged index, FILES and PROV for a single batch member.

    `slots` is one `(dtype_key, staged_files)` pair per required dependency, in
    `requires` order — the positional shape FILES and PROV both ride. PROV keeps
    the per-item maps un-flattened, which is the only record of which staged file
    descends from which; the merged index cannot answer that.
    """
    files = [[str(p) for p, _ix in group] for _k, group in slots]
    prov = [[ix for _p, ix in group] for _k, group in slots]
    entry: dict = merge_indexes(m for group in prov for m in group)
    entry[LinPayload.FILES_KEY] = files
    entry[LinPayload.PROV_KEY] = prov
    return entry


def render_lin_line(entries: list[dict]) -> str:
    return LinPayload(v=LinPayload.VERSION, entries=entries).to_json()


def output_name_hash(entry: dict) -> str:
    # PROV only, NOT lineage_index(): FILES *is* folded into this hash, and
    # dropping it would rename every output file, which re-mints every
    # file_instance_id and severs existing shards from new runs. PROV's values
    # are maps, so the sort below raises on them.
    lin = {k: v for k, v in entry.items() if k != LinPayload.PROV_KEY}
    slin = {k: sorted(lin[k]) for k in sorted(lin.keys())}
    _, h = KeyGenerator.FromStr(json.dumps(slin), l=16)
    return h


def output_file_name(
    entry: dict, dtype: Endpoint, *, batch: int, item: int, branch: int
) -> str:
    """The name a transform's output file takes, everywhere one is named."""
    return (
        f"{batch + 1}-{item + 1}-{branch + 1}."
        f"{output_name_hash(entry)}-{dtype.key}{dtype.GetPreferredFileExtension()}"
    )
