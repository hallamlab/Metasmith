"""A lineage report the run's driver rewrites while nextflow is still going.

It reads what tasks already leave on disk as they finish -- each task's
`.command.cache` records, the orchestrator's hit log and the shard manifests it
names -- and writes two CSVs. It is never a source of truth: nothing reads it
back, and it opens no store and appends to no trace, so it cannot change what a
run computes, caches or collects.
"""

from __future__ import annotations

import csv
import itertools
import json
import os
import re
import threading
from dataclasses import dataclass
from pathlib import Path

from ..caching.admission import shard_for
from ..caching.invocation import read_manifest
from ..caching.promote import CACHE_HITS_LOG, _read_session_id, _record_files
from ..models.lineage import LinPayload

EXCLUDED_NAMESPACES = frozenset({"lib", "containers", "env"})
LINEAGE_CSV = "lineage.csv"
PARENTS_CSV = "lineage_parents.csv"
GIVEN_LINEAGE_FILE = "workflow.lineage_of_given.json"
SELF_ID_KEY = "__self__"
ROW_CAP = 2_000_000


@dataclass(frozen=True)
class Node:
    id: str
    dtype_name: str
    path: str
    parents: tuple[str, ...] = ()


def _namespace(name: str) -> str:
    return name.split("::", 1)[0]


def _shown(name: str) -> bool:
    return _namespace(name) not in EXCLUDED_NAMESPACES


def _dep_names(step, deps) -> list[str]:
    names: list[str] = []
    for d in deps:
        for inst in step.dependency_map.get(d, []):
            if _shown(inst.dtype_name) and inst.dtype_name not in names:
                names.append(inst.dtype_name)
    return names


def plan_columns(plan) -> list[str]:
    columns: list[str] = []
    for inst in plan.given:
        if _shown(inst.dtype_name) and inst.dtype_name not in columns:
            columns.append(inst.dtype_name)
    for step in plan.steps:
        produced = [d for g in step.transform.model.produces for d in g]
        for name in _dep_names(step, produced):
            if name not in columns:
                columns.append(name)
    return columns


def type_edges(plan) -> list[tuple[str, str]]:
    """(child, parent) dtype names, walked the way `WorkflowPlan.BuildDAG` walks them."""
    edges: set[tuple[str, str]] = set()
    names_by_type: dict = {}
    for inst in plan.given:
        if _shown(inst.dtype_name):
            names_by_type.setdefault(inst.dtype, set()).add(inst.dtype_name)
    for endpoint, children in names_by_type.items():
        for p in endpoint.parents:
            for parent in names_by_type.get(p, ()):
                edges.update((c, parent) for c in children)
    for step in plan.steps:
        required = _dep_names(step, step.transform.model.requires)
        produced = _dep_names(step, [d for g in step.transform.model.produces for d in g])
        edges.update((c, p) for c in produced for p in required)
    return sorted(edges)


def _given_parents(workspace: Path) -> dict[str, tuple[str, ...]]:
    try:
        raw = json.loads((workspace / GIVEN_LINEAGE_FILE).read_text())
    except (OSError, ValueError):
        return {}
    out: dict[str, tuple[str, ...]] = {}
    for rows in (raw.get("lineage") or {}).values():
        for row in rows:
            parents = tuple(
                pid for k, ids in row.items() if k != SELF_ID_KEY for pid in ids
            )
            for self_id in row.get(SELF_ID_KEY, []):
                out[self_id] = parents
    return out


def _jsonl(path: Path):
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            yield json.loads(line)
        except ValueError:
            continue


def _file_node(f: dict, path: Path) -> Node | None:
    slot_id, relpath = f.get("slot_id", ""), f.get("relpath", "")
    if not slot_id or not relpath:
        return None
    return Node(
        id=LinPayload.mint_file_id(slot_id, relpath),
        dtype_name=f.get("dtype_name", ""),
        path=str(path),
        parents=tuple(f.get("parents") or ()),
    )


def read_nodes(workspace: Path, plan, cache_root: Path) -> dict[str, Node]:
    """Every instance this run has on disk so far, keyed by its lineage id."""
    nodes: dict[str, Node] = {}
    given_parents = _given_parents(workspace)
    for inst in plan.given:
        path = inst.ResolvePath()
        nodes[inst.instance_id] = Node(
            id=inst.instance_id,
            dtype_name=inst.dtype_name,
            path=str(path if path.is_absolute() else Path(os.path.abspath(path))),
            parents=given_parents.get(inst.instance_id, ()),
        )

    # A work directory outlives a run; only this run's session is its lineage.
    # Before the trace opens there is no session to filter on.
    session = _read_session_id(workspace)
    for rec_file in _record_files(workspace):
        for rec in _jsonl(rec_file):
            if session and int(rec.get("session", -1)) != session:
                continue
            position = int(rec.get("member", 0)) + 1
            for f in rec.get("produces") or []:
                name = re.sub(r"^1-", f"{position}-", Path(f.get("relpath", "")).name, count=1)
                node = _file_node(f, rec_file.parent / name)
                if node is not None:
                    nodes[node.id] = node

    for hit in _jsonl(workspace / CACHE_HITS_LOG):
        key = str(hit.get("key", ""))
        if not key:
            continue
        shard = shard_for(cache_root, key, "lineage")
        for f in (read_manifest(shard) or {}).get("files") or []:
            node = _file_node(f, shard / f.get("relpath", ""))
            if node is not None:
                nodes[node.id] = node
    return nodes


def explode(nodes: dict[str, Node], columns: list[str], cap: int = ROW_CAP) -> tuple[list[tuple[str, ...]], bool]:
    """One row per consistent ancestor combination of each leaf.

    A leaf is an instance no recorded instance names as a parent. Walking up, a
    node with several parents of one type forks the row, one branch per parent;
    a branch that would put a second, different path into a filled column is
    not a combination that exists and is dropped. A diamond reaches one
    ancestor by two routes with the same path, so it never forks.
    """
    index = {c: i for i, c in enumerate(columns)}
    referenced = {p for n in nodes.values() for p in n.parents}
    rows: set[tuple[str, ...]] = set()
    for leaf in sorted(i for i in nodes if i not in referenced):
        stack = [((), frozenset(), (leaf,))]
        while stack:
            filled, seen, pending = stack.pop()
            cells = dict(filled)
            consistent = True
            frontier: list[str] = []
            for nid in pending:
                if nid in seen:
                    continue
                seen = seen | {nid}
                node = nodes[nid]
                col = index.get(node.dtype_name)
                if col is not None:
                    if cells.get(col, node.path) != node.path:
                        consistent = False
                        break
                    cells[col] = node.path
                frontier.extend(
                    p for p in node.parents
                    if p in nodes and p not in seen and _shown(nodes[p].dtype_name)
                )
            if not consistent:
                continue
            if not frontier:
                rows.add(tuple(cells.get(i, "") for i in range(len(columns))))
                if len(rows) >= cap:
                    return sorted(rows), True
                continue
            by_type: dict[str, list[str]] = {}
            for p in dict.fromkeys(frontier):
                by_type.setdefault(nodes[p].dtype_name, []).append(p)
            state = tuple(sorted(cells.items()))
            for choice in itertools.product(*by_type.values()):
                stack.append((state, seen, choice))
    return sorted(rows), False


def write_csv_atomic(path: Path, header: list[str], rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident():x}.tmp")
    try:
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(rows)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


class LineageReport:
    def __init__(self, *, workspace: Path, plan, cache_root: Path, out_dir: Path):
        self.workspace = Path(workspace)
        self.plan = plan
        self.cache_root = Path(cache_root)
        self.out_dir = Path(out_dir)
        self.columns = plan_columns(plan)

    def write_parents(self) -> None:
        write_csv_atomic(self.out_dir / PARENTS_CSV, ["child", "parent"], type_edges(self.plan))

    def fingerprint(self) -> tuple:
        mtimes = []
        for p in _record_files(self.workspace):
            try:
                mtimes.append(p.stat().st_mtime_ns)
            except OSError:
                continue
        try:
            hits = (self.workspace / CACHE_HITS_LOG).stat().st_size
        except OSError:
            hits = -1
        return len(mtimes), max(mtimes, default=0), hits

    def rebuild(self) -> tuple[int, bool]:
        nodes = read_nodes(self.workspace, self.plan, self.cache_root)
        rows, truncated = explode(nodes, self.columns)
        write_csv_atomic(self.out_dir / LINEAGE_CSV, self.columns, rows)
        return len(rows), truncated
