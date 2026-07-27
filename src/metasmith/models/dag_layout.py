"""Deterministic layered-rails layout for directed graphs.

Pure geometry. Given named nodes and directed edges this module returns, for
every node, a (row, lane) cell, and for every edge an orthogonal polyline
through the lane grid. It does no I/O, imports no renderer, and never inspects
a node's `kind` — that value is carried through untouched for whatever backend
draws the result.

Two properties the backends depend on:

- Every edge points strictly downward, and a join lands below all of its
  inputs, because a node is not emitted until all its parents have been.
- A lane is held exclusively by one edge for the whole span between its
  endpoints' rows, so a vertical rail never crosses a node cell. The same
  polylines are therefore collision-free in a character grid and in pixels.

Nothing here depends on the order nodes or edges were added: every choice is
resolved on the node name, so structurally equivalent graphs lay out
identically across runs.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

__all__ = ["LayoutNode", "LayoutEdge", "Layout", "layout", "natural_key"]

_DIGITS = re.compile(r"(\d+)")


def natural_key(name: str) -> tuple:
    """Sort key that orders "2 x" before "10 y".

    Node names carry step numbers (`"7 megahit"`), so plain lexicographic order
    would scatter a pipeline. Digit runs compare as integers, text runs as text.
    """
    return tuple(
        (int(part), "") if part.isdigit() else (-1, part)
        for part in _DIGITS.split(name)
        if part != ""
    )


@dataclass(frozen=True)
class LayoutNode:
    name: str
    kind: Any
    row: int
    lane: int
    depth: int
    spine: bool


@dataclass(frozen=True)
class LayoutEdge:
    src: str
    dst: str
    lane: int
    points: tuple[tuple[float, float], ...]  # (row, lane), half-steps at jogs
    back: bool = False


@dataclass(frozen=True)
class Layout:
    nodes: tuple[LayoutNode, ...]  # in row order
    edges: tuple[LayoutEdge, ...]
    width: int  # lanes
    height: int  # rows

    def __post_init__(self):
        object.__setattr__(self, "_index", {n.name: n for n in self.nodes})

    @property
    def index(self) -> dict[str, LayoutNode]:
        return self._index  # type: ignore[attr-defined]

    def __getitem__(self, name: str) -> LayoutNode:
        return self._index[name]  # type: ignore[attr-defined]

    def crossing_lanes(self, row: int) -> frozenset[int]:
        """Lanes with a vertical rail passing straight through `row`."""
        idx = self.index
        return frozenset(
            e.lane
            for e in self.edges
            if not e.back and idx[e.src].row < row < idx[e.dst].row
        )

    def gap_edges(self, row: int) -> tuple[LayoutEdge, ...]:
        """Edges with a segment in the gap between `row` and `row + 1`."""
        idx = self.index
        return tuple(
            e
            for e in self.edges
            if not e.back and idx[e.src].row <= row < idx[e.dst].row
        )


def layout(
    nodes: Mapping[str, Any] | Sequence[tuple[str, Any]],
    edges: Iterable[tuple[str, str]],
) -> Layout:
    kinds = dict(nodes)
    _edges: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for src, dst in edges:
        if (src, dst) in seen:
            continue
        seen.add((src, dst))
        kinds.setdefault(src, None)
        kinds.setdefault(dst, None)
        _edges.append((src, dst))

    names = sorted(kinds, key=natural_key)
    if not names:
        return Layout(nodes=(), edges=(), width=0, height=0)

    children = {n: [] for n in names}
    for src, dst in _edges:
        children[src].append(dst)
    for n in names:
        children[n] = sorted(set(children[n]), key=natural_key)

    back = _break_cycles(names, children)
    fwd_children = {n: [c for c in children[n] if (n, c) not in back] for n in names}
    parents = {n: [] for n in names}
    for n in names:
        for c in fwd_children[n]:
            parents[c].append(n)

    topo = _topological(names, fwd_children, parents)
    depth = _depths(topo, fwd_children)
    weight, descendants = _subtree_metrics(topo, fwd_children)
    spine = _choose_spine(names, parents, fwd_children, weight, descendants)

    order = _row_order(names, parents, fwd_children, topo, weight, spine)
    node_lane, edge_lane, width = _assign_lanes(order, fwd_children, weight, spine)

    laid = tuple(
        LayoutNode(
            name=n,
            kind=kinds[n],
            row=row,
            lane=node_lane[n],
            depth=depth[n],
            spine=n in spine,
        )
        for row, n in enumerate(order)
    )
    rows = {n.name: n.row for n in laid}
    lanes = {n.name: n.lane for n in laid}

    routed = []
    for src, dst in _edges:
        if (src, dst) in back:
            routed.append(
                LayoutEdge(
                    src=src,
                    dst=dst,
                    lane=lanes[src],
                    points=((rows[src], lanes[src]), (rows[dst], lanes[dst])),
                    back=True,
                )
            )
            continue
        j = edge_lane[(src, dst)]
        routed.append(
            LayoutEdge(
                src=src,
                dst=dst,
                lane=j,
                points=_polyline(rows[src], lanes[src], rows[dst], lanes[dst], j),
            )
        )
    routed.sort(key=lambda e: (rows[e.src], e.lane, natural_key(e.dst)))

    return Layout(nodes=laid, edges=tuple(routed), width=max(width, 1), height=len(order))


# --- passes -----------------------------------------------------------------


def _break_cycles(names: list[str], children: dict[str, list[str]]) -> set[tuple[str, str]]:
    """Iterative DFS; an edge onto a node still on the stack closes a cycle.

    Plan DAGs are acyclic, but the solver graph and name-keyed nodes can both
    fold two distinct instances into one and produce a loop. Everything after
    this pass assumes a total order, so the loop-closing edges are set aside.
    """
    WHITE, GREY, BLACK = 0, 1, 2
    color = dict.fromkeys(names, WHITE)
    back: set[tuple[str, str]] = set()
    for root in names:
        if color[root] != WHITE:
            continue
        color[root] = GREY
        stack = [(root, iter(children[root]))]
        while stack:
            n, it = stack[-1]
            descended = False
            for c in it:
                if color[c] == GREY:
                    back.add((n, c))
                    continue
                if color[c] == WHITE:
                    color[c] = GREY
                    stack.append((c, iter(children[c])))
                    descended = True
                    break
            if not descended:
                color[n] = BLACK
                stack.pop()
    return back


def _topological(
    names: list[str], children: dict[str, list[str]], parents: dict[str, list[str]]
) -> list[str]:
    remaining = {n: len(parents[n]) for n in names}
    ready = [n for n in names if remaining[n] == 0]
    out: list[str] = []
    while ready:
        n = ready.pop()
        out.append(n)
        for c in children[n]:
            remaining[c] -= 1
            if remaining[c] == 0:
                ready.append(c)
    if len(out) != len(names):  # defensive: cycle breaking should prevent this
        out += [n for n in names if n not in set(out)]
    return out


def _depths(topo: list[str], children: dict[str, list[str]]) -> dict[str, int]:
    depth = dict.fromkeys(topo, 0)
    for n in topo:
        for c in children[n]:
            if depth[c] < depth[n] + 1:
                depth[c] = depth[n] + 1
    return depth


def _subtree_metrics(
    topo: list[str], children: dict[str, list[str]]
) -> tuple[dict[str, int], dict[str, int]]:
    """Longest downward path from each node, and how much hangs below it."""
    weight: dict[str, int] = {}
    reach: dict[str, set[str]] = {}
    for n in reversed(topo):
        kids = children[n]
        weight[n] = 1 + max((weight[c] for c in kids), default=0)
        below: set[str] = set()
        for c in kids:
            below.add(c)
            below |= reach[c]
        reach[n] = below
    return weight, {n: len(s) for n, s in reach.items()}


def _choose_spine(
    names: list[str],
    parents: dict[str, list[str]],
    children: dict[str, list[str]],
    weight: dict[str, int],
    descendants: dict[str, int],
) -> set[str]:
    """The heaviest path through the graph; this becomes lane 0."""
    roots = [n for n in names if not parents[n]]
    if not roots:
        roots = names
    cur = min(roots, key=lambda n: (-weight[n], -descendants[n], natural_key(n)))
    path = [cur]
    while children[cur]:
        cur = min(
            children[cur],
            key=lambda c: (-weight[c], -descendants[c], natural_key(c)),
        )
        path.append(cur)
    return set(path)


def _row_order(
    names: list[str],
    parents: dict[str, list[str]],
    children: dict[str, list[str]],
    topo: list[str],
    weight: dict[str, int],
    spine: set[str],
) -> list[str]:
    """One row per node, walked depth-first down the spine.

    Following a branch to its end before starting the next is what makes the
    drawing read as a tree: a pipeline's rows stay contiguous instead of being
    interleaved with whatever else happened to sit at the same depth.

    A node is only emitted once every one of its parents has been; until then
    it is dropped and re-offered when the next parent lands. That guard is what
    puts a join below all of its inputs, and it outranks the spine — a spine
    node with an outstanding input waits like anything else.

    Childless children go first. A leaf costs one row and frees its lane at
    once, so holding it back until its siblings' subtrees are done can only
    stretch its rail; transforms routinely emit a product nobody consumes, and
    those would otherwise trail a line down the whole drawing.
    """
    pending = {n: len(parents[n]) for n in names}
    emitted: set[str] = set()
    order: list[str] = []

    def _rank(n: str):
        tier = 0 if not children[n] else (1 if n in spine else 2)
        return (tier, -weight[n], natural_key(n))

    stack = sorted((n for n in names if not parents[n]), key=_rank, reverse=True)
    while stack:
        n = stack.pop()
        if n in emitted or pending[n] > 0:
            continue
        emitted.add(n)
        order.append(n)
        kids = sorted(children[n], key=_rank)
        for c in kids:
            pending[c] -= 1
        stack += reversed(kids)

    if len(order) < len(names):  # only reachable if cycle breaking left an island
        order += [n for n in topo if n not in emitted]
    return order


def _assign_lanes(
    order: list[str],
    children: dict[str, list[str]],
    weight: dict[str, int],
    spine: set[str],
) -> tuple[dict[str, int], dict[tuple[str, str], int], int]:
    """Lane bookkeeping in the style of a commit graph.

    A node claims the leftmost lane already reserved for it; every other lane
    reserved for it closes, which is what makes a join collapse its inputs back
    together instead of widening the drawing. It then reserves one lane per
    child, the heaviest child inheriting the node's own lane — that inheritance
    is why a linear chain renders as one straight rail.

    Which child inherits is decided by weight, not by emission order: the rail
    should follow the longest continuation even when a short leaf is drawn
    first. The remaining children take lanes in the order they will be emitted,
    so the nearest branch sits closest to its parent.
    """
    rows = {n: i for i, n in enumerate(order)}
    reserved: list[tuple[str, str] | None] = []  # lane -> (child, parent)
    node_lane: dict[str, int] = {}
    edge_lane: dict[tuple[str, str], int] = {}
    width = 0

    def _free(exclude: int | None = None) -> int:
        for i, slot in enumerate(reserved):
            if slot is None and i != exclude:
                return i
        reserved.append(None)
        return len(reserved) - 1

    for n in order:
        claimed = [i for i, slot in enumerate(reserved) if slot is not None and slot[0] == n]
        if claimed:
            lane = claimed[0]
            for i in claimed:
                edge_lane[(reserved[i][1], n)] = i  # type: ignore[index]
                reserved[i] = None
        else:
            lane = _free()
            reserved[lane] = None
        node_lane[n] = lane

        kids = children[n]
        if kids:
            primary = min(
                kids, key=lambda c: (0 if c in spine else 1, -weight[c], natural_key(c))
            )
            reserved[lane] = (primary, n)
            for c in sorted(kids, key=lambda c: rows[c]):
                if c != primary:
                    reserved[_free(exclude=lane)] = (c, n)

        width = max(width, len(reserved))
        while reserved and reserved[-1] is None:
            reserved.pop()

    return node_lane, edge_lane, width


def _polyline(
    row_src: int, lane_src: int, row_dst: int, lane_dst: int, lane: int
) -> tuple[tuple[float, float], ...]:
    """Orthogonal route: drop out of the node, cross to the rail, run down it,
    cross back above the target. Every segment is axis-aligned, which is why
    the half-row corner points come in pairs."""
    points: list[tuple[float, float]] = [(float(row_src), float(lane_src))]
    if lane != lane_src:
        points += [(row_src + 0.5, float(lane_src)), (row_src + 0.5, float(lane))]
    if lane != lane_dst:
        points += [(row_dst - 0.5, float(lane)), (row_dst - 0.5, float(lane_dst))]
    points.append((float(row_dst), float(lane_dst)))
    return tuple(points)
