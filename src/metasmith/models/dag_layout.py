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
    owner = _ownership(names, parents, depth)
    lane_width, owned_size = _lane_widths(topo, fwd_children, owner)

    order = _row_order(
        names, parents, fwd_children, topo, spine, lane_width, owned_size
    )
    node_lane, edge_lane, width = _assign_lanes(order, fwd_children, weight, spine)
    packed_node, packed_edge, packed_width = _recolour(order, node_lane, edge_lane)
    if packed_width < width:  # never accept a repack that costs a lane
        node_lane, edge_lane, width = packed_node, packed_edge, packed_width

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


def _ownership(
    names: list[str], parents: dict[str, list[str]], depth: dict[str, int]
) -> dict[str, str]:
    """A spanning forest of the DAG: one owning parent per non-root node.

    Subtrees in a DAG overlap wherever there is a join, so "how wide is what
    hangs below this node" double-counts unless the overlap is assigned. The
    owner is the deepest parent, which is the one that emits the node under the
    all-parents-first guard; owned subtrees are then disjoint by construction.
    """
    return {
        n: max(parents[n], key=lambda p: (depth[p], natural_key(p)))
        for n in names
        if parents[n]
    }


def _lane_widths(
    topo: list[str], children: dict[str, list[str]], owner: dict[str, str]
) -> tuple[dict[str, int], dict[str, int]]:
    """How many lanes drawing each node's owned subtree takes, and its size.

    Every child of a fan-out reserves a lane at once, so while the i-th of k
    children is being drawn the k-1-i siblings after it are still holding
    theirs: the peak is `max over i of (k - 1 - i) + width(child i)`. Sorting
    the children narrowest-first minimises that maximum, which is what the row
    ordering then does.

    A child owned by someone else costs one lane rather than its whole width —
    its subtree will be drawn under its owner, not here.
    """
    width: dict[str, int] = {}
    size: dict[str, int] = {}
    for n in reversed(topo):
        owned = [c for c in children[n] if owner.get(c) == n]
        size[n] = 1 + sum(size[c] for c in owned)
        costs = sorted(width[c] if owner.get(c) == n else 1 for c in children[n])
        k = len(costs)
        width[n] = max([1] + [(k - 1 - i) + w for i, w in enumerate(costs)])
    return width, size


def _row_order(
    names: list[str],
    parents: dict[str, list[str]],
    children: dict[str, list[str]],
    topo: list[str],
    spine: set[str],
    lane_width: dict[str, int],
    owned_size: dict[str, int],
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

    After that, siblings go narrowest-first: every sibling still waiting holds
    a lane, so the wide subtree should be the one with the fewest siblings left
    beside it. Roots are ranked separately, by how much of the graph they own —
    a reference-database root owning nothing must sink to the bottom, where it
    is emitted a row above its first consumer instead of holding a lane down
    the whole drawing. That used to happen by accident, and stating it is what
    lets the sibling rank change without wrecking it.

    Not doing: emitting a root only when a consumer needs it. It is safe — a
    parentless node is always emittable and lands strictly before its consumer
    — but measured on real plans it is a pessimisation, because a root pulled
    down to its consumer arrives in the middle of an open fan-out instead of
    before it.
    """
    pending = {n: len(parents[n]) for n in names}
    emitted: set[str] = set()
    order: list[str] = []

    def _rank(n: str):
        tier = 0 if not children[n] else (1 if n in spine else 2)
        return (tier, lane_width[n], natural_key(n))

    def _root_rank(n: str):
        return (-owned_size[n], natural_key(n))

    roots = (n for n in names if not parents[n])
    stack = sorted(roots, key=_root_rank, reverse=True)
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


def _recolour(
    order: list[str],
    node_lane: dict[str, int],
    edge_lane: dict[tuple[str, str], int],
) -> tuple[dict[str, int], dict[tuple[str, str], int], int]:
    """Repack the lanes once the rows are known.

    The greedy pass assigns a lane the moment a node reserves one, so a lane
    opened early and released late blocks a later rail that would have fitted
    beside it. Once the rows are fixed, though, every rail and every chain
    occupies a known contiguous run of rows, and packing intervals is a much
    easier problem. Measured on 300 random DAGs this is a lane narrower 18% of
    the time and never wider. It does nothing for the spanish-lakes
    metagenomics plan, whose width is set by rails that really are all live at
    once — see the caller, which keeps the greedy result unless this beats it.

    Only lane indices move. Rows, routing and which rail carries which edge are
    all untouched, so nothing downstream can shift underneath this.

    Two kinds of interval, and the difference is the whole point:

    - a *strand* — a node and the chain of children that inherit its lane —
      holds its rows closed, because a marker sits in each of them.
    - a *rail* holds its rows open: it leaves its source's row and arrives at
      its target's, so it may share a lane with a strand ending exactly where
      it starts. A rail between adjacent rows spans no row at all and needs no
      lane of its own; it jogs across inside the half-row and is given its
      target's lane, which also keeps its polyline free of repeated points.
    """
    rows = {n: i for i, n in enumerate(order)}
    # a node continues its parent's strand exactly when the edge between them
    # stayed in that one lane; at most one parent per node can qualify, because
    # a lane is held by one rail at a time
    def _links(u: str, v: str, j: int) -> bool:
        return j == node_lane[u] == node_lane[v]

    inbound: dict[str, str] = {}
    for (u, v), j in edge_lane.items():
        if _links(u, v, j):
            inbound[v] = u

    strand_of: dict[str, int] = {}
    strands: list[list[int]] = []  # index -> [first row, last row]
    for n in order:
        if n in inbound:
            s = strand_of[inbound[n]]
            strands[s][1] = rows[n]
        else:
            s = len(strands)
            strands.append([rows[n], rows[n]])
        strand_of[n] = s

    items: list[tuple[int, int, object]] = [
        (lo, hi, ("strand", i)) for i, (lo, hi) in enumerate(strands)
    ]
    free_rails: set[tuple[str, str]] = set()
    for (u, v), j in edge_lane.items():
        # note this is a stricter test than "same strand": an edge from a node
        # to its own grandchild stays inside one strand but still needs a lane
        # of its own, because the node between them sits in the strand's lane
        if _links(u, v, j):
            continue
        lo, hi = rows[u] + 1, rows[v] - 1
        if lo > hi:
            free_rails.add((u, v))
        else:
            items.append((lo, hi, ("rail", (u, v))))

    end_of_lane: list[int] = []  # lane -> last row it is busy through
    new_node: dict[str, int] = {}
    new_edge: dict[tuple[str, str], int] = {}
    for lo, hi, item in sorted(items, key=lambda t: (t[0], t[1], _item_key(t[2]))):
        kind, key = item  # type: ignore[misc]
        was = node_lane[order[lo]] if kind == "strand" else edge_lane[key]  # type: ignore[index]
        # keep an item where it already was whenever that lane is free. Pure
        # left-edge colouring packs harder but slides branches sideways under
        # each other, which turns a fan-in comb into a zigzag of rails jogging
        # left and right past one another to save a lane nobody missed.
        if was < len(end_of_lane) and end_of_lane[was] < lo:
            lane = was
        else:
            lane = next(
                (i for i, end in enumerate(end_of_lane) if end < lo), len(end_of_lane)
            )
        if lane == len(end_of_lane):
            end_of_lane.append(hi)
        else:
            end_of_lane[lane] = hi
        if kind == "strand":
            for n in order[lo:hi + 1]:
                if strand_of[n] == key:
                    new_node[n] = lane
        else:
            new_edge[key] = lane  # type: ignore[index]

    for (u, v), j in edge_lane.items():
        if (u, v) in new_edge:
            continue
        new_edge[(u, v)] = new_node[v] if (u, v) in free_rails else new_node[u]
    return new_node, new_edge, max(len(end_of_lane), 1)


def _item_key(item) -> tuple:
    """Total order over items, so ties in the interval sort never flap."""
    kind, key = item
    return (kind, key) if kind == "strand" else (kind, natural_key(key[0]), natural_key(key[1]))


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
