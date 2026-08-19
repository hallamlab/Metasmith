from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

__all__ = [
    "LayoutNode", "LayoutEdge", "Layout", "Metrics", "Motif",
    "layout", "measure", "dominators", "natural_key", "repeat_motifs",
]

_DIGITS = re.compile(r"(\d+)")
_SIDE_BRANCH = (2, 0)
_SIGNATURE_DEPTH = 3


def natural_key(name: str) -> tuple:
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
    points: tuple[tuple[float, float], ...]
    back: bool = False


@dataclass(frozen=True)
class Layout:
    nodes: tuple[LayoutNode, ...]
    edges: tuple[LayoutEdge, ...]
    width: int
    height: int

    def __post_init__(self):
        object.__setattr__(self, "_index", {n.name: n for n in self.nodes})

    @property
    def index(self) -> dict[str, LayoutNode]:
        return self._index  # type: ignore[attr-defined]

    def __getitem__(self, name: str) -> LayoutNode:
        return self._index[name]  # type: ignore[attr-defined]

    def crossing_lanes(self, row: int) -> frozenset[int]:
        idx = self.index
        return frozenset(
            e.lane
            for e in self.edges
            if not e.back and idx[e.src].row < row < idx[e.dst].row
        )

    def gap_edges(self, row: int) -> tuple[LayoutEdge, ...]:
        idx = self.index
        return tuple(
            e
            for e in self.edges
            if not e.back and idx[e.src].row <= row < idx[e.dst].row
        )


def layout(
    nodes: Mapping[str, Any] | Sequence[tuple[str, Any]],
    edges: Iterable[tuple[str, str]],
    order: Sequence[str] | None = None,
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
    sig = _signatures(topo, fwd_children, kinds, _SIGNATURE_DEPTH)
    motifs = _motifs(topo, fwd_children, sig)

    given = _given_order(order, names, fwd_children)
    if given is not None:
        return _compose(
            given, kinds, _edges, back, fwd_children, depth, weight, spine, motifs,
        )

    best_key = best_layout = None
    for jump in _SIDE_BRANCH:
        rows = _row_order(
            names, parents, fwd_children, topo, spine, lane_width, owned_size,
            jump, motifs, sig,
        )
        cand = _compose(
            rows, kinds, _edges, back, fwd_children, depth, weight, spine, motifs
        )
        m = measure(cand, motifs)
        key = (-m.congruent, m.rail_rows, m.lanes, m.crossings)
        if best_key is None or key < best_key:
            best_key, best_layout = key, cand
    return best_layout  # type: ignore[return-value]


def _given_order(
    order: Sequence[str] | None,
    names: list[str],
    fwd_children: dict[str, list[str]],
) -> list[str] | None:
    if order is None:
        return None
    rows = list(order)
    if len(rows) != len(names) or set(rows) != set(names):
        return None
    at = {n: i for i, n in enumerate(rows)}
    for n in rows:
        if any(at[c] <= at[n] for c in fwd_children[n]):
            return None
    return rows


def _compose(
    order: list[str],
    kinds: dict[str, Any],
    _edges: list[tuple[str, str]],
    back: set[tuple[str, str]],
    fwd_children: dict[str, list[str]],
    depth: dict[str, int],
    weight: dict[str, int],
    spine: set[str],
    motifs: Sequence[Motif] = (),
) -> Layout:
    shift: dict[str, tuple[str, str, str]] = {}
    for m in motifs:
        inverse = [{m.twin[y]: y for y in b} for b in m.blocks]
        for i, (head, block) in enumerate(zip(m.heads, m.blocks)):
            if i == 0:
                continue
            for x in block:
                mate = inverse[i - 1].get(m.twin[x])
                if mate is not None:
                    shift[x] = (mate, head, m.heads[i - 1])
    greedy = _assign_lanes(order, fwd_children, weight, spine)
    candidates = [greedy, _recolour(order, *greedy[:2])]
    if shift:
        candidates.append(_recolour(order, *greedy[:2], shift=shift))
        even = _assign_lanes(
            order, fwd_children, weight, spine,
            _canonical_primaries(motifs, fwd_children, spine, weight),
        )
        candidates.append(even)
        candidates.append(_recolour(order, *even[:2], shift=shift))

    best_key = best = None
    for node_lane, edge_lane, width in candidates:
        cand = _build(order, kinds, _edges, back, depth, spine, node_lane, edge_lane, width)
        m = measure(cand, motifs)
        key = (-m.congruent, width, m.crossings, m.detours)
        if best_key is None or key < best_key:
            best_key, best = key, cand
    return best  # type: ignore[return-value]


def _build(
    order: list[str],
    kinds: dict[str, Any],
    _edges: list[tuple[str, str]],
    back: set[tuple[str, str]],
    depth: dict[str, int],
    spine: set[str],
    node_lane: dict[str, int],
    edge_lane: dict[tuple[str, str], int],
    width: int,
) -> Layout:
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


def _break_cycles(names: list[str], children: dict[str, list[str]]) -> set[tuple[str, str]]:
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
    if len(out) != len(names):
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
    return {
        n: max(parents[n], key=lambda p: (depth[p], natural_key(p)))
        for n in names
        if parents[n]
    }


def _lane_widths(
    topo: list[str], children: dict[str, list[str]], owner: dict[str, str]
) -> tuple[dict[str, int], dict[str, int]]:
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
    jump: int,
    motifs: Sequence[Motif] = (),
    sig: Mapping[str, int] | None = None,
) -> list[str]:
    sig = sig or {}
    pending = {n: len(parents[n]) for n in names}
    emitted: set[str] = set()
    order: list[str] = []
    row_of: dict[str, int] = {}

    class_of: dict[str, Motif] = {}
    instance_of: dict[str, int] = {}
    for m in motifs:
        for i, b in enumerate(m.blocks):
            for x in b:
                class_of[x] = m
                instance_of[x] = i

    def _plain_rank(n: str, siblings: list[str]):
        if not children[n]:
            tier = 0
        elif n in spine:
            tier = 2
        else:
            biggest = max(owned_size[s] for s in siblings)
            small = owned_size[n] * jump <= biggest - owned_size[n]
            tier = 1 if jump and small else 3
        return (tier, lane_width[n], natural_key(n))

    child_order: dict[tuple[int, int], int] = {}
    for m in motifs:
        for x in m.blocks[0]:
            kids = sorted(children[x], key=lambda c: _plain_rank(c, children[x]))
            for i, c in enumerate(kids):
                child_order.setdefault((sig[x], sig[c]), i)

    def _rank(parent: str, n: str, siblings: list[str]):
        canon = 0
        if parent in class_of:
            canon = child_order.get((sig[parent], sig[n]), len(siblings))
        return (canon,) + _plain_rank(n, siblings)

    def _root_rank(n: str):
        return (-owned_size[n], natural_key(n))

    roots = sorted((n for n in names if not parents[n]), key=_root_rank)
    held = set(roots[1:])
    supply: set[str] = set()
    for n in topo:
        if parents[n]:
            if all(p in supply for p in parents[n]):
                supply.add(n)
        elif n in held:
            supply.add(n)

    stack: list[str] = []
    blocked: list[str] = []
    forced: set[str] = set()

    def _gated(n: str) -> bool:
        if n in forced:
            return False
        spread: dict[int, set[int]] = {}
        for p in parents[n]:
            m = class_of.get(p)
            if m is not None:
                spread.setdefault(id(m), set()).add(instance_of[p])
        for p in parents[n]:
            m = class_of.get(p)
            if m is None or len(spread[id(m)]) < 2:
                continue
            if any(x not in emitted for b in m.blocks for x in b):
                return True
        return False

    def _release() -> None:
        ready = [n for n in blocked if n not in emitted and not _gated(n)]
        if not ready:
            return
        for n in ready:
            blocked.remove(n)
        stack.extend(reversed(sorted(ready, key=natural_key)))

    def _emit(n: str) -> None:
        emitted.add(n)
        order.append(n)
        row_of[n] = len(order) - 1
        kids = sorted(children[n], key=lambda c: _rank(n, c, children[n]))
        for c in kids:
            pending[c] -= 1
        stack.extend(reversed(kids))
        _release()

    def _hoist_to(n: str, chain: list[str]) -> int | None:
        m = class_of.get(n)
        if m is None or len(m.heads) < 2:
            return None
        at = min((row_of[h] for h in m.heads if h in row_of), default=None)
        if at is None:
            return None
        inside = set(chain)
        for x in chain:
            for p in parents[x]:
                if p not in inside and row_of.get(p, -1) >= at:
                    return None
        return at

    def _pull(n: str) -> bool:
        unmet = [p for p in parents[n] if p not in emitted]
        if not unmet or any(p not in supply for p in unmet):
            return False
        chain: list[str] = []
        seen: set[str] = set()

        def _visit(x: str) -> None:
            if x in emitted or x in seen:
                return
            seen.add(x)
            for p in sorted(parents[x], key=natural_key):
                _visit(p)
            chain.append(x)

        for p in sorted(unmet, key=natural_key):
            _visit(p)
        at = _hoist_to(n, chain)
        mark = len(order)
        for x in chain:
            _emit(x)
        if at is not None and at < mark:
            moved = order[mark:]
            del order[mark:]
            order[at:at] = moved
            row_of.clear()
            row_of.update({x: i for i, x in enumerate(order)})
        return True

    remaining = list(roots[1:])
    stack += roots[:1]
    while True:
        while stack:
            n = stack.pop()
            if n in emitted:
                continue
            if pending[n] > 0 and not _pull(n):
                continue
            if pending[n] == 0:
                if _gated(n):
                    if n not in blocked:
                        blocked.append(n)
                    continue
                _emit(n)
        remaining = [n for n in remaining if n not in emitted]
        if remaining and len(order) < len(names):
            stack.append(remaining.pop(0))
            continue
        blocked[:] = [n for n in blocked if n not in emitted]
        if blocked:
            forced.update(blocked)
            stack.extend(reversed(sorted(blocked, key=natural_key)))
            blocked.clear()
            continue
        break

    if len(order) < len(names):
        order += [n for n in topo if n not in emitted]
    return order


def _canonical_primaries(
    motifs: Sequence[Motif],
    children: dict[str, list[str]],
    spine: set[str],
    weight: dict[str, int],
) -> dict[str, str]:
    out: dict[str, str] = {}
    for m in motifs:
        inverse = [{m.twin[y]: y for y in b} for b in m.blocks]
        for x in m.blocks[0]:
            if not children[x]:
                continue
            first = _primary_child(children[x], spine, weight)
            for inv in inverse[1:]:
                here, mate = inv.get(x), inv.get(first)
                if here is not None and mate is not None:
                    out[here] = mate
    return out


def _primary_child(kids: list[str], spine: set[str], weight: dict[str, int]) -> str:
    return min(kids, key=lambda c: (0 if c in spine else 1, -weight[c], natural_key(c)))


def _assign_lanes(
    order: list[str],
    children: dict[str, list[str]],
    weight: dict[str, int],
    spine: set[str],
    primary_of: Mapping[str, str] | None = None,
) -> tuple[dict[str, int], dict[tuple[str, str], int], int]:
    primary_of = primary_of or {}
    rows = {n: i for i, n in enumerate(order)}
    reserved: list[tuple[str, str] | None] = []
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
            primary = primary_of.get(n) or _primary_child(kids, spine, weight)
            if primary not in kids:
                primary = _primary_child(kids, spine, weight)
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
    shift: Mapping[str, tuple[str, str, str]] | None = None,
) -> tuple[dict[str, int], dict[tuple[str, str], int], int]:
    shift = shift or {}
    rows = {n: i for i, n in enumerate(order)}
    def _links(u: str, v: str, j: int) -> bool:
        return j == node_lane[u] == node_lane[v]

    inbound: dict[str, str] = {}
    for (u, v), j in edge_lane.items():
        if _links(u, v, j):
            inbound[v] = u

    strand_of: dict[str, int] = {}
    strands: list[list[int]] = []
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
        if _links(u, v, j):
            continue
        lo, hi = rows[u] + 1, rows[v] - 1
        if lo > hi:
            free_rails.add((u, v))
        else:
            items.append((lo, hi, ("rail", (u, v))))

    end_of_lane: list[int] = []
    new_node: dict[str, int] = {}
    new_edge: dict[tuple[str, str], int] = {}

    def _offset(x: str) -> int | None:
        info = shift.get(x)
        if info is None:
            return None
        _, head, ref = info
        if head not in new_node or ref not in new_node:
            return None
        return new_node[head] - new_node[ref]

    for lo, hi, item in sorted(items, key=lambda t: (t[0], t[1], _item_key(t[2]))):
        kind, key = item  # type: ignore[misc]
        want = None
        if kind == "strand":
            head = order[lo]
            was = node_lane[head]
            info, delta = shift.get(head), _offset(head)
            if info is not None and delta is not None and info[0] != head:
                base = new_node.get(info[0])
                want = None if base is None else base + delta
        else:
            was = edge_lane[key]  # type: ignore[index]
            u, v = key  # type: ignore[misc]
            iu, iv, delta = shift.get(u), shift.get(v), _offset(u)
            if iu is not None and iv is not None and delta is not None:
                mate = (iu[0], iv[0])
                if iu[1] == iv[1] and mate != (u, v):
                    base = new_edge.get(mate)
                    want = None if base is None else base + delta
        if want is not None and want < 0:
            want = None
        if want is not None and want >= len(end_of_lane):
            end_of_lane += [-1] * (want + 1 - len(end_of_lane))
        for cand in (want, was):
            if cand is not None and cand < len(end_of_lane) and end_of_lane[cand] < lo:
                lane = cand
                break
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
    kind, key = item
    return (kind, key) if kind == "strand" else (kind, natural_key(key[0]), natural_key(key[1]))


def _polyline(
    row_src: int, lane_src: int, row_dst: int, lane_dst: int, lane: int
) -> tuple[tuple[float, float], ...]:
    points: list[tuple[float, float]] = [(float(row_src), float(lane_src))]
    if lane != lane_src:
        points += [(row_src + 0.5, float(lane_src)), (row_src + 0.5, float(lane))]
    if lane != lane_dst:
        points += [(row_dst - 0.5, float(lane)), (row_dst - 0.5, float(lane_dst))]
    points.append((float(row_dst), float(lane_dst)))
    return tuple(points)


@dataclass(frozen=True)
class Metrics:
    rail_rows: int
    lanes: int
    longest_rail: int
    crossings: int
    modules: int
    contiguous: int
    module_spread: int
    repeats: int = 0
    congruent: int = 0
    marker_lanes: int = 0
    detours: int = 0

    @property
    def contiguity(self) -> float:
        return self.contiguous / self.modules if self.modules else 1.0

    @property
    def congruence(self) -> float:
        return self.congruent / self.repeats if self.repeats else 1.0

    def __str__(self) -> str:
        return (
            f"rail={self.rail_rows} lanes={self.lanes} longest={self.longest_rail}"
            f" markers={self.marker_lanes} crossings={self.crossings}"
            f" detours={self.detours}"
            f" modules={self.contiguous}/{self.modules} ({self.contiguity:.0%})"
            f" spread={self.module_spread}"
            f" repeats={self.congruent}/{self.repeats} ({self.congruence:.0%})"
        )


def measure(lay: Layout, motifs: Sequence[Motif] | None = None) -> Metrics:
    idx = lay.index
    forward = [e for e in lay.edges if not e.back]
    spans = [idx[e.dst].row - idx[e.src].row for e in forward]

    crossings = 0
    for row in range(max(lay.height - 1, 0)):
        triples = []
        for e in lay.gap_edges(row):
            src, dst = idx[e.src], idx[e.dst]
            triples.append((
                src.lane if src.row == row else e.lane,
                e.lane,
                dst.lane if dst.row == row + 1 else e.lane,
            ))
        for i, a in enumerate(triples):
            for b in triples[i + 1:]:
                crossings += sum(
                    1 for k in (0, 1) if (a[k] - b[k]) * (a[k + 1] - b[k + 1]) < 0
                )

    names = [n.name for n in lay.nodes]
    parents: dict[str, list[str]] = {n: [] for n in names}
    for e in forward:
        parents[e.dst].append(e.src)
    idom = dominators(names, parents)
    kids: dict[str, list[str]] = {n: [] for n in names}
    for n, d in idom.items():
        if d is not None:
            kids[d].append(n)

    rows = {n.name: n.row for n in lay.nodes}
    modules = contiguous = spread = 0
    for head in names:
        block = _dom_subtree(head, kids)
        if len(block) < 3:
            continue
        modules += 1
        span = [rows[n] for n in block]
        gap = max(span) - min(span) + 1 - len(block)
        spread += gap
        contiguous += gap == 0

    if motifs is None:
        motifs = repeat_motifs(lay)
    repeats, congruent = _congruence(lay, motifs)

    return Metrics(
        rail_rows=sum(spans),
        lanes=lay.width,
        longest_rail=max(spans, default=0),
        crossings=crossings,
        modules=modules,
        contiguous=contiguous,
        module_spread=spread,
        repeats=repeats,
        congruent=congruent,
        marker_lanes=sum(n.lane for n in lay.nodes),
        detours=sum(
            1
            for e in lay.edges
            if not e.back
            and not (
                min(lay[e.src].lane, lay[e.dst].lane)
                <= e.lane
                <= max(lay[e.src].lane, lay[e.dst].lane)
            )
        ),
    )


def dominators(names: list[str], parents: dict[str, list[str]]) -> dict[str, str | None]:
    top = "\0"
    rank = {top: -1}
    rank.update({n: i for i, n in enumerate(names)})
    idom: dict[str, str] = {top: top}

    def _meet(a: str, b: str) -> str:
        while a != b:
            while rank[a] > rank[b]:
                a = idom[a]
            while rank[b] > rank[a]:
                b = idom[b]
        return a

    for n in names:
        ps = [p for p in parents[n] if p in idom] or [top]
        common = ps[0]
        for p in ps[1:]:
            common = _meet(p, common)
        idom[n] = common
    return {n: (None if idom[n] == top else idom[n]) for n in names}


def _dom_subtree(head: str, kids: dict[str, list[str]]) -> list[str]:
    out, stack = [], [head]
    while stack:
        n = stack.pop()
        out.append(n)
        stack += kids[n]
    return out


@dataclass(frozen=True)
class Motif:
    heads: tuple[str, ...]
    blocks: tuple[frozenset[str], ...]
    twin: dict

    @property
    def nodes(self) -> frozenset[str]:
        return frozenset().union(*self.blocks)


def _kind_key(kind: Any) -> Any:
    try:
        hash(kind)
    except TypeError:
        return repr(kind)
    return kind


def _signatures(
    topo: list[str],
    children: dict[str, list[str]],
    kinds: Mapping[str, Any],
    depth: int,
) -> dict[str, int]:
    fan_in: dict[str, int] = dict.fromkeys(topo, 0)
    for n in topo:
        for c in children[n]:
            fan_in[c] += 1
    own = {n: (_kind_key(kinds.get(n)), fan_in[n]) for n in topo}
    ids: dict[tuple, int] = {}
    sig = {n: ids.setdefault((own[n],), len(ids)) for n in topo}
    for _ in range(depth):
        sig = {
            n: ids.setdefault(
                (own[n], tuple(sorted(sig[c] for c in children[n]))), len(ids)
            )
            for n in topo
        }
    return sig


def _pair_blocks(head_a, block_a, head_b, block_b, children, order_key):
    twin = {head_b: head_a}
    stack = [(head_a, head_b)]
    while stack:
        a, b = stack.pop()
        ka = sorted((c for c in children[a] if c in block_a), key=order_key)
        kb = sorted((c for c in children[b] if c in block_b), key=order_key)
        if len(ka) != len(kb):
            return None
        for x, y in zip(ka, kb):
            if y in twin:
                continue
            twin[y] = x
            stack.append((x, y))
    return twin if len(twin) == len(block_b) == len(block_a) else None


def _motifs(
    topo: list[str],
    children: dict[str, list[str]],
    sig: dict[str, int],
) -> tuple[Motif, ...]:
    desc: dict[str, set[str]] = {}
    for n in reversed(topo):
        below: set[str] = set()
        for c in children[n]:
            below.add(c)
            below |= desc[c]
        desc[n] = below

    groups: dict[int, list[str]] = {}
    for n in topo:
        groups.setdefault(sig[n], []).append(n)

    def _order_key(c: str):
        return (sig[c], natural_key(c))

    found: list[Motif] = []
    for members in groups.values():
        if len(members) < 2:
            continue
        members = sorted(members, key=natural_key)
        closed = [desc[m] | {m} for m in members]
        shared: set[str] = set()
        for i, a in enumerate(closed):
            for b in closed[i + 1:]:
                shared |= a & b
        blocks = [frozenset(c - shared) for c in closed]
        if any(m not in b for m, b in zip(members, blocks)):
            continue
        if len(blocks[0]) < 2 or len({len(b) for b in blocks}) != 1:
            continue
        twin: dict[str, str] = {}
        for m, b in zip(members, blocks):
            pairing = _pair_blocks(members[0], blocks[0], m, b, children, _order_key)
            if pairing is None:
                twin = {}
                break
            twin.update(pairing)
        if not twin:
            continue
        found.append(Motif(heads=tuple(members), blocks=tuple(blocks), twin=twin))

    found.sort(
        key=lambda m: (-len(m.blocks[0]), -len(m.heads), natural_key(m.heads[0]))
    )
    kept: list[Motif] = []
    covered: set[str] = set()
    for m in found:
        if m.nodes & covered:
            continue
        kept.append(m)
        covered |= m.nodes
    return tuple(kept)


def repeat_motifs(lay: Layout, depth: int = _SIGNATURE_DEPTH) -> tuple[Motif, ...]:
    names = [n.name for n in lay.nodes]
    kinds = {n.name: n.kind for n in lay.nodes}
    children: dict[str, list[str]] = {n: [] for n in names}
    for e in lay.edges:
        if not e.back:
            children[e.src].append(e.dst)
    for n in names:
        children[n].sort(key=natural_key)
    return _motifs(names, children, _signatures(names, children, kinds, depth))


def _congruence(lay: Layout, motifs: Sequence[Motif]) -> tuple[int, int]:
    rows = {n.name: n.row for n in lay.nodes}
    lanes = {n.name: n.lane for n in lay.nodes}
    total = matched = 0
    for m in motifs:
        seen: dict[frozenset, int] = {}
        for head, block in zip(m.heads, m.blocks):
            shape = frozenset(
                (rows[x] - rows[head], lanes[x] - lanes[head], m.twin[x])
                for x in block
            )
            seen[shape] = seen.get(shape, 0) + 1
        total += len(m.heads)
        matched += max(seen.values())
    return total, matched
