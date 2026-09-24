"""A DAG laid out on a grid: one node per row, one column per node's run.

The whole layout is two integers per node, its row and its column. Every
line in a drawing is derived from them: a node's run holds its column from
the band above it down to the band above its last child, and every edge into
a node arrives along one bar in that node's band. Positions along a column
are half-rows: node row r sits at 2r and its band at 2r - 1.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Sequence

__all__ = [
    "Run", "Feed", "Bar", "Layout", "Metrics", "Motif",
    "layout", "measure", "dominators", "natural_key", "repeat_motifs",
]

_DIGITS = re.compile(r"(\d+)")
_SIGNATURE_DEPTH = 3


def natural_key(name: str) -> tuple:
    return tuple(
        (int(part), "") if part.isdigit() else (-1, part)
        for part in _DIGITS.split(name)
        if part != ""
    )


@dataclass(frozen=True)
class Run:
    """The column a node's output travels down, over half-rows [top, bottom)."""

    node: str
    col: int
    top: int
    bottom: int

    @property
    def length(self) -> int:
        return self.bottom - self.top


@dataclass(frozen=True)
class Feed:
    """One parent's arrival at a bar. `turns` when the parent's run ends
    here and bends into the bar; otherwise the run carries on and branches."""

    src: str
    col: int
    turns: bool


@dataclass(frozen=True)
class Bar:
    node: str
    col: int
    band: int
    feeds: tuple[Feed, ...]

    @property
    def span(self) -> tuple[int, int]:
        cols = [self.col] + [f.col for f in self.feeds]
        return min(cols), max(cols)


@dataclass(frozen=True)
class Layout:
    order: tuple[str, ...]
    col: Mapping[str, int]
    edges: tuple[tuple[str, str], ...]
    back: tuple[tuple[str, str], ...] = ()
    width: int = 0
    optimal: bool = False
    row: dict[str, int] = field(init=False, repr=False, compare=False)
    children: dict[str, list[str]] = field(init=False, repr=False, compare=False)
    parents: dict[str, list[str]] = field(init=False, repr=False, compare=False)
    runs: dict[str, Run] = field(init=False, repr=False, compare=False)
    bars: dict[str, Bar] = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        row = {n: i for i, n in enumerate(self.order)}
        children: dict[str, list[str]] = {n: [] for n in self.order}
        parents: dict[str, list[str]] = {n: [] for n in self.order}
        for src, dst in self.edges:
            assert row[src] < row[dst], f"edge {src} -> {dst} runs upward"
            children[src].append(dst)
            parents[dst].append(src)
        for kids in children.values():
            kids.sort(key=row.__getitem__)
        for ups in parents.values():
            ups.sort(key=row.__getitem__)

        runs = {}
        for n, r in row.items():
            top = 2 * r - 1 if parents[n] else 2 * r
            bottom = 2 * row[children[n][-1]] - 1 if children[n] else 2 * r + 1
            runs[n] = Run(n, self.col[n], top, bottom)
        by_col: dict[int, list[Run]] = {}
        for run in runs.values():
            assert 0 <= run.col < self.width, f"{run.node} outside the grid"
            by_col.setdefault(run.col, []).append(run)
        for held in by_col.values():
            held.sort(key=lambda u: u.top)
            for a, b in zip(held, held[1:]):
                assert a.bottom <= b.top, f"{a.node} and {b.node} share a column"

        bars = {
            n: Bar(n, self.col[n], 2 * row[n] - 1, tuple(
                Feed(p, self.col[p], children[p][-1] == n) for p in parents[n]
            ))
            for n in self.order
            if parents[n]
        }
        for name, value in (("row", row), ("children", children), ("parents", parents),
                            ("runs", runs), ("bars", bars)):
            object.__setattr__(self, name, value)

    @property
    def height(self) -> int:
        return len(self.order)

    def live(self, half_row: int) -> list[Run]:
        return [u for u in self.runs.values() if u.top <= half_row < u.bottom]

    def route(self, src: str, dst: str) -> tuple[tuple[int, int], ...]:
        """(half-row, column) corners of one edge: down the source's run to
        the target's band, along the bar, and down into the target."""
        a, b = self.col[src], self.col[dst]
        band = 2 * self.row[dst] - 1
        points = [(2 * self.row[src], a)]
        if a != b:
            points += [(band, a), (band, b)]
        return tuple(points + [(2 * self.row[dst], b)])


def layout(
    nodes: Iterable[str],
    edges: Iterable[tuple[str, str]],
    order: Sequence[str] | None = None,
) -> Layout:
    names_seen: dict[str, None] = dict.fromkeys(nodes)
    pairs: list[tuple[str, str]] = []
    for src, dst in dict.fromkeys(edges):
        names_seen.setdefault(src)
        names_seen.setdefault(dst)
        pairs.append((src, dst))
    names = sorted(names_seen, key=natural_key)
    if not names:
        return Layout(order=(), col={}, edges=())

    children: dict[str, list[str]] = {n: [] for n in names}
    for src, dst in pairs:
        children[src].append(dst)
    for n in names:
        children[n].sort(key=natural_key)
    back = _break_cycles(names, children)
    children = {n: [c for c in children[n] if (n, c) not in back] for n in names}
    parents: dict[str, list[str]] = {n: [] for n in names}
    for n in names:
        for c in children[n]:
            parents[c].append(n)

    rows = _given_order(order, names, children)
    optimal = False
    if rows is None:
        topo = _topological(names, children, parents)
        sig = _signatures(topo, children, _SIGNATURE_DEPTH)
        rows, optimal = _order_rows(names, children, parents, sig)
        rows = _congruent(rows, _motifs(topo, children, sig), children)

    col, width = _pack(rows, children, parents)
    at = {n: i for i, n in enumerate(rows)}
    forward = sorted(
        ((p, c) for p in names for c in children[p]), key=lambda e: (at[e[0]], at[e[1]])
    )
    return Layout(
        order=tuple(rows), col=col, edges=tuple(forward),
        back=tuple(e for e in pairs if e in back), width=width, optimal=optimal,
    )


def _given_order(
    order: Sequence[str] | None,
    names: list[str],
    children: dict[str, list[str]],
) -> list[str] | None:
    if order is None:
        return None
    rows = list(order)
    if len(rows) != len(names) or set(rows) != set(names):
        return None
    at = {n: i for i, n in enumerate(rows)}
    for n in rows:
        if any(at[c] <= at[n] for c in children[n]):
            return None
    return rows


def _pack(
    order: list[str], children: dict[str, list[str]], parents: dict[str, list[str]]
) -> tuple[dict[str, int], int]:
    """Give each run a column, as few columns as any packing could.

    Runs are intervals placed in order of their tops, so whatever free column
    each takes, the width stays the most runs ever live at once. That leaves
    the choice free for readability: a parent's column freed at this node's
    band first, so the line drops straight in; then the column that keeps the
    runs live alongside longest on the left; then the one nearest its rank by
    length among every run it will overlap, since runs placed later cannot be
    seen yet; then the rightmost, nearest the labels.
    """
    row = {n: i for i, n in enumerate(order)}
    span = {}
    for n in order:
        r = row[n]
        top = 2 * r - 1 if parents[n] else 2 * r
        bottom = 2 * max(row[c] for c in children[n]) - 1 if children[n] else 2 * r + 1
        span[n] = (top, bottom)
    live = width = 0
    for _, step in sorted(
        [(t, 1) for t, _ in span.values()] + [(b, -1) for _, b in span.values()]
    ):
        live += step
        width = max(width, live)

    def longer(a: str, b: str) -> bool:
        la, lb = span[a][1] - span[a][0], span[b][1] - span[b][0]
        return la > lb or (la == lb and span[a][0] < span[b][0])

    target = {
        n: sum(
            1 for m in order
            if span[m][0] < span[n][1] and span[n][0] < span[m][1] and longer(m, n)
        )
        for n in order
    }
    col: dict[str, int] = {}
    held: dict[int, str] = {}
    for n in order:
        top, bottom = span[n]
        drops = set()
        for c, m in list(held.items()):
            if span[m][1] <= top:
                del held[c]
                if m in parents[n]:
                    drops.add(c)
        length = bottom - top

        def disorder(c: int) -> int:
            out = 0
            for d, m in held.items():
                other = span[m][1] - span[m][0]
                out += (d < c and other < length) or (d > c and other > length)
            return out

        free = [c for c in range(width) if c not in held]
        pick = min(
            free, key=lambda c: (c not in drops, disorder(c), abs(c - target[n]), -c)
        )
        col[n] = pick
        held[pick] = n
    return col, width


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


# Total edge length is the sum over the gaps between rows of the edges
# crossing each gap, and the edges crossing a gap depend only on the set of
# nodes above it. Interleaving two weakly connected components only stretches
# their edges, so each is solved alone and they are stacked smallest first.

_ORDER_BUDGET = 60_000


def _order_rows(
    names: list[str],
    children: dict[str, list[str]],
    parents: dict[str, list[str]],
    sig: Mapping[str, int],
) -> tuple[list[str], bool]:
    comps = _components(names, children, parents)
    comps.sort(key=lambda c: (len(c), sorted(sig[n] for n in c), natural_key(c[0])))
    order: list[str] = []
    optimal = True
    for comp in comps:
        seq, proven = _solve_block(comp, children, parents)
        order += seq
        optimal = optimal and proven
    return order, optimal


def _components(
    names: list[str], children: dict[str, list[str]], parents: dict[str, list[str]]
) -> list[list[str]]:
    rank = {n: i for i, n in enumerate(names)}
    seen: set[str] = set()
    out = []
    for n in names:
        if n in seen:
            continue
        seen.add(n)
        members, stack = [], [n]
        while stack:
            cur = stack.pop()
            members.append(cur)
            for nxt in children[cur] + parents[cur]:
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        out.append(sorted(members, key=rank.__getitem__))
    return out


def _solve_block(
    block: list[str],
    children: dict[str, list[str]],
    parents: dict[str, list[str]],
    budget: int = _ORDER_BUDGET,
) -> tuple[list[str], bool]:
    """The order of one component with the least total edge length.

    Total length is the sum over gaps of the edges crossing them, and the
    edges crossing a gap depend only on the set placed above it, so orders
    reaching the same set merge: a DP over placed sets. A state's bound adds
    one row per edge not yet begun, and gives the targets of edges already
    open distinct rows, busiest first; a target still waiting on a parent
    cannot take the next row. States above a known order's length go safely.
    The proof is lost only when a layer still exceeds its budget after that,
    so a first pass at a sixteenth of the budget sets a tighter ceiling for
    the second. Among equal lengths the order first by name wins.
    """
    k = len(block)
    if k == 1:
        return list(block), True
    at = {n: i for i, n in enumerate(block)}
    ups = [sum(1 << at[p] for p in parents[n]) for n in block]
    kids = [[at[c] for c in children[n]] for n in block]
    indeg = [len(parents[n]) for n in block]
    outdeg = [len(children[n]) for n in block]
    top = max(indeg)
    roots = sum(1 << i for i in range(k) if indeg[i] == 0)
    waiting = [(d, sum(1 << i for i in range(k) if indeg[i] == d)) for d in range(top, 0, -1)]

    def bound(hist, avail, unbegun):
        total, rank = 0, 1
        for c in range(top, 0, -1):
            m = hist[c]
            if m:
                total += c * (m * rank + m * (m - 1) // 2)
                rank += m
        first = next((d for d, m in waiting if avail & m), 0)
        if first:
            total -= first * sum(hist[first:]) + sum(c * hist[c] for c in range(1, first))
        return total + unbegun

    def step(placed, avail, hist, v):
        nxt = placed | (1 << v)
        avail &= ~(1 << v)
        hist = list(hist)
        hist[indeg[v]] -= 1
        for c in kids[v]:
            x = (ups[c] & placed).bit_count()
            hist[x] -= 1
            hist[x + 1] += 1
            if ups[c] & ~nxt == 0:
                avail |= 1 << c
        hist[0] = 0
        return nxt, avail, tuple(hist)

    start = (0, 0, 0, roots, (0,) * (top + 2), sum(outdeg))

    def run(width, ceiling):
        states = [start]
        back: list[dict[int, tuple[int, int]]] = []
        proven = True
        for _ in range(k):
            reached: dict[int, tuple] = {}
            for rank, (placed, cost, cut, avail, hist, unbegun) in enumerate(states):
                rest = avail
                while rest:
                    low = rest & -rest
                    rest ^= low
                    v = low.bit_length() - 1
                    nxt = placed | low
                    c = cut + outdeg[v] - indeg[v]
                    cur = reached.get(nxt)
                    if cur is None or cost + c < cur[1]:
                        reached[nxt] = ((rank, v), cost + c, c, rank, v)
            layer = []
            for nxt, (key, cost, cut, rank, v) in reached.items():
                placed, _, _, avail, hist, unbegun = states[rank]
                _, avail, hist = step(placed, avail, hist, v)
                unbegun -= outdeg[v]
                score = cost + bound(hist, avail, unbegun)
                if score <= ceiling:
                    layer.append((key, score, (nxt, cost, cut, avail, hist, unbegun), placed, v))
            layer.sort(key=lambda item: item[0])
            if len(layer) > width:
                proven = False
                keep = sorted(range(len(layer)), key=lambda i: (layer[i][1], i))[:width]
                layer = [layer[i] for i in sorted(keep)]
            back.append({s[0]: (placed, v) for _, _, s, placed, v in layer})
            states = [s for _, _, s, _, _ in layer]
        if not states:
            return None, None, False
        seq, placed = [], states[0][0]
        for t in range(k - 1, -1, -1):
            placed, v = back[t][placed]
            seq.append(v)
        seq.reverse()
        return seq, states[0][1], proven

    greedy, placed, avail, hist, ceiling, cut = [], 0, roots, start[4], 0, 0
    for _ in range(k):
        v = max(_bits(avail), key=lambda i: (indeg[i] - outdeg[i], -i))
        greedy.append(v)
        cut += outdeg[v] - indeg[v]
        ceiling += cut
        placed, avail, hist = step(placed, avail, hist, v)

    best, proven = greedy, False
    width = max(8, budget // k)
    for width in (max(1, width // 16), width):
        seq, cost, done = run(width, ceiling)
        if seq is not None and (cost < ceiling or done):
            best, ceiling = seq, cost
        if done:
            proven = True
            break
    return [block[i] for i in best], proven


def _bits(mask: int):
    while mask:
        low = mask & -mask
        yield low.bit_length() - 1
        mask ^= low


def _congruent(
    order: list[str], motifs: Sequence[Motif], children: dict[str, list[str]]
) -> list[str]:
    """Give repeated blocks one internal order wherever that costs no length.

    Each instance's order, read through the twin map, is replayed onto the
    rows every other instance already holds; a replay is kept only if it
    respects precedence and does not lengthen the total. The instance whose
    order the most others can adopt sets the pattern.
    """
    edges = [(p, c) for p in order for c in children[p]]

    def length(r):
        return sum(r[c] - r[p] for p, c in edges)

    def valid(r):
        return all(r[p] < r[c] for p, c in edges)

    row = {n: i for i, n in enumerate(order)}
    for m in motifs:
        best = None
        for pi, pattern in enumerate(m.blocks):
            r = dict(row)
            seq = [m.twin[x] for x in sorted(pattern, key=r.__getitem__)]
            for qi, block in enumerate(m.blocks):
                if qi == pi:
                    continue
                inverse = {m.twin[y]: y for y in block}
                slots = sorted(r[y] for y in block)
                trial = dict(r)
                for slot, canon in zip(slots, seq):
                    trial[inverse[canon]] = slot
                if valid(trial) and length(trial) <= length(r):
                    r = trial
            same = sum(
                [m.twin[x] for x in sorted(b, key=r.__getitem__)] == seq for b in m.blocks
            )
            key = (-same, length(r), pi)
            if best is None or key < best[0]:
                best = (key, r)
        row = best[1]
    return sorted(order, key=row.__getitem__)


@dataclass(frozen=True)
class Metrics:
    length: int
    width: int
    crossings: int
    off_right: int
    drops: int
    disorder: int
    repeats: int = 0
    congruent: int = 0
    optimal: bool = False

    @property
    def congruence(self) -> float:
        return self.congruent / self.repeats if self.repeats else 1.0

    def __str__(self) -> str:
        return (
            f"length={self.length} width={self.width} crossings={self.crossings}"
            f" off_right={self.off_right} drops={self.drops} disorder={self.disorder}"
            f" repeats={self.congruent}/{self.repeats} optimal={self.optimal}"
        )


def measure(lay: Layout, motifs: Sequence[Motif] | None = None) -> Metrics:
    """`crossings` counts runs a bar passes over; `disorder` counts pairs of
    runs live together with the shorter on the left; `drops` counts parents
    whose run falls straight into their last child."""
    crossings = drops = 0
    for bar in lay.bars.values():
        lo, hi = bar.span
        feeding = {f.src for f in bar.feeds}
        crossings += sum(
            1 for u in lay.live(bar.band)
            if lo < u.col < hi and u.node != bar.node and u.node not in feeding
        )
        drops += sum(1 for f in bar.feeds if f.turns and f.col == bar.col)

    runs = sorted(lay.runs.values(), key=lambda u: u.top)
    disorder = 0
    for i, a in enumerate(runs):
        for b in runs[i + 1:]:
            if b.top >= a.bottom:
                break
            left, right = (a, b) if a.col < b.col else (b, a)
            disorder += left.length < right.length

    if motifs is None:
        motifs = repeat_motifs(lay)
    repeats, congruent = _congruence(lay, motifs)
    return Metrics(
        length=sum(lay.row[c] - lay.row[p] for p, c in lay.edges),
        width=lay.width,
        crossings=crossings,
        off_right=sum(lay.width - 1 - c for c in lay.col.values()),
        drops=drops,
        disorder=disorder,
        repeats=repeats,
        congruent=congruent,
        optimal=lay.optimal,
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


@dataclass(frozen=True)
class Motif:
    heads: tuple[str, ...]
    blocks: tuple[frozenset[str], ...]
    twin: dict

    @property
    def nodes(self) -> frozenset[str]:
        return frozenset().union(*self.blocks)


def _signatures(topo: list[str], children: dict[str, list[str]], depth: int) -> dict[str, int]:
    fan_in: dict[str, int] = dict.fromkeys(topo, 0)
    for n in topo:
        for c in children[n]:
            fan_in[c] += 1
    ids: dict[tuple, int] = {}
    sig = {n: ids.setdefault((fan_in[n],), len(ids)) for n in topo}
    for _ in range(depth):
        sig = {
            n: ids.setdefault(
                (fan_in[n], tuple(sorted(sig[c] for c in children[n]))), len(ids)
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
    names = list(lay.order)
    children = {n: sorted(lay.children[n], key=natural_key) for n in names}
    return _motifs(names, children, _signatures(names, children, depth))


def _congruence(lay: Layout, motifs: Sequence[Motif]) -> tuple[int, int]:
    total = matched = 0
    for m in motifs:
        seen: dict[frozenset, int] = {}
        for head, block in zip(m.heads, m.blocks):
            shape = frozenset(
                (lay.row[x] - lay.row[head], lay.col[x] - lay.col[head], m.twin[x])
                for x in block
            )
            seen[shape] = seen.get(shape, 0) + 1
        total += len(m.heads)
        matched += max(seen.values())
    return total, matched
