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

import numpy as np

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
    optimal_columns: bool = False
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
    blocks: Iterable[Sequence[str]] | None = None,
) -> Layout:
    """`blocks` partitions the nodes into runs of consecutive rows, each led
    by its first member; a node in no block is a block of its own."""
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

    units = _units(blocks, names, children, parents)
    rows = _given_order(order, names, children, units)
    optimal = False
    if rows is None:
        topo = _topological(names, children, parents)
        sig = _signatures(topo, children, _SIGNATURE_DEPTH)
        rows, optimal = _order_rows(names, children, parents, sig, units)
        rows = _furthest_first(rows, children, parents, units)
        rows = _congruent(rows, _motifs(topo, children, sig), children, units)

    col, width, packed = _columns(rows, children, parents)
    at = {n: i for i, n in enumerate(rows)}
    forward = sorted(
        ((p, c) for p in names for c in children[p]), key=lambda e: (at[e[0]], at[e[1]])
    )
    return Layout(
        order=tuple(rows), col=col, edges=tuple(forward),
        back=tuple(e for e in pairs if e in back), width=width, optimal=optimal,
        optimal_columns=packed,
    )


def _given_order(
    order: Sequence[str] | None,
    names: list[str],
    children: dict[str, list[str]],
    units: list[tuple[str, ...]],
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
    if not _keeps_units(at, units):
        return None
    return rows


def _furthest_first(
    rows: list[str],
    children: dict[str, list[str]],
    parents: dict[str, list[str]],
    units: list[tuple[str, ...]],
) -> list[str]:
    """Inside a block, members of equal weight cost the same in any order, so
    the one used furthest down goes first. Their runs then step down and
    left in a diagonal instead of in name order."""
    at = {n: i for i, n in enumerate(rows)}
    weight = {n: len(parents[n]) - len(children[n]) for n in rows}

    def reach(n):
        return max((at[c] for c in children[n]), default=-1)

    rows = list(rows)
    for unit in units:
        start = min(at[n] for n in unit)
        span = rows[start:start + len(unit)]
        i = 0
        while i < len(span):
            j = i
            while j < len(span) and weight[span[j]] == weight[span[i]]:
                j += 1
            run = set(span[i:j])
            if not any(c in run for n in run for c in children[n]):
                span[i:j] = sorted(span[i:j], key=lambda n: (-reach(n), natural_key(n)))
            i = j
        rows[start:start + len(unit)] = span
    return rows


def _keeps_units(row: Mapping[str, int], units: list[tuple[str, ...]]) -> bool:
    for unit in units:
        first = row[unit[0]]
        if sorted(row[n] - first for n in unit) != list(range(len(unit))):
            return False
    return True


def _units(
    blocks: Iterable[Sequence[str]] | None,
    names: list[str],
    children: dict[str, list[str]],
    parents: dict[str, list[str]],
) -> list[tuple[str, ...]]:
    """The blocks as a partition of every node, each in its row order.

    Total length is the sum over nodes of (in-degree - out-degree) * row. A
    block's rows are consecutive, so its start adds the same to every order
    of its members, and the rest is least with the heaviest member first
    (rearrangement): the order inside a block does not depend on where the
    block goes. Internal edges still win over weight. A block that an order
    of whole blocks could not honour, because a path leaves it and comes
    back, is dissolved.
    """
    known = set(names)
    owner: dict[str, int] = {}
    groups: list[list[str]] = []
    for block in blocks or ():
        members = [n for n in dict.fromkeys(block) if n in known and n not in owner]
        if len(members) > 1:
            for n in members:
                owner[n] = len(groups)
            groups.append(members)
    weight = {n: len(parents[n]) - len(children[n]) for n in names}

    def arrange(members: list[str]) -> tuple[str, ...]:
        inside = set(members)
        pending = sorted(members, key=lambda n: (-weight[n], natural_key(n)))
        if not inside & set(parents[members[0]]):
            pending.remove(members[0])
            pending.insert(0, members[0])
        out: list[str] = []
        while pending:
            n = next(m for m in pending if all(p in out or p not in inside for p in parents[m]))
            pending.remove(n)
            out.append(n)
        return tuple(out)

    while True:
        unit_of = {n: owner.get(n, n) for n in names}
        succ: dict = {u: set() for u in unit_of.values()}
        indeg = dict.fromkeys(succ, 0)
        for n in names:
            for c in children[n]:
                a, b = unit_of[n], unit_of[c]
                if a != b and b not in succ[a]:
                    succ[a].add(b)
                    indeg[b] += 1
        ready = [u for u, d in indeg.items() if d == 0]
        while ready:
            u = ready.pop()
            del indeg[u]
            for v in succ[u]:
                indeg[v] -= 1
                if indeg[v] == 0:
                    ready.append(v)
        left = set(indeg)
        sinks = [u for u in left if not succ[u] & left]
        while sinks:
            left -= set(sinks)
            sinks = [u for u in left if not succ[u] & left]
        stuck = {u for u in left if isinstance(u, int)}
        if not stuck:
            break
        for n in [n for n, g in owner.items() if g in stuck]:
            del owner[n]

    kept = sorted({g for g in owner.values()})
    arranged = {g: arrange(groups[g]) for g in kept}
    return [arranged[owner[n]] if n in owner else (n,) for n in names
            if n not in owner or arranged[owner[n]][0] == n]


# Columns are chosen by a sweep down the rows whose state is the column of
# every live run: the exact algorithm for storyline crossings (Kostitsyna and
# Nöllenburg, GD 2015), fixed-parameter in the width. A move's cost depends
# only on the state and the move, so states holding one arrangement merge.
# A layer's states are rows of one array, so each layer is a few array
# operations whatever the budget.

_COLUMN_BUDGET = 400_000


def _columns(
    order: list[str],
    children: dict[str, list[str]],
    parents: dict[str, list[str]],
    budget: int = _COLUMN_BUDGET,
) -> tuple[dict[str, int], int, bool]:
    """Give each run a column in stages: as few columns as any packing could,
    then the fewest crossings, then the least horizontal travel summed over
    edges, then the columns nearest the labels. A parent whose run goes on
    below the bar is crossed like any other run: only its last child's bar
    may pass it.

    Every node takes the column of a parent whose run ends at it, so a run
    never bends into its last child when it could fall straight in. That
    column is always free, since the parent frees it at the very half-row
    the node starts, so the rule never costs width.

    Runs are placed in order of their tops, so whatever free column each
    takes, the width stays the most runs ever live at once. A layer over its
    budget first drops the states that cannot beat a known assignment, then
    keeps the best by cost plus a bound on the bars still to come: a node
    with two parents already placed will cross every run between them that
    outlives its band. The proof is lost only when a layer is cut; a first
    pass at a sixteenth of the budget sets the ceiling for the second. A cut
    beam misses cheap repairs, so an unproven assignment is then polished
    by moving single runs.
    """
    n = len(order)
    if n == 0:
        return {}, 0, True
    row = {v: i for i, v in enumerate(order)}
    pars = [np.array(sorted(row[p] for p in parents[v]), dtype=np.int32) for v in order]
    top = np.array([2 * i - 1 if len(pars[i]) else 2 * i for i in range(n)])
    bottom = np.array([
        2 * max(row[c] for c in children[v]) - 1 if children[v] else 2 * i + 1
        for i, v in enumerate(order)
    ])
    ending = [[] for _ in range(n)]
    for i, v in enumerate(order):
        if children[v]:
            ending[max(row[c] for c in children[v])].append(i)
    ending = [np.array(e, dtype=np.int32) for e in ending]
    live = width = 0
    for _, delta in sorted([(t, 1) for t in top.tolist()] + [(b, -1) for b in bottom.tolist()]):
        live += delta
        width = max(width, live)
    # a sentinel past the last node answers every lookup of an empty column
    bottom_of = np.append(bottom, -1)
    columns = np.arange(width)

    seen = [0] * n
    waiting: list[list[int]] = []
    open_now: set[int] = set()
    for i in range(n):
        for c in children[order[i]]:
            j = row[c]
            seen[j] += 1
            if seen[j] == 2:
                open_now.add(j)
        open_now.discard(i)
        waiting.append(sorted(open_now))

    def positions(states):
        at = np.zeros((len(states), n + 1), dtype=np.int64)
        at[np.arange(len(states))[:, None], states] = columns
        return at

    def bound(i, states):
        at = positions(states)
        ends = bottom_of[states]
        cross = np.zeros(len(states), dtype=np.int64)
        bar = np.zeros(len(states), dtype=np.int64)
        for j in waiting[i]:
            ups = at[:, [p for p in pars[j] if p <= i]]
            lo, hi = ups.min(axis=1), ups.max(axis=1)
            bar += hi - lo
            between = (columns > lo[:, None]) & (columns < hi[:, None])
            outlives = ends > 2 * j - 1
            cross += (between & outlives).sum(axis=1)
        return cross, bar

    def at_most(score, ceiling):
        below = np.zeros(len(score), dtype=bool)
        equal = np.ones(len(score), dtype=bool)
        for k, c in enumerate(ceiling):
            below |= equal & (score[:, k] < c)
            equal &= score[:, k] == c
        return below | equal

    def run(beam, ceiling):
        states = np.full((1, width), n, dtype=np.int32)
        cost = np.zeros((1, 3), dtype=np.int64)
        back: list[tuple[np.ndarray, np.ndarray]] = []
        proven = True
        for i in range(n):
            cols = np.where(bottom_of[states] <= top[i], n, states)
            empty = cols == n
            ups = positions(states)[:, pars[i]] if len(pars[i]) else None
            prefix = np.zeros((len(cols), width + 1), dtype=np.int64)
            np.cumsum(~empty, axis=1, out=prefix[:, 1:])
            allowed = np.isin(states, ending[i]) if len(ending[i]) else empty

            s, c = np.nonzero(allowed)
            step = np.empty((len(s), 3), dtype=np.int64)
            if ups is not None:
                lo = np.minimum(ups.min(axis=1)[s], c)
                hi = np.maximum(ups.max(axis=1)[s], c)
                step[:, 0] = np.where(hi > lo + 1, prefix[s, hi] - prefix[s, np.minimum(lo + 1, width)], 0)
                step[:, 1] = np.abs(ups[s] - c[:, None]).sum(axis=1)
            else:
                step[:, 0] = step[:, 1] = 0
            step[:, 2] = width - 1 - c
            step += cost[s]
            nxt = cols[s]
            nxt[np.arange(len(s)), c] = i

            rank = np.lexsort(step.T[::-1])
            nxt, step, s, c = nxt[rank], step[rank], s[rank], c[rank]
            keys = np.ascontiguousarray(nxt).view(np.dtype((np.void, nxt.itemsize * width))).ravel()
            _, first = np.unique(keys, return_index=True)
            first.sort()
            nxt, step, s, c = nxt[first], step[first], s[first], c[first]

            if len(nxt) > beam:
                cross, bar = bound(i, nxt)
                score = step.copy()
                score[:, 0] += cross
                score[:, 1] += bar
                if ceiling is not None:
                    keep = at_most(score, ceiling)
                    nxt, step, s, c, score = nxt[keep], step[keep], s[keep], c[keep], score[keep]
                if len(nxt) > beam:
                    proven = False
                    best = np.lexsort(score.T[::-1])[:beam]
                    best.sort()
                    nxt, step, s, c = nxt[best], step[best], s[best], c[best]
            if not len(nxt):
                return None, None, False
            back.append((s, c))
            states, cost = nxt, step

        final = int(np.lexsort(cost.T[::-1])[0])
        col = [0] * n
        k = final
        for i in range(n - 1, -1, -1):
            s, c = back[i]
            col[i] = int(c[k])
            k = int(s[k])
        return col, tuple(int(x) for x in cost[final]), proven

    beam = max(8, budget // (n * width))
    best, cost, proven = run(max(1, beam // 16), None)
    if not proven:
        again, better, done = run(beam, cost)
        if again is not None and (better < cost or done):
            best = again
        proven = done
    if not proven:
        best = _improve(top.tolist(), bottom.tolist(), [p.tolist() for p in pars],
                        [e.tolist() for e in ending], best, width)
    return {v: best[i] for i, v in enumerate(order)}, width, proven


_IMPROVE_BUDGET = 20_000


def _improve(
    top: list[int],
    bottom: list[int],
    pars: list[list[int]],
    ending: list[list[int]],
    col: list[int],
    width: int,
    budget: int = _IMPROVE_BUDGET,
) -> list[int]:
    """Move a run, or swap two, while that lowers crossings and then travel.
    A run moves with the stack of runs falling straight out of it, and its
    head only to a column one of its ending parents holds, so every move
    keeps the straight rule. A move only touches the bars inside the moved
    runs' lives, so each trial rescores just those."""
    n = len(col)
    col = list(col)
    last = [-1] * n
    for j in range(n):
        for p in ending[j]:
            last[p] = j
    lo_h = min(top)
    grid = [[-1] * width for _ in range(max(bottom) - lo_h + 1)]
    for v in range(n):
        for h in range(top[v], bottom[v]):
            grid[h - lo_h][col[v]] = v
    overlaps: list[list[int]] = [[] for _ in range(n)]
    by_top = sorted(range(n), key=top.__getitem__)
    for a, u in enumerate(by_top):
        for v in by_top[a + 1:]:
            if top[v] >= bottom[u]:
                break
            overlaps[u].append(v)
            overlaps[v].append(u)

    def stack(v):
        out = [v]
        while last[out[-1]] >= 0 and col[last[out[-1]]] == col[v]:
            out.append(last[out[-1]])
        return out

    def straight(v):
        return not ending[v] or any(col[p] == col[v] for p in ending[v])

    def bar(j):
        if not pars[j]:
            return 0, 0
        cols = [col[j]] + [col[p] for p in pars[j]]
        lo, hi = min(cols), max(cols)
        cells = grid[2 * j - 1 - lo_h]
        cross = sum(1 for k in range(lo + 1, hi) if cells[k] >= 0 and cells[k] != j)
        return cross, sum(abs(col[p] - col[j]) for p in pars[j])

    def score(vs):
        js = set()
        for v in vs:
            js.update(range((top[v] + 1) // 2, (bottom[v] + 1) // 2 + 1))
        cross = h = 0
        for j in js:
            if j < n:
                x, y = bar(j)
                cross += x
                h += y
        return cross, h, -sum(col[v] for v in vs)

    def lift(vs):
        for v in vs:
            for h in range(top[v], bottom[v]):
                grid[h - lo_h][col[v]] = -1

    def drop(vs, c):
        for v in vs:
            col[v] = c
            for h in range(top[v], bottom[v]):
                grid[h - lo_h][c] = v

    def free(vs, c):
        return all(grid[h - lo_h][c] == -1 for v in vs for h in range(top[v], bottom[v]))

    trials = 0
    improved = True
    while improved and trials < budget:
        improved = False
        for v in range(n):
            if trials >= budget:
                break
            vs = stack(v)
            for c in range(width):
                was = col[v]
                if c == was:
                    continue
                trials += 1
                before = score(vs)
                lift(vs)
                if free(vs, c):
                    drop(vs, c)
                    if straight(v) and score(vs) < before:
                        improved = True
                        break
                    lift(vs)
                drop(vs, was)
            for u in overlaps[v]:
                vs = stack(v)
                if u < v or col[u] == col[v] or u in vs:
                    continue
                us = stack(u)
                if v in us:
                    continue
                a, b = col[v], col[u]
                trials += 1
                both = vs + us
                before = score(both)
                lift(both)
                if free(vs, b) and free(us, a):
                    drop(vs, b)
                    drop(us, a)
                    if straight(v) and straight(u) and score(both) < before:
                        improved = True
                        continue
                    lift(both)
                drop(vs, a)
                drop(us, b)
    return col


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
    units: list[tuple[str, ...]],
) -> tuple[list[str], bool]:
    unit_of = {n: u for u in units for n in u}
    comps = _components(names, children, parents, unit_of)
    comps.sort(key=lambda c: (len(c), sorted(sig[n] for n in c), natural_key(c[0])))
    order: list[str] = []
    optimal = True
    for comp in comps:
        inside = set(comp)
        seq, proven = _solve_component(
            comp, children, parents, units=[u for u in units if u[0] in inside]
        )
        order += seq
        optimal = optimal and proven
    return order, optimal


def _components(
    names: list[str],
    children: dict[str, list[str]],
    parents: dict[str, list[str]],
    unit_of: Mapping[str, tuple[str, ...]] | None = None,
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
            mates = list(unit_of[cur]) if unit_of else []
            for nxt in children[cur] + parents[cur] + mates:
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        out.append(sorted(members, key=rank.__getitem__))
    return out


def _solve_component(
    comp: list[str],
    children: dict[str, list[str]],
    parents: dict[str, list[str]],
    budget: int = _ORDER_BUDGET,
    units: list[tuple[str, ...]] | None = None,
) -> tuple[list[str], bool]:
    """The order of one component with the least total edge length.

    Total length is the sum over gaps of the edges crossing them, and the
    edges crossing a gap depend only on the set placed above it, so orders
    reaching the same set merge: a DP over placed sets. A move places a whole
    block, which is ready once every parent outside it is. A state's bound
    adds one row per edge not yet begun, and gives the targets of edges
    already open distinct rows, busiest first; a target still waiting on a
    parent cannot take the next row. It bounds every order of single nodes,
    so it bounds the block orders among them. States above a known order's
    length go safely. The proof is lost only when a layer still exceeds its
    budget after that, so a first pass at a sixteenth of the budget sets a
    tighter ceiling for the second. Among equal lengths the order first by
    name wins.
    """
    k = len(comp)
    at = {n: i for i, n in enumerate(comp)}
    groups = [tuple(at[n] for n in u) for u in units] if units else [(i,) for i in range(k)]
    if len(groups) == 1:
        return [comp[i] for i in groups[0]], True
    ups = [sum(1 << at[p] for p in parents[n]) for n in comp]
    kids = [[at[c] for c in children[n]] for n in comp]
    indeg = [len(parents[n]) for n in comp]
    outdeg = [len(children[n]) for n in comp]
    top = max(indeg)
    roots = sum(1 << i for i in range(k) if indeg[i] == 0)
    waiting = [(d, sum(1 << i for i in range(k) if indeg[i] == d)) for d in range(top, 0, -1)]

    lead = {g[0]: j for j, g in enumerate(groups)}
    mask = [sum(1 << i for i in g) for g in groups]
    ext = [0] * len(groups)
    delta = [0] * len(groups)
    inner = [0] * len(groups)
    begins = [0] * len(groups)
    for j, g in enumerate(groups):
        for i in g:
            ext[j] |= ups[i]
            delta[j] += outdeg[i] - indeg[i]
            inner[j] += delta[j]
            begins[j] += outdeg[i]
        ext[j] &= ~mask[j]

    def ready(placed, avail):
        for v in _bits(avail):
            j = lead.get(v)
            if j is not None and ext[j] & ~placed == 0:
                yield j

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

    def place(placed, avail, hist, j):
        for v in groups[j]:
            placed, avail, hist = step(placed, avail, hist, v)
        return placed, avail, hist

    start = (0, 0, 0, roots, (0,) * (top + 2), sum(outdeg))

    def run(width, ceiling):
        states = [start]
        back: list[dict[int, tuple[int, int]]] = []
        proven = True
        for _ in groups:
            reached: dict[int, tuple] = {}
            for rank, (placed, cost, cut, avail, hist, unbegun) in enumerate(states):
                for j in ready(placed, avail):
                    nxt = placed | mask[j]
                    total = cost + len(groups[j]) * cut + inner[j]
                    cur = reached.get(nxt)
                    if cur is None or total < cur[1]:
                        reached[nxt] = ((rank, j), total, cut + delta[j], rank, j)
            layer = []
            for nxt, (key, cost, cut, rank, j) in reached.items():
                placed, _, _, avail, hist, unbegun = states[rank]
                _, avail, hist = place(placed, avail, hist, j)
                unbegun -= begins[j]
                score = cost + bound(hist, avail, unbegun)
                if score <= ceiling:
                    layer.append((key, score, (nxt, cost, cut, avail, hist, unbegun), placed, j))
            layer.sort(key=lambda item: item[0])
            if len(layer) > width:
                proven = False
                keep = sorted(range(len(layer)), key=lambda i: (layer[i][1], i))[:width]
                layer = [layer[i] for i in sorted(keep)]
            back.append({s[0]: (placed, j) for _, _, s, placed, j in layer})
            states = [s for _, _, s, _, _ in layer]
        if not states:
            return None, None, False
        seq, placed = [], states[0][0]
        for t in range(len(groups) - 1, -1, -1):
            placed, j = back[t][placed]
            seq.append(j)
        seq.reverse()
        return seq, states[0][1], proven

    greedy, placed, avail, hist, ceiling, cut = [], 0, roots, start[4], 0, 0
    for _ in groups:
        j = max(ready(placed, avail), key=lambda j: (-delta[j] / len(groups[j]), -j))
        greedy.append(j)
        ceiling += len(groups[j]) * cut + inner[j]
        cut += delta[j]
        placed, avail, hist = place(placed, avail, hist, j)

    best, proven = greedy, False
    width = max(8, budget // len(groups))
    for width in (max(1, width // 16), width):
        seq, cost, done = run(width, ceiling)
        if seq is not None and (cost < ceiling or done):
            best, ceiling = seq, cost
        if done:
            proven = True
            break
    return [comp[i] for j in best for i in groups[j]], proven


def _bits(mask: int):
    while mask:
        low = mask & -mask
        yield low.bit_length() - 1
        mask ^= low


def _congruent(
    order: list[str],
    motifs: Sequence[Motif],
    children: dict[str, list[str]],
    units: list[tuple[str, ...]] = (),
) -> list[str]:
    """Give repeated subgraphs one internal order wherever that costs no length.

    Each instance's order, read through the twin map, is replayed onto the
    rows every other instance already holds; a replay is kept only if it
    respects precedence and the blocks, and does not lengthen the total. The
    instance whose order the most others can adopt sets the pattern.
    """
    edges = [(p, c) for p in order for c in children[p]]
    units = [u for u in units if len(u) > 1]

    def length(r):
        return sum(r[c] - r[p] for p, c in edges)

    def valid(r):
        return all(r[p] < r[c] for p, c in edges) and _keeps_units(r, units)

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
    hlen: int = 0
    bends: int = 0
    repeats: int = 0
    congruent: int = 0
    optimal: bool = False
    optimal_columns: bool = False

    @property
    def congruence(self) -> float:
        return self.congruent / self.repeats if self.repeats else 1.0

    def __str__(self) -> str:
        return (
            f"length={self.length} width={self.width} crossings={self.crossings}"
            f" hlen={self.hlen} bends={self.bends} off_right={self.off_right} drops={self.drops}"
            f" disorder={self.disorder} repeats={self.congruent}/{self.repeats}"
            f" optimal={self.optimal} optimal_columns={self.optimal_columns}"
        )


def measure(lay: Layout, motifs: Sequence[Motif] | None = None) -> Metrics:
    """`crossings` counts runs a bar passes over, a parent that goes on
    below it included; `hlen` sums every edge's horizontal travel;
    `disorder` counts pairs of runs live together with the one reaching
    further down on the left, an earlier start breaking a tie; `drops`
    counts parents whose run falls straight into their last child; `bends`
    counts nodes some run ends at that none falls straight into."""
    crossings = drops = hlen = 0
    for bar in lay.bars.values():
        lo, hi = bar.span
        hlen += sum(abs(f.col - bar.col) for f in bar.feeds)
        crossings += sum(
            1 for u in lay.live(bar.band) if lo < u.col < hi and u.node != bar.node
        )
        drops += sum(1 for f in bar.feeds if f.turns and f.col == bar.col)

    bends = sum(
        1 for bar in lay.bars.values()
        if any(f.turns for f in bar.feeds) and not any(f.turns and f.col == bar.col for f in bar.feeds)
    )
    runs = sorted(lay.runs.values(), key=lambda u: u.top)
    disorder = 0
    for i, a in enumerate(runs):
        for b in runs[i + 1:]:
            if b.top >= a.bottom:
                break
            left, right = (a, b) if a.col < b.col else (b, a)
            disorder += (left.bottom, -left.top) > (right.bottom, -right.top)

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
        hlen=hlen,
        bends=bends,
        repeats=repeats,
        congruent=congruent,
        optimal=lay.optimal,
        optimal_columns=lay.optimal_columns,
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
