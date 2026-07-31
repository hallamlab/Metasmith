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

`measure` scores a finished layout, and `layout` uses it on itself: where a pass
has two defensible answers it draws both and keeps the cheaper one, rather than
carrying a constant tuned against one plan. Cost is the total vertical distance
the edges travel, then lanes, then crossings — the first of those is the one
that decides whether the drawing reads as the modules the graph actually has,
because a step drawn far from what feeds it takes a rail through everything in
between.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

__all__ = [
    "LayoutNode", "LayoutEdge", "Layout", "Metrics", "Motif",
    "layout", "measure", "dominators", "natural_key", "repeat_motifs",
]

_DIGITS = re.compile(r"(\d+)")
# how much smaller than the main line a side branch has to be to be drawn
# first; 0 means never, and the two are tried against each other
_SIDE_BRANCH = (2, 0)
# how far down a node's descendants its shape hash looks. Measured: 1 is too
# local — it pairs the three merge steps of the metagenomics plan on nothing
# more than their kind and fan-in — and every depth from 2 to 6 returns exactly
# the same classes, on that plan and across 300 random DAGs. 3 is taken from
# the middle of that plateau, since a deeper graph is the case where the depths
# would start to differ and none of them costs anything measurable.
_SIGNATURE_DEPTH = 3


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
    order: Sequence[str] | None = None,
) -> Layout:
    """Place a graph. `order` fixes the rows and only the lanes are chosen.

    A caller whose rows already exist — a form whose fields are the nodes, and
    which lays them out in the order the person typed them — cannot use the row
    order this module would pick, because the two would disagree about which
    row a node is in and the rails would be drawn across the markers. Passing
    the rows in is the whole of the fix; everything after `_row_order` is
    unchanged, so such a drawing is the same drawing, just not re-sorted.

    It must be a permutation of the node set (every node exactly once, and no
    name the graph does not have), and it must be topological — an edge running
    upward would break the one property every backend is written against — or
    it is ignored and the module picks the rows itself.
    """
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

    # two sibling policies rather than one tuned constant: whether a small side
    # branch is drawn before the main line or after it is the single choice the
    # walk makes that a graph's shape can reverse, so both are drawn and the
    # cheaper one wins. Everything else about the two is identical.
    #
    # Congruence leads the key, and it is the one place symmetry is allowed to
    # cost rail: three blocks that read as three copies are worth more than the
    # rows saved by letting each of them find its own cheapest shape.
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
    """A caller's row order, or None if it cannot be honoured.

    Silently ignored rather than raised on: the caller is a wire payload, and a
    stale one — a row deleted between the request being built and it arriving —
    should still draw the graph rather than 500. What it must not do is draw it
    with an order the rest of this module's invariants do not hold for.
    """
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
    """Lanes and routing for one candidate row order.

    Three lane assignments are drawn and compared on congruence, then width,
    then crossings, then detours. Width was the only test for a long time, and
    it left the greedy result in place whenever the repack merely tied — which
    is most of the time, and is exactly when the repack is worth having,
    because closing the gaps a lane left open also stops the rails jogging past
    one another to reach them.

    Crossings was the first answer to that, and it does not see the case it was
    added for: a rail sent out to a lane of its own between two nodes one row
    apart leaves its corridor and comes straight back without crossing
    anything. `detours` counts exactly that, last, so it can only separate
    candidates that are already equal on everything anyone would trade for.

    The last two are the congruence pass, and it takes both halves of the lane
    assignment to work. The rows already make the blocks congruent; without
    these they can still be congruent in shape and sit in unrelated lanes,
    which is most of what stops them reading as copies. Neither is a
    constraint — a lane already busy is simply not taken — so on a graph with
    no repeats they are the repack and cannot lose.
    """
    # node -> (its counterpart one instance up, its own head, that head).
    # One instance up and not the first: instances are packed in row order, so
    # the nearest one already placed is the one whose lanes are still reachable
    # — chaining them also means an awkward first block does not make every
    # copy after it awkward too.
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
    jump: int,
    motifs: Sequence[Motif] = (),
    sig: Mapping[str, int] | None = None,
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

    Then a side branch at least `jump` times smaller than the largest sibling,
    then the spine, then everything else — each group narrowest-first, because
    every sibling still waiting holds a lane and the wide subtree should be the
    one with the fewest siblings left beside it.

    Letting a small branch go before the main line is the same trade as putting
    leaves first, one size up. Making the whole side branch wait costs it a rail
    as long as the main line's entire subtree — that is where a five-step
    taxonomy branch off the reads ends up sixty rows below the reads it needs.
    Letting it go first costs the main line the handful of rows the branch
    occupies. The threshold is a ratio and not a count so it does not have to
    know how big the graph is; `jump = 0` disables it, and the caller draws it
    both ways and keeps the cheaper one.

    Only one root is seeded — the one owning most of the graph. Every other root
    is *supply*: a reference database, or a second input the graph joins in
    later. Supply is held back and emitted on demand, immediately above the
    first step that stalls waiting for it, together with the whole chain behind
    it. That placement is what keeps a module together: the three gtdbtk steps
    belong beside the three binners that feed them, and they end up 35 rows
    below instead if the database they share is emitted at the top or sunk to
    the bottom.

    Pulling one root at a time, greedily and without the chain, was tried and is
    a pessimisation — the root arrives in the middle of an open fan-out instead
    of before it. Moving the closure is what makes the difference: the chain is
    emitted and consumed in consecutive rows, so it never holds a lane open
    across anything.

    Three things then bend that walk towards drawing a repeated block the same
    way every time, and each of them is a place where a per-instance tie-break
    used to leak into the picture:

    - **Order inside a block.** Every rule above resolves a tie on the node
      itself — which of two steps is on the spine, which of them owns the
      output they share — so one binner emitted gtdbtk before checkm and the
      next emitted them the other way round. A node inside a repeat class
      instead ranks its children by the order the class's *first* instance put
      them in, matched by shape.

    - **Shared supply.** A reference database pulled in by the second step of
      the first block makes that block two rows longer than its copies. When
      the stalled consumer is inside a class with more than one instance the
      chain is hoisted above the first instance instead, so it precedes the
      whole group and every block starts at the same place.

    - **Shared sinks.** A node joining several instances — the one taxonomy
      output all three gtdbtk steps write — is held back until every instance
      is finished, rather than landing wherever the walk happened to have a
      node left over, which is what used to wedge it into the middle of the
      third block.
    """
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

    # the canonical intra-class order, read off each class's first instance
    # with the ordinary rules and then imposed on the rest of them. Keyed by
    # shape on both ends because the nodes are named per instance.
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
    # a node is supply when every root above it is being held back; its whole
    # ancestry is then supply too, which is what makes the pull terminate
    supply: set[str] = set()
    for n in topo:
        if parents[n]:
            if all(p in supply for p in parents[n]):
                supply.add(n)
        elif n in held:
            supply.add(n)

    stack: list[str] = []
    blocked: list[str] = []  # joins waiting for every instance of their class
    forced: set[str] = set()  # ... and the ones whose wait cannot be satisfied

    def _gated(n: str) -> bool:
        """True while `n` joins more than one instance of a class that is not
        finished. A shared output belongs below every block it joins."""
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
        """Where the supply behind `n` should go: above the first instance of
        `n`'s class, or None to leave it where it was pulled.

        Only when nothing in the chain has a parent already drawn at or below
        that row — moving the chain up past one of its own inputs would reverse
        an edge, and the whole layout is built on that not happening.
        """
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
        """Emit the supply behind `n`, deepest chain first. False if `n` is
        waiting on anything the walk is going to reach on its own."""
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
        # a held root nothing stalled on — a disconnected component, or supply
        # for a node the cycle breaker cut away from it
        remaining = [n for n in remaining if n not in emitted]
        if remaining and len(order) < len(names):
            stack.append(remaining.pop(0))
            continue
        # nothing left to finish a class with, so the gate can never open;
        # drawing the join in the wrong place beats not drawing it at all
        blocked[:] = [n for n in blocked if n not in emitted]
        if blocked:
            forced.update(blocked)
            stack.extend(reversed(sorted(blocked, key=natural_key)))
            blocked.clear()
            continue
        break

    if len(order) < len(names):  # only reachable if cycle breaking left an island
        order += [n for n in topo if n not in emitted]
    return order


def _canonical_primaries(
    motifs: Sequence[Motif],
    children: dict[str, list[str]],
    spine: set[str],
    weight: dict[str, int],
) -> dict[str, str]:
    """Give every instance of a class the lane inheritance the first one got.

    Which child continues a node's rail is settled on the node itself — is it
    on the spine, how much hangs below it — and inside a repeated block those
    answers differ per instance, so one block keeps its step in the block's own
    lane and its copies push theirs off to the side. Mapped through the class's
    pairing, all three make the same choice.
    """
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
    """Which child inherits a node's lane, so a chain renders as one rail."""
    return min(kids, key=lambda c: (0 if c in spine else 1, -weight[c], natural_key(c)))


def _assign_lanes(
    order: list[str],
    children: dict[str, list[str]],
    weight: dict[str, int],
    spine: set[str],
    primary_of: Mapping[str, str] | None = None,
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

    `primary_of` overrides that choice per node, and exists for one reason: it
    is a per-instance tie-break like every other, and it decides whether a
    repeated block's steps sit in their block's own lane or somewhere off to
    the side. Only the first of three bin-fasta nodes continues into its checkm
    step here — the other two continue into the aggregator every block feeds,
    which is on the spine and outweighs it — so two of the three checkm steps
    end up in whatever lane happened to be free.
    """
    primary_of = primary_of or {}
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
            primary = primary_of.get(n) or _primary_child(kids, spine, weight)
            if primary not in kids:  # defensive: an override must name a child
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
    """Repack the lanes once the rows are known.

    The greedy pass assigns a lane the moment a node reserves one, so a lane
    opened early and released late blocks a later rail that would have fitted
    beside it. Once the rows are fixed, though, every rail and every chain
    occupies a known contiguous run of rows, and packing intervals is a much
    easier problem. Measured on 300 random DAGs this is a lane narrower 18% of
    the time and never wider. It does nothing for the width of the spanish-lakes
    metagenomics plan, which is set by rails that really are all live at once —
    but it takes a fifth of the crossings out of it, which is why the caller
    compares the two on crossings and not only on width.

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

    `shift` is the congruence pass: an item with a counterpart in a repeat
    class's first instance asks for the lane that counterpart was given, offset
    by however far this instance's head sits from the first one's. Offset and
    not the absolute lane, because the heads themselves usually cannot line up
    — three binners fanning out of one node are three parallel rails by
    construction, one lane apart. Nothing about the packing's correctness rests
    on this: a lane is still only handed out when no other item holds those
    rows, so the worst it can do is what the plain repack would have done.
    """
    shift = shift or {}
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

    def _offset(x: str) -> int | None:
        """How far this instance's head sits from the first instance's."""
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
        # keep an item where it already was whenever that lane is free. Pure
        # left-edge colouring packs harder but slides branches sideways under
        # each other, which turns a fan-in comb into a zigzag of rails jogging
        # left and right past one another to save a lane nobody missed.
        #
        # Packing an item next to the node it hangs off instead, so that a
        # transform's four products come out side by side, was tried: it is 16%
        # more crossings for no measured gain in how many fan-outs land on
        # adjacent lanes, because a lane near the parent is rarely the free one.
        #
        # `want` — where this item's counterpart in the first instance ended up,
        # shifted by this instance's own head — is tried ahead of `was`, and
        # both are only preferences. `want` may name a lane no item has reached
        # yet, and opening it is allowed: the block's copies are worth a lane,
        # and if they are not this candidate loses to the plain repack on width.
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


# --- measurement ------------------------------------------------------------


@dataclass(frozen=True)
class Metrics:
    """What a drawing costs, as numbers a change can be argued with.

    `rail_rows` is the objective the row order minimises — the total vertical
    distance the edges have to travel, which is what makes a module read as one
    block instead of a step and a rail down the rest of the page. `crossings`
    counts the segment pairs the character backend draws as a `┼`.

    `congruence` is the one the selection now leads with: where the plan does
    the same thing three times the drawing has to show three copies of one
    block, and no amount of saved rail buys that back.
    """
    rail_rows: int
    lanes: int
    longest_rail: int
    crossings: int
    modules: int
    contiguous: int
    module_spread: int
    repeats: int = 0
    congruent: int = 0
    # sum of every node's lane index. Lane 0 is the one beside the labels, so
    # this is how far the markers sit from their own names. Measured but not
    # ranked: it was in the selection key for one commit and taken back out,
    # because buying it costs crossings (~11% across 300 random DAGs) and
    # produces rails that leave a lane and come straight back to it. Kept so
    # the next person to want it can argue with a number rather than an
    # impression.
    marker_lanes: int = 0
    # rails given a lane outside the span between their own two endpoints'
    # lanes, so the rail leaves the corridor between the nodes it joins and
    # comes back to it. Most of them are unavoidable -- a node with seven
    # children needs seven parallel rails and only one of them can be inside --
    # which is why this is a tie-break and never a term anything is traded for.
    detours: int = 0

    @property
    def contiguity(self) -> float:
        """Fraction of modules drawn as an unbroken run of rows."""
        return self.contiguous / self.modules if self.modules else 1.0

    @property
    def congruence(self) -> float:
        """Fraction of repeat instances drawn as a copy of the first one."""
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
    """Score a finished layout. Back edges are excluded throughout — they are
    drawn as an annotation, not routed, so they cost neither rail nor crossing.

    `motifs` is only ever an optimisation: `layout` already knows them and
    passes them in, and anyone else gets them recomputed from the drawing.
    """
    idx = lay.index
    forward = [e for e in lay.edges if not e.back]
    spans = [idx[e.dst].row - idx[e.src].row for e in forward]

    crossings = 0
    for row in range(max(lay.height - 1, 0)):
        # the same (entered from, rail, left towards) triples the character
        # backend paints, so this counts what actually gets drawn: a gap is one
        # sub-row for the fan-out and one for the fan-in, and a pair can cross
        # in either
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
        # a transform and its one product is a module by construction and is
        # contiguous whatever the row order does, so counting those would put
        # the score in the nineties before any work is done
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
    """Immediate dominator of every node, or None for a graph root.

    Cooper, Harvey and Kennedy's iterative formulation, which needs only one
    pass here: `names` is in a topological order, so every parent of a node has
    already been resolved when the node is reached and the fixpoint is immediate.

    The dominator tree is this module's definition of a *module*: everything a
    node dominates is reachable only through it, so those nodes belong to it and
    can be moved as one block without any edge to the rest of the graph
    reversing.

    Plans have many roots, so the meet of two nodes in different components has
    to land somewhere: a virtual root above every parentless node gives it a
    place, and is stripped back out to None on the way home.
    """
    top = "\0"  # no caller id can collide: node names come from real ids
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


# --- repeating motifs --------------------------------------------------------


@dataclass(frozen=True)
class Motif:
    """A shape the graph draws more than once, and the blocks that draw it.

    `heads` are the instances; `blocks[i]` is everything `heads[i]` dominates,
    so the blocks are disjoint and each can be moved as one thing. `twin` maps
    every node of every block onto its counterpart in `blocks[0]`, which is
    what lets a later instance be given the arrangement the first one got.
    """
    heads: tuple[str, ...]
    blocks: tuple[frozenset[str], ...]
    twin: dict  # node -> the corresponding node in blocks[0]

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
    """Each node's forward shape: its own kind and fan-in, plus the multiset of
    its children's shapes, to `depth` levels, interned to an int.

    A *shape* hash and not a label hash, because what is being matched is named
    per instance — a plan's three binners emit `comebin_contig_to_bin_table`
    and `semibin2_contig_to_bin_table`, so nothing at the block level matches
    by name, and the step numbers keep even the transforms apart.

    Fan-in is part of a node's own key and not an afterthought. Looking only
    downward, the metagenomics plan's `aggregator` — which collects all three
    binners and the checkm summary — is the same four-node chain as a
    `diamond` annotation step at any depth, and calling them one class puts
    two reference databases fourteen rows above where they are read.
    """
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
    """Match `block_b` onto `block_a` node by node, walking both in one order.

    None when they do not line up: the signature says the two nodes look alike
    to `_SIGNATURE_DEPTH` and the graph is free to differ below that.
    """
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
    """Every shape drawn more than once, biggest first and never nested.

    An instance's *block* is what hangs below it and below none of its
    siblings. Not the dominator subtree, which is what a module is elsewhere in
    this file and is the wrong tool here: a step taking a shared reference
    database is dominated by neither its producer nor the database, so the
    dominator reading cuts each of the three gtdbtk steps out of the binner
    block it belongs to and leaves a four-node stub. Subtracting what the
    siblings share keeps them, and the blocks come out disjoint by
    construction.

    Three filters after that. A block of one is dropped — every leaf of a kind
    repeats, and an instance that owns nothing arranges nothing. A class whose
    blocks do not line up node for node is dropped, because the signature only
    promises they look alike to `_SIGNATURE_DEPTH`. And a class every one of
    whose instances sits inside a class already kept is dropped, so what comes
    back is the whole repeated block rather than each of its parts: the three
    binners, not also the three bin-fasta subtrees inside them.
    """
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
            continue  # one instance hangs below another; they are not siblings
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
    """The repeated shapes of a finished layout, for callers that colour or
    score one. Same answer the row order worked from, recomputed from the
    drawing so nothing has to be threaded through it."""
    names = [n.name for n in lay.nodes]  # row order is a topological order
    kinds = {n.name: n.kind for n in lay.nodes}
    children: dict[str, list[str]] = {n: [] for n in names}
    for e in lay.edges:
        if not e.back:
            children[e.src].append(e.dst)
    for n in names:
        children[n].sort(key=natural_key)
    return _motifs(names, children, _signatures(names, children, kinds, depth))


def _congruence(lay: Layout, motifs: Sequence[Motif]) -> tuple[int, int]:
    """How many repeat instances are drawn as copies of one another.

    An instance's *arrangement* is where its block's nodes sit relative to its
    head, in rows and in lanes, with each node named by its counterpart in the
    first block so two instances are comparable at all. The class's canonical
    arrangement is whichever one the most instances take, and the score is how
    many take it — the first instance has no special claim on being right, and
    scoring against it would call three blocks incongruent because the one at
    the top had a rail in the way.
    """
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
