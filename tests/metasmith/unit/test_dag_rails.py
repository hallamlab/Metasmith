"""The rule a drawing has to keep: one drawn path per edge, and no other.

A run carries one node's output and may feed many children, and a bar
gathers every parent of one node, so lines are shared. The drawing stays
readable only if following it from a node's marker arrives at exactly that
node's children, each by one way. Every corner of a route is drawn as an arc
and is a connection; a line passing straight through a point where another
line crosses is not. That is checkable off the routes alone: walk the drawn
unit segments, go straight wherever a line continues, and turn only where
some route turns in that direction.
"""

import random

import pytest

from metasmith.models.dag_layout import layout

_DOWN, _LEFT, _RIGHT = (1, 0), (0, -1), (0, 1)


def _direction(a, b):
    dh, dc = b[0] - a[0], b[1] - a[1]
    return (dh and dh // abs(dh), dc and dc // abs(dc))


def _drawing(lay):
    """Drawn unit steps as (point, direction), and the arcs as
    (point, direction in, direction out)."""
    steps, arcs = set(), set()
    for src, dst in lay.edges:
        points = lay.route(src, dst)
        for a, b in zip(points, points[1:]):
            d = _direction(a, b)
            p = a
            while p != b:
                steps.add((p, d))
                p = (p[0] + d[0], p[1] + d[1])
        for a, b, c in zip(points, points[1:], points[2:]):
            arcs.add((b, _direction(a, b), _direction(b, c)))
    return steps, arcs


def arrivals(lay, src, drawing=None):
    steps, arcs = drawing or _drawing(lay)
    markers = {(2 * lay.row[n], lay.col[n]): n for n in lay.order}
    counts: dict[str, int] = {}
    start = (2 * lay.row[src], lay.col[src])
    stack = [(start, _DOWN)] if (start, _DOWN) in steps else []
    while stack:
        p, d = stack.pop()
        q = (p[0] + d[0], p[1] + d[1])
        if q in markers:
            counts[markers[q]] = counts.get(markers[q], 0) + 1
            continue
        for out in (_DOWN, _LEFT, _RIGHT):
            if (q, out) in steps and (out == d or (q, d, out) in arcs):
                stack.append((q, out))
    return counts


def assert_rails_are_unambiguous(lay):
    drawing = _drawing(lay)
    for n in lay.order:
        wanted = {c: 1 for c in lay.children[n]}
        got = arrivals(lay, n, drawing)
        assert got == wanted, f"the drawing leads from {n} to {got}, its edges go to {wanted}"


def assert_grid_invariants(lay):
    live = max((len(lay.live(h)) for h in range(-1, 2 * lay.height)), default=0)
    assert lay.width == live
    assert set(lay.bars) == {c for _, c in lay.edges}
    for n, bar in lay.bars.items():
        assert bar.band == 2 * lay.row[n] - 1
    for src, dst in lay.edges:
        points = lay.route(src, dst)
        legs = [a for a, b in zip(points, points[1:]) if a[0] == b[0]]
        assert len(legs) <= 1
        assert all(h == 2 * lay.row[dst] - 1 for h, _ in legs)
        assert lay.runs[src].bottom >= 2 * lay.row[dst] - 1


def _random_dag(seed):
    rng = random.Random(seed)
    n = 3 + seed % 18
    p = (0.15, 0.3, 0.5)[seed % 3]
    names = [f"n{i}" for i in range(n)]
    perm = names[:]
    rng.shuffle(perm)
    edges = [(perm[i], perm[j]) for i in range(n) for j in range(i + 1, n) if rng.random() < p]
    if seed % 5 == 0 and edges:
        edges.append(edges[0][::-1])
    return names, edges


_FAN = [("assembly", t) for t in ("bowtie2", "metabat2", "semibin2", "comebin", "das_tool")]
_BINNING = _FAN + [
    ("reads", "assembly"), ("reads", "bowtie2"), ("bowtie2", "bam"),
    ("bam", "metabat2"), ("bam", "semibin2"), ("bam", "comebin"),
    ("metabat2", "das_tool"), ("semibin2", "das_tool"), ("comebin", "das_tool"),
    ("das_tool", "checkm2"),
]


def test_the_binning_tail_is_narrower_than_its_fan_out():
    assert layout([], _BINNING).width <= 4


@pytest.mark.parametrize("edges", [
    _FAN,
    _BINNING,
    [("a", "b"), ("b", "c"), ("c", "d")],
    [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")],
    [("a", "j"), ("b", "j"), ("c", "j"), ("a", "b"), ("a", "c")],
], ids=["fan", "binning", "chain", "diamond", "join"])
def test_a_named_shape_is_unambiguous(edges):
    lay = layout([], edges)
    assert_grid_invariants(lay)
    assert_rails_are_unambiguous(lay)


@pytest.mark.parametrize("seed", range(600))
def test_a_random_dag_keeps_the_grid_and_one_path_per_edge(seed):
    lay = layout(*_random_dag(seed))
    assert_grid_invariants(lay)
    assert_rails_are_unambiguous(lay)
