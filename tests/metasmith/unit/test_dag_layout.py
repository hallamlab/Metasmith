import random

import pytest

from metasmith.models.dag_layout import layout, measure, natural_key, repeat_motifs


def _lay(edges, nodes=()):
    return layout(nodes, edges)


def _rows(lay):
    return list(lay.order)


def _length(lay):
    return sum(lay.row[c] - lay.row[p] for p, c in lay.edges)


def _shortest(names, edges):
    """The least total edge length over every topological order, by an exact
    DP over placed sets: the edges crossing a gap depend only on the set
    above it, so this shares no bound or budget with the solver it checks."""
    at = {n: i for i, n in enumerate(names)}
    ups = [0] * len(names)
    out = [0] * len(names)
    for p, c in edges:
        ups[at[c]] |= 1 << at[p]
        out[at[p]] += 1
    ins = [bin(u).count("1") for u in ups]
    best = {0: (0, 0)}
    for _ in names:
        nxt = {}
        for placed, (cost, cut) in best.items():
            for v in range(len(names)):
                if placed >> v & 1 or ups[v] & ~placed:
                    continue
                c = cut + out[v] - ins[v]
                key = placed | 1 << v
                if key not in nxt or cost + c < nxt[key][0]:
                    nxt[key] = (cost + c, c)
        best = nxt
    return next(iter(best.values()))[0]


def _random_dag(seed, n, p):
    rng = random.Random(seed)
    names = [f"n{i}" for i in range(n)]
    perm = names[:]
    rng.shuffle(perm)
    edges = [(perm[i], perm[j]) for i in range(n) for j in range(i + 1, n) if rng.random() < p]
    return names, edges


def _sparse_dag(seed, n):
    rng = random.Random(seed)
    names = [f"n{i}" for i in range(n)]
    edges = []
    for j in range(1, n):
        for _ in range(1 + (rng.random() < 0.8)):
            edges.append((names[rng.randrange(max(0, j - 12), j)], names[j]))
    return names, edges


# -- the row order -----------------------------------------------------------


@pytest.mark.parametrize("seed", range(300))
def test_the_row_order_is_the_shortest_there_is(seed):
    names, edges = _random_dag(seed, 3 + seed % 7, (0.15, 0.3, 0.5)[seed % 3])
    lay = layout(names, edges)
    assert lay.optimal
    assert _length(lay) == _shortest(list(lay.order), list(lay.edges))


def test_a_thousand_nodes_come_out_the_same_every_time():
    names, edges = _sparse_dag(1000, 1000)
    first = layout(names, edges)
    rng = random.Random(0)
    rng.shuffle(names)
    rng.shuffle(edges)
    again = layout(names, edges)
    assert (again.order, again.col) == (first.order, first.col)


def test_a_small_graph_is_proven_shortest():
    lay = _lay([("reads", "qc"), ("qc", "asm"), ("reads", "map"), ("asm", "map"),
                ("map", "bins"), ("asm", "bins")])
    assert lay.optimal


def test_a_caller_may_fix_the_rows():
    mine = ["c", "b", "a", "d"]
    lay = layout(mine, [("a", "d"), ("b", "d")], order=mine)
    assert _rows(lay) == mine
    assert not lay.optimal


def test_an_order_that_would_reverse_an_edge_is_declined():
    edges = [("a", "b")]
    assert _rows(layout([], edges, order=["b", "a"])) == ["a", "b"]
    assert _rows(layout([], edges, order=["a"])) == ["a", "b"]
    assert _rows(layout([], edges, order=["a", "b", "c"])) == ["a", "b"]


def test_step_numbers_sort_numerically():
    assert _rows(_lay([("r", "2 b"), ("r", "10 a")])) == ["r", "2 b", "10 a"]
    assert natural_key("2 b") < natural_key("10 a")


def test_a_leaf_child_is_emitted_directly_under_its_parent():
    rows = _rows(_lay([("t", "dead_end"), ("t", "used"), ("used", "next"), ("next", "last")]))
    assert rows.index("dead_end") == rows.index("t") + 1


def test_a_join_is_below_every_parent():
    lay = _lay([("a", "j"), ("b", "j"), ("c", "j"), ("a", "b"), ("a", "c")])
    assert all(lay.row[p] < lay.row["j"] for p in ("a", "b", "c"))


def test_disconnected_components_all_appear():
    assert set(_rows(_lay([("a", "b"), ("x", "y")]))) == {"a", "b", "x", "y"}


def test_disjoint_components_are_ordered_smallest_first():
    lay = _lay([("a", "b"), ("a", "c"), ("a", "d"), ("x", "y")])
    assert _rows(lay) == ["x", "y", "a", "b", "c", "d"]


def test_component_order_tie_break_is_deterministic_and_input_order_independent():
    edges = [("p", "q"), ("m", "n")]
    assert _rows(_lay(edges)) == ["m", "n", "p", "q"]
    assert _rows(_lay(list(reversed(edges)))) == ["m", "n", "p", "q"]


def test_an_isolated_node_is_a_component_of_one():
    assert _rows(layout(["solo"], [("a", "b"), ("b", "c")])) == ["solo", "a", "b", "c"]


_SHUFFLE_EDGES = [
    ("reads", "qc"), ("reads", "stats"), ("qc", "assembly"),
    ("assembly", "bin_a"), ("assembly", "bin_b"), ("assembly", "genes"),
    ("bin_a", "qual"), ("bin_b", "qual"), ("genes", "annot"),
    ("qual", "final"), ("annot", "final"),
]


def test_layout_ignores_the_order_edges_were_added():
    reference = _lay(_SHUFFLE_EDGES)
    rng = random.Random(0)
    for _ in range(20):
        shuffled = _SHUFFLE_EDGES[:]
        rng.shuffle(shuffled)
        lay = _lay(shuffled)
        assert (lay.order, lay.col, lay.edges) == (reference.order, reference.col, reference.edges)


def test_layout_ignores_the_order_nodes_were_declared():
    names = sorted({n for e in _SHUFFLE_EDGES for n in e})
    reference = _lay(_SHUFFLE_EDGES, names)
    rng = random.Random(1)
    for _ in range(10):
        rng.shuffle(names)
        lay = _lay(_SHUFFLE_EDGES, names)
        assert (lay.order, lay.col) == (reference.order, reference.col)


def test_cycle_is_broken_and_flagged():
    lay = _lay([("a", "b"), ("b", "c"), ("c", "a")])
    assert len(lay.back) == 1
    assert len(lay.edges) == 2
    assert lay.height == 3


def test_self_loop_is_a_back_edge():
    lay = _lay([("a", "a")])
    assert (lay.back, lay.edges) == ((("a", "a"),), ())


def test_duplicate_edges_collapse():
    assert len(_lay([("a", "b"), ("a", "b")]).edges) == 1


def test_edge_endpoints_not_declared_as_nodes_are_adopted():
    assert set(layout([], [("a", "b")]).order) == {"a", "b"}


def test_empty_graph():
    lay = _lay([])
    assert (lay.order, lay.edges, lay.height, lay.width) == ((), (), 0, 0)


# -- repeated blocks ---------------------------------------------------------

_TAGS = ("a", "b", "c")
_REPEATS = (
    [("in", f"{i} run") for i, t in enumerate(_TAGS)]
    + [(f"{i} run", f"out::{t}_bins") for i, t in enumerate(_TAGS)]
    + [(f"{i} run", f"out::{t}_table") for i, t in enumerate(_TAGS)]
    + [(f"out::{t}_bins", f"{i + 3} score") for i, t in enumerate(_TAGS)]
    + [(f"out::{t}_bins", f"{i + 6} classify") for i, t in enumerate(_TAGS)]
    + [("db::ref", f"{i + 6} classify") for i, _ in enumerate(_TAGS)]
    + [(f"{i + 3} score", "sink::quality") for i, _ in enumerate(_TAGS)]
    + [(f"{i + 6} classify", "sink::taxonomy") for i, _ in enumerate(_TAGS)]
)


def _run_motif(lay):
    return next(m for m in repeat_motifs(lay) if m.heads[0] == "0 run")


def test_a_repeated_block_is_found_by_shape_not_by_name():
    lay = _lay(_REPEATS)
    assert _run_motif(lay).blocks[0] == frozenset(
        {"0 run", "out::a_bins", "out::a_table", "3 score", "6 classify"}
    )


def test_every_instance_of_a_block_keeps_one_internal_order():
    lay = _lay(_REPEATS)
    m = _run_motif(lay)
    orders = {
        tuple(m.twin[x] for x in sorted(block, key=lay.row.__getitem__))
        for block in m.blocks
    }
    assert len(orders) == 1, orders


def test_congruence_counts_instances_drawn_alike():
    m = measure(_lay(_REPEATS))
    assert m.repeats == 3
    assert 1 <= m.congruent <= m.repeats
    assert m.congruence == m.congruent / m.repeats


def test_a_graph_with_nothing_repeated_is_congruent_by_definition():
    m = measure(_lay([("a", "b"), ("b", "c"), ("c", "d")]))
    assert (m.repeats, m.congruent, m.congruence) == (0, 0, 1.0)


def test_two_nodes_of_a_kind_are_not_a_class_on_their_own():
    assert repeat_motifs(_lay([("r", "x"), ("r", "y")])) == ()


def test_an_ancestor_and_its_descendant_are_never_two_instances():
    lay = _lay([("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")])
    assert all(not (set(m.blocks[0]) & set(m.blocks[1])) for m in repeat_motifs(lay))


# -- the grid ----------------------------------------------------------------


def _max_live(lay):
    return max((len(lay.live(h)) for h in range(-1, 2 * lay.height)), default=0)


_SHAPES = {
    "chain": [("a", "b"), ("b", "c"), ("c", "d")],
    "diamond": [("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")],
    "join": [("a", "j"), ("b", "j"), ("c", "j"), ("a", "b"), ("a", "c")],
    "fan": [("root", f"leaf_{i}") for i in range(7)] + [(f"leaf_{i}", "sink") for i in range(7)],
    "reuse": [("root", "x"), ("x", "mid"), ("mid", "y"), ("y", "end"),
              ("root", "mid"), ("mid", "end")],
    "shuffle": _SHUFFLE_EDGES,
    "repeats": list(_REPEATS),
}


@pytest.mark.parametrize("name", _SHAPES)
def test_the_width_is_the_most_runs_ever_live_at_once(name):
    lay = _lay(_SHAPES[name])
    assert lay.width == _max_live(lay)


@pytest.mark.parametrize("name", _SHAPES)
def test_every_fed_node_has_one_bar_in_the_band_above_it(name):
    lay = _lay(_SHAPES[name])
    fed = {c for _, c in lay.edges}
    assert set(lay.bars) == fed
    for n, bar in lay.bars.items():
        assert bar.band == 2 * lay.row[n] - 1
        assert bar.col == lay.col[n]
        assert sorted(f.src for f in bar.feeds) == sorted(lay.parents[n])


@pytest.mark.parametrize("name", _SHAPES)
def test_a_route_has_at_most_one_horizontal_leg_and_it_is_the_bar(name):
    lay = _lay(_SHAPES[name])
    for src, dst in lay.edges:
        points = lay.route(src, dst)
        legs = [(a, b) for a, b in zip(points, points[1:]) if a[0] == b[0]]
        assert len(legs) <= 1
        for (h, _), _ in legs:
            assert h == lay.bars[dst].band
        for (h0, c0), (h1, c1) in zip(points, points[1:]):
            assert h0 == h1 or c0 == c1


def test_a_chain_is_one_straight_column():
    lay = _lay(_SHAPES["chain"])
    assert lay.width == 1
    assert set(lay.col.values()) == {0}
    assert all(len(lay.route(*e)) == 2 for e in lay.edges)


def test_a_diamond_needs_one_extra_column():
    assert _lay(_SHAPES["diamond"]).width == 2


def test_a_column_is_reused_after_its_run_ends():
    assert _lay(_SHAPES["reuse"]).width == 2


def test_a_fan_out_takes_no_more_columns_than_it_needs():
    lay = _lay(_SHAPES["fan"])
    assert lay.width == _max_live(lay) <= 8


def test_a_last_child_takes_its_parents_column_as_a_straight_drop():
    lay = _lay([("t", "dead_end"), ("t", "used"), ("used", "next")])
    assert lay.col["used"] == lay.col["t"]
    assert lay.col["next"] == lay.col["used"]
    assert measure(lay).drops == 2


def test_the_longer_run_goes_left():
    lay = _lay([("r", "short"), ("r", "long1"), ("long1", "long2"), ("long2", "long3")])
    assert lay.col["long1"] < lay.col["short"]
    assert measure(lay).disorder == 0


def test_runs_sharing_a_column_never_overlap():
    lay = _lay(_SHAPES["repeats"])
    by_col = {}
    for run in lay.runs.values():
        by_col.setdefault(run.col, []).append((run.top, run.bottom))
    for spans in by_col.values():
        spans.sort()
        assert all(a[1] <= b[0] for a, b in zip(spans, spans[1:]))


def test_a_leaf_run_ends_before_the_next_row():
    lay = _lay([("a", "b")], ["a", "b", "solo"])
    assert (lay.runs["b"].top, lay.runs["b"].bottom) == (2 * lay.row["b"] - 1, 2 * lay.row["b"] + 1)


def test_the_metrics_read_off_the_grid():
    lay = _lay(_SHAPES["diamond"])
    m = measure(lay)
    assert (m.length, m.width, m.optimal) == (_length(lay), 2, True)
    assert m.off_right == sum(lay.width - 1 - c for c in lay.col.values())
