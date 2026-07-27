"""Structural properties of the rails layout.

These assert on the layout core rather than on rendered output, so they stay
meaningful if the glyphs or the pixel grid change.
"""
import random

import pytest

from metasmith.models.dag_layout import layout


def _lay(edges, nodes=None):
    return layout(nodes or {}, edges)


def _rows(lay):
    return [n.name for n in lay.nodes]


def _lane(lay, name):
    return lay[name].lane


# --- shape ------------------------------------------------------------------


def test_empty_graph():
    lay = _lay([])
    assert (lay.nodes, lay.edges, lay.height) == ((), (), 0)


def test_linear_chain_is_one_lane():
    lay = _lay([("a", "b"), ("b", "c"), ("c", "d")])
    assert _rows(lay) == ["a", "b", "c", "d"]
    assert {n.lane for n in lay.nodes} == {0}
    assert lay.width == 1


def test_chain_edges_are_straight():
    lay = _lay([("a", "b"), ("b", "c")])
    for e in lay.edges:
        assert {lane for _, lane in e.points} == {0.0}


def test_diamond_opens_and_closes_one_extra_lane():
    lay = _lay([("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")])
    assert lay.width == 2
    assert _lane(lay, "a") == _lane(lay, "d") == 0


def test_join_is_below_every_parent():
    lay = _lay([("a", "j"), ("b", "j"), ("c", "j"), ("a", "b"), ("a", "c")])
    j = lay["j"].row
    assert all(lay[p].row < j for p in ("a", "b", "c"))


def test_lane_is_reused_after_a_branch_closes():
    # two branches that never overlap in time should share one lane
    lay = _lay([("root", "x"), ("x", "mid"), ("mid", "y"), ("y", "end"),
                ("root", "mid"), ("mid", "end")])
    assert lay.width == 2


def test_leaf_child_is_emitted_directly_under_its_parent():
    # a product nobody consumes must not trail a rail down the whole drawing
    lay = _lay([("t", "dead_end"), ("t", "used"), ("used", "next"), ("next", "last")])
    rows = _rows(lay)
    assert rows.index("dead_end") == rows.index("t") + 1


def test_heaviest_child_inherits_the_lane():
    lay = _lay([("t", "dead_end"), ("t", "used"), ("used", "next")])
    assert _lane(lay, "used") == _lane(lay, "t")
    assert _lane(lay, "dead_end") != _lane(lay, "t")


def test_spine_is_the_heaviest_path_and_sits_in_lane_zero():
    lay = _lay([("r", "short"), ("r", "long1"), ("long1", "long2"), ("long2", "long3")])
    spine = [n.name for n in lay.nodes if n.spine]
    assert spine == ["r", "long1", "long2", "long3"]
    assert all(_lane(lay, n) == 0 for n in spine)


def test_depth_is_longest_path_not_shortest():
    lay = _lay([("a", "b"), ("b", "c"), ("a", "c")])
    assert lay["c"].depth == 2


# --- invariants the backends rely on ----------------------------------------


def test_no_node_sits_in_a_lane_an_edge_is_spanning():
    lay = _lay([
        ("r", "a"), ("r", "b"), ("r", "c"),
        ("a", "a2"), ("a2", "a3"), ("b", "b2"), ("c", "c2"),
        ("a3", "j"), ("b2", "j"), ("c2", "j"),
    ])
    for e in lay.edges:
        lo, hi = lay[e.src].row, lay[e.dst].row
        for n in lay.nodes:
            if lo < n.row < hi:
                assert n.lane != e.lane, f"{n.name} sits on the rail {e.src}->{e.dst}"


def test_every_edge_segment_is_axis_aligned():
    lay = _lay([("r", "a"), ("r", "b"), ("a", "a2"), ("a2", "j"), ("b", "j")])
    for e in lay.edges:
        for (r0, l0), (r1, l1) in zip(e.points, e.points[1:]):
            assert r0 == r1 or l0 == l1


def test_edges_run_downward():
    lay = _lay([("r", "a"), ("r", "b"), ("a", "j"), ("b", "j")])
    for e in lay.edges:
        assert lay[e.src].row < lay[e.dst].row


# --- determinism ------------------------------------------------------------

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
        assert _lay(shuffled) == reference


def test_layout_ignores_the_order_nodes_were_declared():
    names = sorted({n for e in _SHUFFLE_EDGES for n in e})
    rng = random.Random(1)
    reference = _lay(_SHUFFLE_EDGES, {n: "k" for n in names})
    for _ in range(10):
        rng.shuffle(names)
        assert _lay(_SHUFFLE_EDGES, {n: "k" for n in names}) == reference


def test_step_numbers_sort_numerically():
    # "10 x" must not tie-break ahead of "2 x"
    lay = _lay([("r", "2 b"), ("r", "10 a")])
    assert _rows(lay) == ["r", "2 b", "10 a"]


# --- degenerate input -------------------------------------------------------


def test_cycle_is_broken_and_flagged():
    lay = _lay([("a", "b"), ("b", "c"), ("c", "a")])
    backs = [e for e in lay.edges if e.back]
    assert len(backs) == 1
    assert len(lay.nodes) == 3
    assert len({n.row for n in lay.nodes}) == 3


def test_self_loop_is_a_back_edge():
    lay = _lay([("a", "a")])
    assert [e.back for e in lay.edges] == [True]


def test_duplicate_edges_collapse():
    lay = _lay([("a", "b"), ("a", "b")])
    assert len(lay.edges) == 1


def test_disconnected_components_all_appear():
    lay = _lay([("a", "b"), ("x", "y")])
    assert set(_rows(lay)) == {"a", "b", "x", "y"}


def test_kind_is_carried_through_untouched():
    sentinel = object()
    lay = layout({"a": sentinel, "b": None}, [("a", "b")])
    assert lay["a"].kind is sentinel


def test_edge_endpoints_not_declared_as_nodes_are_adopted():
    lay = layout({}, [("a", "b")])
    assert {n.name for n in lay.nodes} == {"a", "b"}


@pytest.mark.parametrize("size", [1, 2, 50])
def test_wide_fan_out_stays_consistent(size):
    edges = [("root", f"leaf_{i:02d}") for i in range(size)]
    lay = _lay(edges)
    assert lay.height == size + 1
    assert lay.width <= size + 1
