import random

import pytest

from metasmith.models.dag_layout import layout, measure, repeat_motifs


def _lay(edges, nodes=None):
    return layout(nodes or {}, edges)


def _rows(lay):
    return [n.name for n in lay.nodes]


def _lane(lay, name):
    return lay[name].lane


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
    lay = _lay([("root", "x"), ("x", "mid"), ("mid", "y"), ("y", "end"),
                ("root", "mid"), ("mid", "end")])
    assert lay.width == 2


def test_leaf_child_is_emitted_directly_under_its_parent():
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
    lay = _lay([("r", "2 b"), ("r", "10 a")])
    assert _rows(lay) == ["r", "2 b", "10 a"]


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
_KINDS = {
    **{f"{i} run": "T" for i in range(3)},
    **{f"{i} score": "T" for i in range(3, 6)},
    **{f"{i} classify": "T" for i in range(6, 9)},
}


def test_a_repeated_block_is_found_by_shape_not_by_name():
    lay = _lay(_REPEATS, _KINDS)
    heads = {m.heads for m in repeat_motifs(lay)}
    assert ("0 run", "1 run", "2 run") in heads
    block = next(m for m in repeat_motifs(lay) if m.heads[0] == "0 run").blocks[0]
    assert block == frozenset(
        {"0 run", "out::a_bins", "out::a_table", "3 score", "6 classify"}
    )


def test_every_instance_of_a_block_is_a_contiguous_run_of_rows():
    lay = _lay(_REPEATS, _KINDS)
    rows = {n.name: n.row for n in lay.nodes}
    for m in repeat_motifs(lay):
        for block in m.blocks:
            span = sorted(rows[x] for x in block)
            assert span == list(range(span[0], span[0] + len(span))), block


def test_every_instance_emits_its_children_in_the_same_order():
    lay = _lay(_REPEATS, _KINDS)
    rows = {n.name: n.row for n in lay.nodes}
    m = next(x for x in repeat_motifs(lay) if x.heads[0] == "0 run")
    shapes = {
        tuple(sorted((rows[x] - rows[head], m.twin[x]) for x in block))
        for head, block in zip(m.heads, m.blocks)
    }
    assert len(shapes) == 1, shapes


def test_the_shared_supply_is_drawn_once_above_the_first_instance():
    lay = _lay(_REPEATS, _KINDS)
    rows = {n.name: n.row for n in lay.nodes}
    m = next(x for x in repeat_motifs(lay) if x.heads[0] == "0 run")
    assert rows["db::ref"] < min(rows[h] for h in m.heads)


def test_a_shared_output_waits_for_every_instance():
    lay = _lay(_REPEATS, _KINDS)
    rows = {n.name: n.row for n in lay.nodes}
    m = next(x for x in repeat_motifs(lay) if x.heads[0] == "0 run")
    last = max(rows[x] for b in m.blocks for x in b)
    for sink in ("sink::quality", "sink::taxonomy"):
        assert rows[sink] > last, sink


def test_congruence_is_the_biggest_set_of_instances_drawn_alike():
    m = measure(_lay(_REPEATS, _KINDS))
    assert m.repeats >= 3
    assert m.congruent >= 3
    assert m.congruence == m.congruent / m.repeats


def test_a_graph_with_nothing_repeated_is_congruent_by_definition():
    m = measure(_lay([("a", "b"), ("b", "c"), ("c", "d")]))
    assert (m.repeats, m.congruent, m.congruence) == (0, 0, 1.0)


def test_two_nodes_of_a_kind_are_not_a_class_on_their_own():
    lay = _lay([("r", "x"), ("r", "y")])
    assert repeat_motifs(lay) == ()


def test_an_ancestor_and_its_descendant_are_never_two_instances():
    lay = _lay([("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")])
    assert all(
        not (set(m.blocks[0]) & set(m.blocks[1])) for m in repeat_motifs(lay)
    )


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


def _detours(lay):
    return [
        (e.src, e.dst)
        for e in lay.edges
        if not e.back
        and not (
            min(lay[e.src].lane, lay[e.dst].lane)
            <= e.lane
            <= max(lay[e.src].lane, lay[e.dst].lane)
        )
    ]


def test_a_rail_between_neighbouring_rows_does_not_take_a_lane_of_its_own():
    edges = [
        ("asv_seqs", "map_contigs"),
        ("assembly", "map_contigs"),
        ("identity_threshold", "map_contigs"),
        ("map_contigs", "asv_contig_map"),
        ("asv_seqs", "classify"),
        ("silva_classifier", "classify"),
        ("classify", "asv_taxonomy"),
    ]
    lay = _lay(edges)
    assert _detours(lay) == []


def test_a_fan_out_is_allowed_every_lane_it_needs():
    edges = [("root", f"leaf_{i}") for i in range(7)]
    edges += [(f"leaf_{i}", "sink") for i in range(7)]
    lay = _lay(edges)
    assert measure(lay).lanes <= 8
    assert measure(lay).detours == len(_detours(lay))


def test_a_caller_may_fix_the_rows():
    mine = ["c", "b", "a", "d"]
    lay = layout({n: None for n in mine}, [("a", "d"), ("b", "d")], order=mine)
    assert _rows(lay) == mine


def test_an_order_that_would_reverse_an_edge_is_declined():
    edges = [("a", "b")]
    assert _rows(layout({}, edges, order=["b", "a"])) == ["a", "b"]
    assert _rows(layout({}, edges, order=["a"])) == ["a", "b"]
    assert _rows(layout({}, edges, order=["a", "b", "c"])) == ["a", "b"]


def test_given_rows_still_keep_the_rails_off_the_markers():
    order = ["a", "b", "c", "d", "e"]
    edges = [("a", "b"), ("a", "c"), ("b", "c"), ("a", "d"), ("a", "e"), ("d", "e")]
    lay = layout({n: None for n in order}, edges, order)
    assert _rows(lay) == order
    occupied = {(n.row, n.lane) for n in lay.nodes}
    for e in lay.edges:
        for row in range(lay[e.src].row + 1, lay[e.dst].row):
            assert (row, e.lane) not in occupied
