import shutil
from pathlib import Path

import pytest

from metasmith.models.dag_renderer import (
    STYLES, DagRenderer, Label, LabelMode, NodeKind,
)


def test_transform_node_styling():
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "step1")
    dot = r.to_dot()
    assert '"step1" [shape="oval", style="filled", fillcolor="#CCCCCC"]' in dot


def test_data_node_styling():
    r = DagRenderer()
    r.add_node(NodeKind.DATA, "input.fasta")
    assert '"input.fasta" [shape="box"]' in r.to_dot()


def test_header_has_font_and_rankdir_defaults():
    dot = DagRenderer().to_dot()
    assert 'fontname="Arial"' in dot
    assert 'rankdir="TB"' in dot
    assert dot.startswith("digraph G {")
    assert dot.rstrip().endswith("}")


def test_font_and_rankdir_overridable():
    dot = DagRenderer(font="Helvetica", rankdir="LR").to_dot()
    assert 'fontname="Helvetica"' in dot
    assert 'rankdir="LR"' in dot


def test_add_node_first_kind_wins():
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "x")
    r.add_node(NodeKind.DATA, "x")
    dot = r.to_dot()
    assert 'shape="oval"' in dot
    assert dot.count('"x" [') == 1


def test_add_edge_dedupes():
    r = DagRenderer()
    r.add_edge("a", "b")
    r.add_edge("a", "b")
    r.add_edge("a", "b")
    assert r.to_dot().count('"a" -> "b"') == 1


def test_add_edge_direction_matters():
    r = DagRenderer()
    r.add_edge("a", "b")
    r.add_edge("b", "a")
    dot = r.to_dot()
    assert '"a" -> "b"' in dot
    assert '"b" -> "a"' in dot


def test_add_edge_auto_declares_endpoints_as_data():
    r = DagRenderer()
    r.add_edge("input.fasta", "output.gbk")
    dot = r.to_dot()
    assert '"input.fasta" [shape="box"]' in dot
    assert '"output.gbk" [shape="box"]' in dot


def test_add_edge_does_not_override_prior_transform_kind():
    # load-bearing for the consumer adapters: a transform node declared via
    # add_node must keep its oval shape even when later add_edge calls reference
    # it.
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "step1")
    r.add_edge("input.fasta", "step1")
    r.add_edge("step1", "output.gbk")
    dot = r.to_dot()
    assert '"step1" [shape="oval", style="filled", fillcolor="#CCCCCC"]' in dot
    assert '"step1" [shape="box"]' not in dot


def test_to_dot_is_insertion_ordered_and_deterministic():
    def build():
        r = DagRenderer()
        r.add_node(NodeKind.TRANSFORM, "given")
        r.add_edge("given", "a")
        r.add_edge("given", "b")
        r.add_edge("given", "c")
        return r.to_dot()
    assert build() == build()
    dot = build()
    assert dot.index('"a"') < dot.index('"b"') < dot.index('"c"')


# --- identity vs label ------------------------------------------------------
#
# Transform names are just the definition file's stem, so a plan that runs one
# transform three times has three steps with the same name. The step number in
# the id is the only thing keeping them apart, and the label is free to drop it.


def test_same_named_steps_stay_distinct_nodes():
    r = DagRenderer()
    for i in (1, 2, 3):
        r.add_node(NodeKind.TRANSFORM, f"{i} checkm", Label(name="checkm"))
        r.add_edge(f"bins::from_binner_{i}", f"{i} checkm")
        r.add_edge(f"{i} checkm", f"qc::stats_{i}")
    assert len(r.layout().nodes) == 9
    assert r.to_text().count("checkm") == 3


def test_label_defaults_to_splitting_the_id_on_the_namespace():
    labels = DagRenderer()._labels  # noqa: F841 - documents the empty case
    r = DagRenderer()
    r.add_edge("assembly::contigs", "plain")
    assert r.labels["assembly::contigs"].namespace == "assembly"
    assert r.labels["assembly::contigs"].name == "contigs"
    assert r.labels["plain"].namespace == ""
    assert r.labels["plain"].full == "plain"


def test_dot_output_is_unchanged_for_nodes_whose_label_is_their_id():
    # consumers run their own graphviz over this; a redundant label= attribute
    # would churn their output for nothing
    r = DagRenderer()
    r.add_edge("a", "b")
    assert 'label=' not in r.to_dot()


def test_dot_carries_the_label_only_when_it_differs_from_the_id():
    r = DagRenderer()
    # what the plan caller does: the full form is the id, so plain DOT is
    # exactly what it was before labels existed
    r.add_node(NodeKind.TRANSFORM, "7 megahit",
               Label(name="megahit", full="7 megahit"))
    r.add_node(NodeKind.TRANSFORM, "8 bbduk", Label(name="bbduk"))
    dot = r.to_dot()
    assert '"7 megahit" [shape="oval", style="filled", fillcolor="#CCCCCC"]' in dot
    assert 'label="bbduk"' in dot  # full defaults to the name, which is shorter


def test_first_label_wins_like_the_kind_does():
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "x", Label(name="first"))
    r.add_node(NodeKind.TRANSFORM, "x", Label(name="second"))
    assert r.labels["x"].name == "first"


# --- markers and the label column -------------------------------------------


def test_svg_draws_the_namespace_above_the_name_at_half_size():
    r = DagRenderer()
    r.add_node(NodeKind.DATA, "assembly::contigs")
    svg = r.to_svg()
    ns = [l for l in svg.splitlines() if ">assembly<" in l][0]
    name = [l for l in svg.splitlines() if ">contigs<" in l][0]
    assert 'font-size="6.5"' in ns and 'font-size="13"' in name
    assert 'text-anchor="start"' in ns and 'text-anchor="start"' in name
    def _attr(line, key):
        return line.split(f'{key}="')[1].split('"')[0]

    assert float(_attr(ns, "y")) < float(_attr(name, "y"))
    assert _attr(ns, "x") == _attr(name, "x")  # same left edge


def test_long_names_are_clipped_but_stay_whole_on_hover():
    long = "x" * 60
    r = DagRenderer()
    r.add_node(NodeKind.DATA, f"ns::{long}")
    svg = r.to_svg()
    drawn = [l for l in svg.splitlines() if "…" in l][0]
    shown = drawn.split(">", 1)[1].split("</text>")[0]
    assert len(shown) == 32 and shown.endswith("…")
    assert f"<title>ns::{long}</title>" in svg


def test_label_column_is_narrower_than_labels_beside_every_marker():
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "given")
    for i in range(6):
        r.add_edge("given", f"a_very_long_type_name_number_{i}")
        r.add_edge(f"a_very_long_type_name_number_{i}", f"sink_{i}")

    def _w(mode):
        rr = DagRenderer(label_mode=mode)
        rr._nodes, rr._labels = r._nodes, r._labels
        rr._edges, rr._seen_edges = r._edges, r._seen_edges
        return float(rr.to_svg().split('width="')[1].split('"')[0])

    assert _w(LabelMode.COLUMN) < _w(LabelMode.BESIDE)


def test_lane_pitch_does_not_depend_on_label_length():
    # the whole point of moving the label out of the node: one long name must
    # not shove every branch to its right across the page
    def _lane_x(name):
        r = DagRenderer()
        r.add_node(NodeKind.TRANSFORM, "root")
        r.add_edge("root", name)
        r.add_edge("root", "other")
        svg = r.to_svg()
        return [
            float(l.split('cx="')[1].split('"')[0])
            for l in svg.splitlines() if "<circle" in l
        ]

    assert _lane_x("short") == _lane_x("a" * 40)


# --- format dispatch --------------------------------------------------------
#
# Placement is metasmith's; graphviz is only reached for raster formats, and
# only ever as a rasterizer of coordinates we computed.


def test_render_strips_suffix_and_uses_it_as_format(tmp_path):
    out = DagRenderer().render(tmp_path / "plan.dag.svg")
    assert out == tmp_path / "plan.dag.svg"
    assert out.exists()


def test_render_uses_format_arg_when_path_has_no_suffix(tmp_path):
    out = DagRenderer().render(tmp_path / "plan", format="dot")
    assert out == tmp_path / "plan.dot"


def test_render_creates_missing_parent_directories(tmp_path):
    out = DagRenderer().render(tmp_path / "nested" / "deeper" / "plan.svg")
    assert out.exists()


def test_dot_render_is_the_plain_graph(tmp_path):
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "step1")
    r.add_edge("input.fasta", "step1")
    written = r.render(tmp_path / "graph.dot").read_text()
    assert written.startswith("digraph G {")
    assert "pos=" not in written


def test_svg_needs_no_graphviz(tmp_path, monkeypatch):
    # importing graphviz on the default path would put a native dependency back
    # in front of every staged run
    import builtins
    real_import = builtins.__import__

    def _refuse(name, *a, **kw):
        assert not name.startswith("graphviz"), "svg path imported graphviz"
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", _refuse)
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "step1")
    r.add_edge("input.fasta", "step1")
    r.add_edge("step1", "output.gbk")
    out = r.render(tmp_path / "graph.svg")
    assert out.read_text().startswith("<?xml")


def test_svg_is_a_standalone_parsable_document(tmp_path):
    from xml.etree import ElementTree

    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "step <1> & co")
    r.add_edge("input.fasta", "step <1> & co")
    root = ElementTree.fromstring(r.render(tmp_path / "g.svg").read_text())
    assert root.tag.endswith("svg")
    labels = {e.text for e in root.iter() if e.tag.endswith("text")}
    assert "step <1> & co" in labels


def test_svg_distinguishes_the_three_kinds_by_shape():
    # by shape, not by colour: a step and a datum are both unfilled, so their
    # fill is the background's and a colour comparison would pass vacuously
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "step1")
    r.add_node(NodeKind.DATA, "thing")
    r.add_edge("thing", "step1")
    r.add_edge("step1", "wanted")
    r.mark(NodeKind.TARGET, "wanted")
    svg = r.to_svg()
    assert svg.count("<polygon") == 1  # the step, a triangle on its point
    assert svg.count("<circle") == 1  # the datum
    assert svg.count("<rect x=") == 1  # the target; the background rect has no x
    assert f'fill="{STYLES[NodeKind.TARGET].fill}"' in svg


def test_svg_draws_no_arrowheads():
    r = DagRenderer()
    r.add_edge("a", "b")
    svg = r.to_svg()
    assert "marker-end" not in svg
    assert "<marker" not in svg


def test_transform_labels_are_greyer_than_data_labels():
    assert STYLES[NodeKind.TRANSFORM].text != STYLES[NodeKind.DATA].text
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "step1")
    r.add_edge("thing", "step1")
    svg = r.to_svg()
    for kind in (NodeKind.TRANSFORM, NodeKind.DATA):
        assert f'fill="{STYLES[kind].text}"' in svg


def test_text_marks_the_two_kinds_differently():
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "step1")
    r.add_edge("thing", "step1")
    lines = r.to_text().splitlines()
    assert lines[0].startswith(STYLES[NodeKind.DATA].marker)
    assert lines[-1].startswith(STYLES[NodeKind.TRANSFORM].marker)


def test_text_ascii_fallback_is_seven_bit():
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "step1")
    r.add_edge("a", "step1")
    r.add_edge("b", "step1")
    r.add_edge("step1", "c")
    text = r.to_text(unicode=False)
    text.encode("ascii")  # raises if a box-drawing glyph leaked through


def test_text_render_writes_a_text_file(tmp_path):
    r = DagRenderer()
    r.add_edge("a", "b")
    out = r.render(tmp_path / "plan.text")
    assert out.read_text().splitlines()[0].endswith("a")


def test_raster_dot_pins_every_node_and_edge():
    r = DagRenderer()
    r.add_edge("a", "b")
    dot = r.to_raster_dot()
    # two markers, their two edge-free label nodes, and one edge
    assert dot.count("pos=") == 5
    assert '"a" -> "b"' in dot


def test_raster_label_nodes_are_separate_and_edge_free():
    # graphviz cannot position an xlabel or vary font size within one label, so
    # the label rides on its own pinned plaintext node
    r = DagRenderer()
    r.add_edge("ns::a", "ns::b")
    dot = r.to_raster_dot()
    assert '"__label__ns::a" [' in dot
    assert 'shape="plaintext"' in dot
    assert "__label__" not in dot.split("->")[1]  # never an edge endpoint
    assert 'POINT-SIZE="6.5"' in dot  # the half-size namespace line
    assert '<BR ALIGN="LEFT"/>' in dot


def test_raster_label_prefix_dodges_a_colliding_node_id():
    r = DagRenderer()
    r.add_edge("__label__x", "y")
    dot = r.to_raster_dot()
    # a bare "__label__x" would have been unified with the real node by name
    assert '"___label____label__x" [' in dot


@pytest.mark.skipif(shutil.which("neato") is None,
                    reason="graphviz `neato` binary not installed")
def test_raster_render_goes_through_neato(tmp_path):
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "step1")
    r.add_edge("input.fasta", "step1")
    r.add_edge("step1", "output.gbk")
    out = r.render(tmp_path / "graph.png")
    assert out.exists() and out.read_bytes()[:4] == b"\x89PNG"


def test_raster_without_neato_says_what_is_missing(tmp_path, monkeypatch):
    monkeypatch.setattr("metasmith.models.dag_draw.shutil.which", lambda _: None)
    with pytest.raises(RuntimeError, match="neato"):
        DagRenderer().render(tmp_path / "graph.png")


# --- height and 45 degree corners -------------------------------------------


def test_a_straight_chain_costs_one_line_per_node():
    r = DagRenderer()
    for a, b in zip("abcd", "bcde"):
        r.add_edge(a, b)
    assert len(r.to_text().rstrip("\n").splitlines()) == 5


def test_unconnected_neighbours_in_one_lane_keep_their_separator():
    # without the blank row these four read as a single chain, because the
    # second component reuses the lane the first one closed
    r = DagRenderer()
    r.add_edge("a", "b")
    r.add_edge("c", "d")
    assert r.to_text() == "○  a\n○  b\n\n○  c\n○  d\n"


def test_a_one_lane_jog_is_a_single_unbroken_curve():
    r = DagRenderer()
    r.add_edge("root", "left")
    r.add_edge("root", "right")
    r.add_edge("left", "join")
    r.add_edge("right", "join")
    for pts in _edge_paths(r.to_svg()):
        for (ax, ay), (bx, by) in zip(pts, pts[1:]):
            # the two quarter-circles meet, so every non-vertical run is the
            # chord of one of them and rises exactly as far as it travels
            if ax != bx:
                assert abs(abs(bx - ax) - abs(by - ay)) < 0.15, pts
        # ... and none of them is separated by a flat horizontal stub
        assert not any(ay == by and ax != bx for (ax, ay), (bx, by) in zip(pts, pts[1:]))


def test_a_multi_lane_jog_keeps_a_flat_run_between_two_curves():
    r = DagRenderer()
    for i in range(4):
        r.add_edge("root", f"child_{i}")
        r.add_edge(f"child_{i}", "join")
    flats = [
        (a, b)
        for pts in _edge_paths(r.to_svg())
        for a, b in zip(pts, pts[1:])
        if a[1] == b[1] and a[0] != b[0]
    ]
    assert flats, "a jog of several lanes should not collapse to one curve"


def test_every_corner_is_an_arc():
    r = DagRenderer()
    r.add_edge("root", "left")
    r.add_edge("root", "right")
    bent = [
        p for p in r.to_svg().splitlines()
        if p.startswith("<path") and len(_edge_paths(p)[0]) > 2
    ]
    assert bent, "the fan-out should have produced a jog"
    assert all(" A " in p for p in bent)
    assert all("A" not in p.split('d="')[1] for p in r.to_svg().splitlines()
               if p.startswith("<path") and p not in bent)


def test_the_two_directions_of_travel_land_in_different_bands():
    # a jog leaving a node and a jog merging into the next one share a half-row;
    # drawn at one y they overlay each other and neither has a direction
    r = DagRenderer()
    r.add_edge("a", "b")
    r.add_edge("a", "c")
    r.add_edge("b", "d")
    r.add_edge("c", "d")
    ys = {y for pts in _edge_paths(r.to_svg()) for _, y in pts}
    assert len(ys) > 4


def _edge_paths(svg: str) -> list[list[tuple[float, float]]]:
    """The endpoint of every command on each edge path.

    One pair per command, not every pair in the string: an `A` also carries its
    radii as an `r,r` token, and taking that as a point would put the curve
    somewhere near the origin.
    """
    out = []
    for line in svg.splitlines():
        if not line.startswith("<path"):
            continue
        points, pending = [], None
        for tok in line.split('d="')[1].split('"')[0].split():
            if tok[0].isalpha():
                if pending is not None:
                    points.append(pending)
                pending = None
            elif "," in tok:
                pending = tuple(map(float, tok.split(",")))
        if pending is not None:
            points.append(pending)
        out.append(points)
    return out


# --- golden text ------------------------------------------------------------
#
# Fed explicit graphs rather than routed through WorkflowPlan, so a change in
# an unrelated caller cannot break them.


def _build(nodes, edges) -> DagRenderer:
    r = DagRenderer()
    for kind, name in nodes:
        r.add_node(kind, name)
    for src, dst in edges:
        r.add_edge(src, dst)
    return r


T, D = NodeKind.TRANSFORM, NodeKind.DATA


def test_golden_chain():
    r = _build(
        [(D, "reads"), (T, "bbduk"), (D, "clean"), (T, "megahit"), (D, "contigs")],
        [("reads", "bbduk"), ("bbduk", "clean"),
         ("clean", "megahit"), ("megahit", "contigs")],
    )
    # a gap where every rail runs straight through costs no row at all
    assert r.to_text() == (
        "○  reads\n"
        "▽  bbduk\n"
        "○  clean\n"
        "▽  megahit\n"
        "○  contigs\n"
    )


def test_golden_diamond():
    r = _build(
        [(D, "a"), (T, "l"), (T, "r"), (D, "j")],
        [("a", "l"), ("a", "r"), ("l", "j"), ("r", "j")],
    )
    # lane 0 is drawn rightmost, against the labels, so the fan opens leftwards
    assert r.to_text() == (
        "  ○  a\n"
        "┌─┤\n"
        "│ ▽  l\n"
        "▽ │  r\n"
        "└─┤\n"
        "  ○  j\n"
    )


def test_golden_three_way_fan_in():
    r = _build(
        [(D, "contigs"), (T, "metabat2"), (T, "semibin2"), (T, "comebin"),
         (T, "checkm2"), (D, "qc")],
        [("contigs", "metabat2"), ("contigs", "semibin2"), ("contigs", "comebin"),
         ("metabat2", "checkm2"), ("semibin2", "checkm2"), ("comebin", "checkm2"),
         ("checkm2", "qc")],
    )
    # semibin2 last: it is the one carrying the chain below the join, so the
    # two that end at the join are drawn first and free their lanes
    assert r.to_text() == (
        "    ○  contigs\n"
        "┌─┬─┤\n"
        "│ ▽ │  metabat2\n"
        "│ │ ▽  comebin\n"
        "▽ │ │  semibin2\n"
        "└─┴─┤\n"
        "    ▽  checkm2\n"
        "    ○  qc\n"
    )


def test_golden_wide_fan_out():
    # the `given` super-node shape: width tracks the fan-out, and long type
    # names push the label column right without wrapping
    r = _build(
        [(T, "given")] + [(D, f"std::input_{i}") for i in range(4)],
        [("given", f"std::input_{i}") for i in range(4)],
    )
    assert r.to_text() == (
        "      ▽  given\n"
        "┌─┬─┬─┤\n"
        "│ │ │ ○  std::input_0\n"
        "│ │ ○    std::input_1\n"
        "│ ○      std::input_2\n"
        "○        std::input_3\n"
    )
