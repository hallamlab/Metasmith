import shutil
from pathlib import Path

import pytest

from tests.metasmith.fixtures import load_dag

from metasmith.models.dag_draw import geometry, marker_size
from metasmith.models.dag_renderer import (
    DARK, STYLES, THEMES, DagRenderer, Label, LabelMode, NodeKind,
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


def test_svg_distinguishes_a_step_by_shape_and_a_target_by_fill():
    # a step is a shape of its own; a target is not. It is the same circle as
    # the datum it is, drawn solid -- so this is the one kind distinguished by
    # its fill, and the two hollow ones share the background's fill
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "step1")
    r.add_node(NodeKind.DATA, "thing")
    r.add_edge("thing", "step1")
    r.add_edge("step1", "wanted")
    r.mark(NodeKind.TARGET, "wanted")
    svg = r.to_svg()
    assert svg.count("<polygon") == 1  # the step, a triangle on its point
    assert svg.count("<circle") == 2  # the datum and the target
    assert svg.count("<rect x=") == 0  # nothing is a box; the background has no x
    assert f'fill="{STYLES[NodeKind.TARGET].fill}"' in svg
    assert STYLES[NodeKind.TARGET].fill != STYLES[NodeKind.DATA].fill


def _marker_widths(svg: str) -> dict[str, float]:
    """The drawn width of each marker shape in an SVG, by shape name."""
    def _attr(line, key):
        return float(line.split(f'{key}="')[1].split('"')[0])

    out = {}
    for line in svg.splitlines():
        if line.startswith("<circle"):
            out["circle"] = 2 * _attr(line, "r")
        elif line.startswith("<rect x="):
            out["square"] = _attr(line, "width")  # nothing draws one today
        elif line.startswith("<polygon"):
            xs = [
                float(p.split(",")[0])
                for p in line.split('points="')[1].split('"')[0].split()
            ]
            out["triangle"] = max(xs) - min(xs)
    return out


def _three_kinds() -> DagRenderer:
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "step1")
    r.add_node(NodeKind.DATA, "thing")
    r.add_edge("thing", "step1")
    r.add_edge("step1", "wanted")
    r.mark(NodeKind.TARGET, "wanted")
    return r


def test_every_marker_draws_at_one_width():
    # they were 1.25 of a circumradius, 0.90 of a diameter and 0.82 of a side —
    # three different quantities, drawing a 17px triangle beside a 10px circle
    w = _marker_widths(_three_kinds().to_svg())
    assert set(w) == {"circle", "triangle"}
    # the SVG carries one decimal, so equal widths can still differ by 0.1
    assert max(w.values()) - min(w.values()) < 0.11, w


def test_the_triangle_is_equilateral_and_so_shorter_than_it_is_wide():
    svg = _three_kinds().to_svg()
    line = [l for l in svg.splitlines() if l.startswith("<polygon")][0]
    pts = [
        tuple(map(float, p.split(",")))
        for p in line.split('points="')[1].split('"')[0].split()
    ]
    width = max(x for x, _ in pts) - min(x for x, _ in pts)
    height = max(y for _, y in pts) - min(y for _, y in pts)
    assert abs(height / width - 0.866) < 0.01


def test_only_the_solid_target_outline_is_double_weight():
    # an intermediate is not louder than the step that made it, so the hollow
    # circle carries the triangle's weight; the target is the one a reader is
    # hunting for and keeps the heavier outline on top of its solid fill
    step = STYLES[NodeKind.TRANSFORM]
    data = STYLES[NodeKind.DATA]
    target = STYLES[NodeKind.TARGET]
    assert data.stroke_width == step.stroke_width == 1.5
    assert target.stroke_width == 3.0
    r = _three_kinds()
    svg = r.to_svg()
    assert f'stroke-width="{data.stroke_width}"' in svg
    assert f'stroke-width="{target.stroke_width}"' in svg
    dot = r.to_raster_dot()
    assert f"penwidth={target.stroke_width:g}" in dot
    assert f"penwidth={step.stroke_width:g}" in dot
    # the pinned global that made every PNG outline the same weight
    assert "penwidth=1.2]" not in dot


def test_a_rail_stops_at_the_shape_it_points_at():
    # a triangle is shorter than it is wide, so trimming every endpoint by one
    # marker radius leaves a gap under it and overshoots into a square
    from metasmith.models import dag_draw as dd

    r = _three_kinds()
    lay = r.layout()
    g, _ = dd._grid(lay, font_size=13.0, labels=r.labels)
    tri = dd.marker_size(STYLES[NodeKind.TRANSFORM], g.marker_d)[1] / 2
    circ = dd.marker_size(STYLES[NodeKind.DATA], g.marker_d)[1] / 2
    into_step = next(e for e in lay.edges if e.dst == "step1")
    pts = dd._pixel_path(lay, into_step, g, STYLES)[0]
    assert abs(pts[0][1] - (g.y(lay["thing"].row) + circ)) < 0.05
    assert abs(pts[-1][1] - (g.y(lay["step1"].row) - tri)) < 0.05
    assert tri < circ  # the whole reason one radius would not do


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


def test_a_jog_is_banded_by_the_way_it_travels():
    # the band used to be read off the half-row -- departure above, arrival
    # below -- which only agrees with the direction while the rail lane is
    # outside both endpoints' lanes. Here it is not, and the two readings
    # disagree.
    from metasmith.models import dag_draw as dd

    r = _build(
        [(D, "a"), (T, "l"), (T, "r"), (D, "j")],
        [("a", "l"), ("a", "r"), ("l", "j"), ("r", "j")],
    )
    lay = r.layout()
    g, _ = dd._grid(lay, font_size=13.0, labels=r.labels)
    e = next(x for x in lay.edges if (x.src, x.dst) == ("r", "j"))
    assert lay["j"].lane < lay["r"].lane  # it runs back inwards, to the right

    pts = dd._pixel_path(lay, e, g, STYLES)[0]
    lo, hi = sorted((g.x(lay["r"].lane), g.x(lay["j"].lane)))
    band = [y for x, y in pts if lo < x < hi]
    assert band, "the jog should have left a point between the two lanes"
    assert all(y > g.y(lay["r"].row + 0.5) for y in band)  # rightward: under

    # ... and one running the other way sits in the other band
    down = next(x for x in lay.edges if (x.src, x.dst) == ("a", "r"))
    assert down.lane > lay["a"].lane  # leaves lane 0 outwards, to the left
    pts = dd._pixel_path(lay, down, g, STYLES)[0]
    lo, hi = sorted((g.x(lay["a"].lane), g.x(down.lane)))
    band = [y for x, y in pts if lo < x < hi]
    assert band and all(y < g.y(lay["a"].row + 0.5) for y in band)


def test_rails_travelling_the_same_way_share_one_line():
    """Two jogs crossing the same gap in the same direction line up.

    A fan-out's children and a fan-in's parents can land in the same gap, and
    banding them by role rather than by direction put one line of a converging
    fan a band away from the rest of it -- visible as a seam. Nothing here is
    about which band is which: only that two rails a reader sees as running
    together are drawn at one height.
    """
    from metasmith.models import dag_draw as dd

    r = load_dag()
    lay = r.layout()
    g, _ = dd._grid(lay, font_size=13.0, labels=r.labels)

    runs = {}
    for e in lay.edges:
        pts = [
            (g.x(lane), g.y(row) + role * dd.BAND * g.gap(row))
            for role, (row, lane) in zip(dd._jog_roles(lay, e), e.points)
        ]
        for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
            if y0 != y1 or x0 == x1:
                continue
            runs.setdefault((round(y0, 6), x1 < x0), []).append(f"{e.src}->{e.dst}")

    by_gap = {}
    for (y, leftward), names in runs.items():
        by_gap.setdefault((round(y / g.row_pitch), leftward), set()).add(y)
    split = {k: v for k, v in by_gap.items() if len(v) > 1}
    assert not split, f"same-direction rails at different heights: {split}"


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


# -- themes ------------------------------------------------------------------


def _plate_of(svg: str) -> str:
    """The background rect's fill -- the only rect with no `x`."""
    line = [l for l in svg.splitlines() if l.startswith("<rect width=")][0]
    return line.split('fill="')[1].split('"')[0]


def test_light_is_the_default_and_is_what_was_always_drawn():
    # the assertion that matters most: every rendering already on disk was made
    # by a caller that passed no theme, and this is the one that ages badly
    assert load_dag().to_svg() == load_dag(theme="light").to_svg()
    svg = load_dag().to_svg()
    assert _plate_of(svg) == "#FFFFFF"
    assert 'stroke="#666666"' in svg


def test_dark_repaints_the_ground_and_leaves_the_geometry_alone():
    light, dark = load_dag().to_svg(), load_dag(theme="dark").to_svg()
    assert _plate_of(dark) == DARK.plate.background
    assert f'stroke="{DARK.plate.edge}"' in dark
    assert "#FFFFFF" not in dark  # no light ink survived onto the dark ground
    # a theme is ink, never placement: same viewBox, same rails, same markers
    assert _edge_paths(light) == _edge_paths(dark)
    assert _marker_widths(light) == _marker_widths(dark)
    assert (
        [l for l in light.splitlines() if l.startswith("<svg")]
        == [l for l in dark.splitlines() if l.startswith("<svg")]
    )


def test_the_two_themes_differ_only_in_colour():
    # `replace` off the light style is what guarantees this: a marker shape, a
    # scale or a stroke weight cannot drift between the two drawings
    colours = {"fill", "stroke", "text", "muted"}
    for kind, light in STYLES.items():
        dark = DARK.styles[kind]
        for f in light.__dataclass_fields__:
            if f in colours:
                continue
            assert getattr(dark, f) == getattr(light, f), (kind, f)


def test_an_unknown_theme_raises_like_an_unknown_scheme():
    with pytest.raises(ValueError, match="unknown theme"):
        DagRenderer(theme="twilight")


def test_the_raster_path_pins_its_own_background():
    # graphviz would otherwise use whatever the local build defaults to, which
    # is the one way a PNG can disagree with the SVG about what it sits on
    assert 'bgcolor="#FFFFFF"' in load_dag().to_raster_dot()
    dark = load_dag(theme="dark").to_raster_dot()
    assert f'bgcolor="{DARK.plate.background}"' in dark
    assert f'color="{DARK.plate.edge}"' in dark


def test_the_text_backend_is_theme_independent():
    # a terminal owns its own background and the only colour here is an ANSI
    # escape chosen against the reader's palette, so there is nothing to theme
    for kw in ({}, {"color": True}, {"unicode": False}):
        assert load_dag().to_text(**kw) == load_dag(theme="dark").to_text(**kw)


def test_every_theme_renders_every_scheme():
    for theme in THEMES:
        for scheme in ("none", "module"):
            assert load_dag(theme=theme, colour=scheme).to_svg().startswith("<?xml")


class TestGeometryIsWhatTheSvgDraws:
    """`render_svg` is written in terms of `geometry`, and must stay so.

    The extraction exists so a second consumer -- the GUI's info panel, which
    draws clickable buttons over the same edge layer -- gets the placement this
    module already computes instead of running a layout engine of its own. What
    keeps the two from drifting is that the SVG is not an independent drawing
    of the same numbers: it *is* these numbers. That is what is asserted here.
    """

    def _geo(self, r):
        lay = r.layout()
        geo = geometry(lay, r._theme.styles, labels=r.labels, label_mode=r._label_mode)
        return geo, r.to_svg()

    def test_the_canvas_is_the_geometry_canvas(self):
        g, svg = self._geo(load_dag())
        assert f'width="{g.width:.0f}" height="{g.height:.0f}"' in svg

    def test_every_edge_path_is_drawn_verbatim(self):
        g, svg = self._geo(load_dag())
        drawn = [e for e in g.edges if not e.back]
        assert drawn, "the fixture has forward edges"
        for e in drawn:
            assert f'd="{e.d}"' in svg

    def test_a_back_edge_carries_no_path_and_is_not_drawn(self):
        # the SVG skips them; a geometry entry with an empty `d` is how a
        # consumer is told the edge exists without being handed a line to draw
        g, _ = self._geo(load_dag())
        assert all(e.d == "" for e in g.edges if e.back)

    def test_every_node_label_lands_at_its_geometry_position(self):
        g, svg = self._geo(load_dag())
        for n in g.nodes:
            assert f'x="{n.label_x:.1f}" y="{n.cy + 0.36 * 13.0:.1f}"' in svg

    def test_a_marker_size_follows_its_style(self):
        g, _ = self._geo(load_dag())
        by_kind = {n.kind: n for n in g.nodes}
        for kind, n in by_kind.items():
            assert (n.marker_w, n.marker_h) == marker_size(STYLES[kind], g.marker_d)

    def test_both_label_modes_place_labels_differently(self):
        # BESIDE widens each lane to its own labels; COLUMN pins them all at one
        # x. A consumer picks by how much room it has, so both must work.
        col, _ = self._geo(load_dag(label_mode=LabelMode.COLUMN))
        bes, _ = self._geo(load_dag(label_mode=LabelMode.BESIDE))
        assert col.anchor == "start" and bes.anchor == "end"
        assert len({n.label_x for n in col.nodes}) == 1
        assert len({n.label_x for n in bes.nodes}) > 1
