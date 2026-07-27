import shutil
from pathlib import Path

import pytest

from metasmith.models.dag_renderer import STYLES, DagRenderer, NodeKind


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


def test_svg_distinguishes_transform_from_data():
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "step1")
    r.add_node(NodeKind.DATA, "thing")
    r.add_edge("thing", "step1")
    svg = r.to_svg()
    assert STYLES[NodeKind.TRANSFORM].fill in svg
    assert STYLES[NodeKind.DATA].fill in svg


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
    assert dot.count("pos=") == 3  # two nodes, one edge
    assert '"a" -> "b"' in dot


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
    assert r.to_text() == (
        "○  reads\n"
        "│\n"
        "●  bbduk\n"
        "│\n"
        "○  clean\n"
        "│\n"
        "●  megahit\n"
        "│\n"
        "○  contigs\n"
    )


def test_golden_diamond():
    r = _build(
        [(D, "a"), (T, "l"), (T, "r"), (D, "j")],
        [("a", "l"), ("a", "r"), ("l", "j"), ("r", "j")],
    )
    assert r.to_text() == (
        "○    a\n"
        "├─┐\n"
        "● │  l\n"
        "│ │\n"
        "│ ●  r\n"
        "├─┘\n"
        "○    j\n"
    )


def test_golden_three_way_fan_in():
    r = _build(
        [(D, "contigs"), (T, "metabat2"), (T, "semibin2"), (T, "comebin"),
         (T, "checkm2"), (D, "qc")],
        [("contigs", "metabat2"), ("contigs", "semibin2"), ("contigs", "comebin"),
         ("metabat2", "checkm2"), ("semibin2", "checkm2"), ("comebin", "checkm2"),
         ("checkm2", "qc")],
    )
    assert r.to_text() == (
        "○      contigs\n"
        "├─┬─┐\n"
        "● │ │  comebin\n"
        "│ │ │\n"
        "│ ● │  metabat2\n"
        "│ │ │\n"
        "│ │ ●  semibin2\n"
        "├─┴─┘\n"
        "●      checkm2\n"
        "│\n"
        "○      qc\n"
    )


def test_golden_wide_fan_out():
    # the `given` super-node shape: width tracks the fan-out, and long type
    # names push the label column right without wrapping
    r = _build(
        [(T, "given")] + [(D, f"std::input_{i}") for i in range(4)],
        [("given", f"std::input_{i}") for i in range(4)],
    )
    assert r.to_text() == (
        "●        given\n"
        "├─┬─┬─┐\n"
        "○ │ │ │  std::input_0\n"
        "  │ │ │\n"
        "  ○ │ │  std::input_1\n"
        "    │ │\n"
        "    ○ │  std::input_2\n"
        "      │\n"
        "      ○  std::input_3\n"
    )
