import shutil
from pathlib import Path

import pytest

from metasmith.models.dag_renderer import DagRenderer, NodeKind


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


def test_render_strips_suffix_and_uses_it_as_format(tmp_path, monkeypatch):
    captured = {}

    class _FakeSource:
        def __init__(self, dot, filename, format):
            captured["filename"] = filename
            captured["format"] = format

        def render(self, **kw):
            Path(captured["filename"] + "." + captured["format"]).write_text("x")

    class _FakeGraphviz:
        Source = _FakeSource

    monkeypatch.setattr(
        "metasmith.models.dag_renderer._import_graphviz_quietly",
        lambda: _FakeGraphviz,
    )
    out = DagRenderer().render(tmp_path / "plan.dag.svg")
    assert captured["format"] == "svg"
    assert captured["filename"] == str(tmp_path / "plan.dag")
    assert out == tmp_path / "plan.dag.svg"


def test_render_uses_format_arg_when_path_has_no_suffix(tmp_path, monkeypatch):
    captured = {}

    class _FakeSource:
        def __init__(self, dot, filename, format):
            captured["format"] = format

        def render(self, **kw):
            pass

    class _FakeGraphviz:
        Source = _FakeSource

    monkeypatch.setattr(
        "metasmith.models.dag_renderer._import_graphviz_quietly",
        lambda: _FakeGraphviz,
    )
    DagRenderer().render(tmp_path / "plan", format="pdf")
    assert captured["format"] == "pdf"


@pytest.mark.skipif(shutil.which("dot") is None,
                    reason="graphviz `dot` binary not installed")
def test_render_writes_svg(tmp_path):
    r = DagRenderer()
    r.add_node(NodeKind.TRANSFORM, "step1")
    r.add_edge("input.fasta", "step1")
    r.add_edge("step1", "output.gbk")
    out = r.render(tmp_path / "graph.svg")
    assert out.exists()
    head = out.read_text()[:200]
    assert head.startswith("<?xml") or head.startswith("<svg")
