"""Colour schemes over a drawn layout.

Colour is off by default and every scheme is a pure function of the finished
layout, so these read the maps directly and then check that each backend
actually carries one.
"""
import pytest

from metasmith.models.dag_colour import (
    PALETTE, SCHEMES, UNMATCHED, Colouring, colour_layout,
)
from metasmith.models.dag_layout import repeat_motifs
from metasmith.models.dag_renderer import DARK, LIGHT, STYLES, DagRenderer, NodeKind

from tests.metasmith.fixtures import load_dag

T, D = NodeKind.TRANSFORM, NodeKind.DATA


def _repeats() -> DagRenderer:
    """Two copies of one three-step block hanging off one input."""
    r = DagRenderer()
    r.add_node(D, "src")
    for tag in ("a", "b"):
        r.add_node(T, f"1{tag} split", )
        r.add_edge("src", f"1{tag} split")
        r.add_edge(f"1{tag} split", f"out::{tag}_part")
        r.add_edge(f"1{tag} split", f"out::{tag}_log")
        r.add_node(T, f"2{tag} merge")
        r.add_edge(f"out::{tag}_part", f"2{tag} merge")
        r.add_edge(f"2{tag} merge", f"out::{tag}_final")
    return r


# --- the default ------------------------------------------------------------


def test_no_scheme_is_the_default_and_leaves_the_styles_alone():
    r = DagRenderer()
    r.add_node(T, "step")
    r.add_edge("thing", "step")
    assert not r.colouring()
    svg = r.to_svg()
    assert f'stroke="{STYLES[NodeKind.DATA].stroke}"' in svg
    assert not any(hue in svg for hue in PALETTE)


def test_an_unknown_scheme_is_an_error_not_a_grey_drawing():
    with pytest.raises(ValueError, match="unknown colour scheme"):
        DagRenderer(colour="rainbow")
    with pytest.raises(ValueError, match="unknown colour scheme"):
        colour_layout(load_dag().layout(), "rainbow")


def test_every_named_scheme_colours_every_node():
    lay = load_dag().layout()
    for scheme in SCHEMES:
        c = colour_layout(lay, scheme)
        if scheme == "none":
            assert not c
            continue
        assert set(c.nodes) == {n.name for n in lay.nodes}, scheme
        assert all(v.startswith("#") for v in c.nodes.values()), scheme


# --- what each scheme means -------------------------------------------------


def test_lane_gives_neighbouring_lanes_different_hues():
    # GitHub's reading: colour is for tracing one rail, so adjacent lanes must
    # not agree. It says nothing at all about repeats, which is the point of
    # having the other scheme beside it.
    lay = load_dag().layout()
    c = colour_layout(lay, "lane")
    by_lane = {n.lane: c.nodes[n.name] for n in lay.nodes}
    for lane in sorted(by_lane)[:-1]:
        if lane + 1 in by_lane:
            assert by_lane[lane] != by_lane[lane + 1], lane
    assert all(c.nodes[n.name] == by_lane[n.lane] for n in lay.nodes)


def test_repeat_paints_every_instance_of_a_motif_the_same():
    # the opposite of graph colouring, and the only scheme that helps with the
    # thing the drawing is bad at
    lay = _repeats().layout()
    motifs = repeat_motifs(lay)
    assert motifs, "the fixture should repeat"
    c = colour_layout(lay, "repeat")
    for m in motifs:
        hues = {c.nodes[x] for x in m.nodes}
        assert len(hues) == 1, m.heads
        assert hues != {UNMATCHED}
    outside = {n.name for n in lay.nodes} - set().union(*(m.nodes for m in motifs))
    assert all(c.nodes[x] == UNMATCHED for x in outside)


def test_repeat_leaves_a_graph_with_no_repeats_entirely_grey():
    r = DagRenderer()
    for a, b in zip("abcd", "bcde"):
        r.add_edge(a, b)
    assert set(colour_layout(r.layout(), "repeat").nodes.values()) == {UNMATCHED}


def test_module_gives_touching_modules_different_hues():
    # the graph-colouring reading: it answers "where does this block end",
    # which is the opposite question to the one `repeat` answers
    from metasmith.models.dag_colour import _by_module, _module_owner

    lay = load_dag().layout()
    hue, owner = _by_module(lay), _module_owner(lay)
    touching = {
        (owner[e.src], owner[e.dst])
        for e in lay.edges
        if not e.back and owner[e.src] != owner[e.dst]
        and owner[e.src] is not None and owner[e.dst] is not None
    }
    assert touching
    for a, b in touching:
        assert hue[a] != hue[b], (a, b)
    assert 1 < len({v for v in hue.values() if v != UNMATCHED}) <= len(PALETTE)


def test_namespace_follows_the_prefix_and_greys_what_has_none():
    r = DagRenderer()
    r.add_edge("ns::a", "ns::b")
    r.add_edge("ns::b", "other::c")
    r.add_edge("other::c", "bare")
    c = colour_layout(r.layout(), "namespace")
    assert c.nodes["ns::a"] == c.nodes["ns::b"]
    assert c.nodes["ns::a"] != c.nodes["other::c"]
    assert c.nodes["bare"] == UNMATCHED


def test_an_edge_takes_its_source_colour():
    # the rail is shared property; the source is the end the reader's eye is
    # already on, so a fan-in arrives in as many colours as it has inputs
    lay = load_dag().layout()
    c = colour_layout(lay, "lane")
    for e in lay.edges:
        if e.back:
            continue
        assert c.edges[(e.src, e.dst)] == c.nodes[e.src]


# --- the backends carry it --------------------------------------------------


def test_svg_puts_the_hue_on_the_marker_and_the_rail():
    r = load_dag(colour="lane")
    svg = r.to_svg()
    hues = set(r.colouring().nodes.values())
    assert hues <= set(PALETTE)
    for hue in hues:
        # a target carries the hue on its fill, not its outline
        assert f'stroke="{hue}"' in svg or f'fill="{hue}"' in svg
    assert any(f'<path d=' in l and 'stroke="#' in l for l in svg.splitlines())


def test_a_colour_scheme_tints_the_outline_of_a_hollow_marker_and_the_fill_of_a_solid_one():
    r = DagRenderer(colour="namespace")
    r.add_node(NodeKind.DATA, "ns::thing")
    r.add_edge("ns::thing", "ns::wanted")
    r.mark(NodeKind.TARGET, "ns::wanted")
    hue = r.colouring().nodes["ns::thing"]
    svg = r.to_svg()
    # both are circles, so they are told apart by which one is filled -- which
    # is exactly the distinction `Style.solid` names and `tint` reads
    circles = [l for l in svg.splitlines() if l.startswith("<circle")]
    hollow = [l for l in circles if f'fill="{STYLES[NodeKind.DATA].fill}"' in l][0]
    solid = [l for l in circles if f'fill="{hue}"' in l][0]
    assert f'stroke="{hue}"' in hollow  # hollow: hue on the outline
    assert f'stroke="{STYLES[NodeKind.TARGET].stroke}"' in solid  # solid: on the fill


def test_raster_dot_carries_the_hue_on_nodes_and_edges():
    r = load_dag(colour="repeat")
    dot = r.to_raster_dot()
    hues = {v for v in r.colouring().nodes.values() if v != UNMATCHED}
    assert hues
    for hue in hues:
        assert f'color="{hue}"' in dot


def test_text_only_colours_when_asked_and_stays_plain_otherwise():
    r = load_dag(colour="lane")
    assert "\033[" not in r.to_text()
    assert "\033[38;2;" in r.to_text(color=True)  # 24-bit, from the scheme


def test_a_scheme_does_not_move_anything():
    # colour is applied to a finished layout; it must not be able to change one
    plain, painted = load_dag(), load_dag(colour="repeat")
    assert plain.layout() == painted.layout()


def test_the_colouring_is_deterministic():
    lay = load_dag().layout()
    for scheme in SCHEMES:
        assert colour_layout(lay, scheme) == colour_layout(lay, scheme)


def test_an_empty_colouring_is_falsey():
    assert not Colouring()
    assert Colouring(nodes={"a": "#000000"})


# -- one palette, either ground ----------------------------------------------


def _luminance(hexcolour: str) -> float:
    def channel(v):
        v = int(v, 16) / 255
        return v / 12.92 if v <= 0.04045 else ((v + 0.055) / 1.055) ** 2.4

    h = hexcolour.lstrip("#")
    r, g, b = (channel(h[i:i + 2]) for i in (0, 2, 4))
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def _contrast(a: str, b: str) -> float:
    la, lb = sorted((_luminance(a), _luminance(b)))
    return (lb + 0.05) / (la + 0.05)


def test_one_palette_reads_on_either_ground():
    # `PALETTE` is deliberately not themed: these are mid-saturation hues chosen
    # to sit on white *and* stay distinguishable, and a second set would be two
    # things to keep in agreement. What justifies the one set is that a hue
    # already accepted on white is *strictly safer* on the dark ground -- so
    # this asserts the relation, not an absolute floor the light plate itself
    # does not meet.
    for hue in list(PALETTE) + [UNMATCHED]:
        light = _contrast(hue, LIGHT.plate.background)
        dark = _contrast(hue, DARK.plate.background)
        assert dark >= light, (hue, light, dark)
        assert dark >= 3.0, (hue, dark)


def test_the_colouring_does_not_depend_on_the_theme():
    # colour is a pure function of the finished layout and a theme is a property
    # of the ground; neither may learn about the other
    assert (
        load_dag(colour="module").colouring().nodes
        == load_dag(colour="module", theme="dark").colouring().nodes
    )
