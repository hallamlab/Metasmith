"""The drawing code against a real plan: the spanish-lakes metagenomics
workflow, 73 nodes and 100 edges, six reference-database roots, and three
binners each carrying their own checkm and gtdbtk step.

Invariants rather than goldens — a 73-row rendering pinned character by
character would be rewritten by every tuning change and read by nobody. The
graph comes from a committed fixture, so no solver, transform library or
sibling checkout is on this path; see `fixtures/generate_stress_dag.py`.
"""
from xml.etree import ElementTree

import pytest

from metasmith.models.dag_renderer import LabelMode, NodeKind

from .fixtures import load_dag


@pytest.fixture(scope="module")
def dag():
    return load_dag()


def test_the_fixture_is_the_shape_we_think_it_is(dag):
    lay = dag.layout()
    assert len(lay.nodes) == 73
    assert len(lay.edges) == 100


def test_repeated_transforms_stay_separate_steps(dag):
    # the whole reason node identity is not the drawn label: all three of these
    # are named `checkm`, and folding them would cost the graph six real nodes
    ids = {n.name for n in dag.layout().nodes}
    for name in ("checkm", "gtdbtk"):
        assert len({i for i in ids if i.endswith(f" {name}")}) == 3
    labels = dag.labels
    assert sum(1 for i in ids if labels[i].name == "checkm") == 3


def test_no_step_number_reaches_the_page(dag):
    for node in dag.layout().nodes:
        assert not dag.labels[node.name].name[:1].isdigit()


def test_every_edge_still_points_downward(dag):
    lay = dag.layout()
    idx = lay.index
    for e in lay.edges:
        if not e.back:
            assert idx[e.src].row < idx[e.dst].row


def test_no_rail_crosses_a_node(dag):
    # the invariant the character grid and the pixel grid both rely on
    lay = dag.layout()
    idx = lay.index
    occupied = {(n.row, n.lane) for n in lay.nodes}
    for e in lay.edges:
        if e.back:
            continue
        for row in range(idx[e.src].row + 1, idx[e.dst].row):
            assert (row, e.lane) not in occupied


def test_lane_count_is_the_liveness_floor(dag):
    lay = dag.layout()
    idx = lay.index
    live = max(
        sum(1 for e in lay.edges
            if not e.back and idx[e.src].row < row < idx[e.dst].row)
        for row in range(lay.height)
    )
    assert lay.width <= live + 1


def test_dropping_the_blank_gaps_is_most_of_the_height(dag):
    lines = dag.to_text().rstrip("\n").splitlines()
    assert len(lines) < 2 * len(dag.layout().nodes)


def test_both_svgs_parse_and_the_label_column_is_much_narrower():
    def _svg(mode):
        r = load_dag(label_mode=mode)
        doc = r.to_svg()
        ElementTree.fromstring(doc)  # raises if malformed
        return float(doc.split('width="')[1].split('"')[0])

    column, beside = _svg(LabelMode.COLUMN), _svg(LabelMode.BESIDE)
    assert column < beside / 3


def test_no_drawn_name_runs_past_the_bound_and_the_rest_is_on_hover(dag):
    svg = dag.to_svg()
    root = ElementTree.fromstring(svg)
    drawn = [e.text or "" for e in root.iter() if e.tag.endswith("text")]
    assert drawn
    assert all(len(t) <= 32 for t in drawn)
    titles = {e.text for e in root.iter() if e.tag.endswith("title")}
    for t in drawn:
        if t.endswith("…"):
            assert any(x.endswith(t[:-1] + t[:0]) or t[:-1] in x for x in titles)


def test_the_requested_outputs_are_marked_on_the_nodes(dag):
    kinds = {k for k in dag._nodes.values()}
    assert NodeKind.TARGET in kinds
    assert "target" not in dag._nodes  # no synthetic sink holding a lane each


def test_rendering_is_deterministic(dag):
    assert load_dag().to_svg() == load_dag().to_svg()
    assert load_dag().to_text() == load_dag().to_text()
