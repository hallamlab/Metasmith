from xml.etree import ElementTree

import pytest

from metasmith.models.dag_layout import measure, repeat_motifs
from metasmith.models.dag_renderer import DagMode, LabelMode, NodeKind

from tests.metasmith.fixtures import load_dag


@pytest.fixture(scope="module")
def dag():
    return load_dag()


def test_the_fixture_is_the_shape_we_think_it_is(dag):
    lay = dag.layout()
    assert len(lay.order) == 73
    assert len(lay.edges) == 100


def test_repeated_transforms_stay_separate_steps(dag):
    ids = set(dag.layout().order)
    for name in ("checkm", "gtdbtk"):
        assert len({i for i in ids if i.endswith(f" {name}")}) == 3
    assert sum(1 for i in ids if dag.labels[i].name == "checkm") == 3


def test_no_step_number_reaches_the_page(dag):
    for name in dag.layout().order:
        assert not dag.labels[name].name[:1].isdigit()


def test_every_edge_still_points_downward(dag):
    lay = dag.layout()
    assert all(lay.row[s] < lay.row[d] for s, d in lay.edges)


def test_the_width_is_the_liveness_floor(dag):
    lay = dag.layout()
    assert lay.width == max(len(lay.live(h)) for h in range(-1, 2 * lay.height))


def test_dropping_the_blank_gaps_is_most_of_the_height(dag):
    lines = dag.to_text().rstrip("\n").splitlines()
    assert len(lines) < 2 * dag.layout().height


def test_both_svgs_parse_and_the_label_column_is_much_narrower():
    def _svg(mode):
        r = load_dag(label_mode=mode)
        doc = r.to_svg()
        ElementTree.fromstring(doc)
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
    assert "target" not in dag._nodes


def test_the_drawing_does_not_get_more_expensive(dag):
    # Release drew this graph in 549 rail rows, 14 lanes and 242 crossings.
    # The row order is proven shortest under the step blocks; the columns are
    # not proven, so a solver change can move those either way.
    m = measure(dag.layout())
    assert m.optimal
    assert m.length <= 457
    assert m.width <= 7
    assert m.crossings <= 2
    assert m.hlen <= 77
    assert m.congruent >= 3


@pytest.mark.parametrize("mode", [DagMode.PLAIN, DagMode.COLLAPSED, DagMode.STEPS])
def test_every_step_has_its_own_outputs_directly_below_it(mode):
    r = load_dag(mode=mode)
    nodes, edges = r._graph()
    lay = r.layout()
    producers = {}
    for src, dst in edges:
        producers.setdefault(dst, set()).add(src)
    for step, kind in nodes.items():
        if kind is not NodeKind.TRANSFORM:
            continue
        own = [n for n, k in nodes.items()
               if k is not NodeKind.TRANSFORM and producers.get(n) == {step}]
        rows = sorted(lay.row[n] for n in own)
        assert rows == list(range(lay.row[step] + 1, lay.row[step] + 1 + len(own))), step


BINNERS = ("comebin", "semibin2", "metabat2")


def _binner_motif(lay):
    return next(
        m for m in repeat_motifs(lay)
        if all(any(n.endswith(f" {b}") for n in m.heads) for b in BINNERS)
    )


def test_the_three_blocks_emit_their_children_in_the_same_order(dag):
    lay = dag.layout()
    m = _binner_motif(lay)
    orders = {tuple(m.twin[x] for x in sorted(b, key=lay.row.__getitem__)) for b in m.blocks}
    assert len(orders) == 1, orders


def test_the_database_download_sits_right_above_its_database(dag):
    lay = dag.layout()
    assert lay.row["3 downloadGtdbDB"] == lay.row["ref::gtdb"] - 1


def test_the_binner_blocks_are_a_repeat_class_the_layout_knows_about(dag):
    lay = dag.layout()
    classes = {m.heads for m in repeat_motifs(lay)}
    binners = next(
        (h for h in classes if all(any(n.endswith(f" {b}") for n in h) for b in BINNERS)),
        None,
    )
    assert binners is not None, classes
    assert len(binners) == 3


def test_rendering_is_deterministic(dag):
    assert load_dag().to_svg() == load_dag().to_svg()
    assert load_dag().to_text() == load_dag().to_text()
