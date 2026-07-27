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

from metasmith.models.dag_layout import measure, repeat_motifs
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


def test_the_drawing_does_not_get_more_expensive(dag):
    """Ceilings, not goldens: a tuning change is free to improve any of these
    and has to say so out loud to make one worse.

    Where they came from, oldest first — this plan drawn before the row order
    learned to emit a reference database beside the step that wants it, then
    with that but before it learned to draw a repeated block the same way each
    time, then now:

        rail=545 lanes=14 longest=56 crossings=127 repeats=-
        rail=527 lanes=13 longest=35 crossings=123 repeats=1/6
        rail=536 lanes=13 longest=35 crossings=123 repeats=4/6

    Rail is the one that got worse, by nine rows out of five hundred, and it
    bought the three binner blocks: contiguous, identically ordered, and the
    shared database drawn once above all three instead of inside the first.
    Lanes, crossings and the longest rail are unchanged.
    """
    m = measure(dag.layout())
    assert m.rail_rows <= 536
    assert m.lanes <= 13
    assert m.crossings <= 123
    # the one that was the whole complaint: a step dragged the length of the
    # page away from the module it belongs to, by the database it shares
    assert m.longest_rail <= 35
    # ... and the one this is now optimised for first
    assert m.congruent >= 4


def test_a_shared_reference_database_is_drawn_beside_its_consumer(dag):
    # all three gtdbtk steps take the one gtdb download, and each belongs with
    # the binner that feeds it rather than with the other two
    lay = dag.layout()
    rows = {n.name: n.row for n in lay.nodes}
    for binner in ("comebin", "semibin2", "metabat2"):
        fasta = rows[f"sequences::{binner}_bin_fasta"]
        gtdbtk = min(
            r for n, r in rows.items() if n.endswith(" gtdbtk") and r > fasta
        )
        assert gtdbtk - fasta == 1, binner


# --- the three binners are three copies of one block -------------------------


BINNERS = ("comebin", "semibin2", "metabat2")


def _blocks(lay):
    """Each binner's five rows, keyed by the role the node plays.

    The checkm and gtdbtk steps are named per instance only by a step number,
    so they are found as the first of each below that binner's bin fasta —
    which is exactly the claim these tests are making about the drawing.
    """
    rows = {n.name: n.row for n in lay.nodes}
    out = {}
    for b in BINNERS:
        fasta = rows[f"sequences::{b}_bin_fasta"]
        out[b] = dict(
            head=rows[next(n for n in rows if n.endswith(f" {b}"))],
            table=rows[f"binning::{b}_contig_to_bin_table"],
            fasta=fasta,
            gtdbtk=min(r for n, r in rows.items()
                       if n.endswith(" gtdbtk") and r > fasta),
            checkm=min(r for n, r in rows.items()
                       if n.endswith(" checkm") and r > fasta),
        )
    return out


def test_each_binner_block_is_a_contiguous_run_of_rows(dag):
    lay = dag.layout()
    blocks = _blocks(lay)
    for b, r in blocks.items():
        span = sorted(r.values())
        assert span == list(range(span[0], span[0] + 5)), (b, r)
    # ... and the three runs are back to back, in step order
    starts = sorted(min(r.values()) for r in blocks.values())
    assert starts[1] == starts[0] + 5 and starts[2] == starts[1] + 5


def test_the_three_blocks_emit_their_children_in_the_same_order(dag):
    # the single thing that used to differ most: one block put gtdbtk before
    # checkm and the next put checkm before gtdbtk, because `18 checkm` is on
    # the spine and `19 checkm` is not
    offsets = {
        b: tuple(k for k, _ in sorted(r.items(), key=lambda kv: kv[1]))
        for b, r in _blocks(dag.layout()).items()
    }
    assert len(set(offsets.values())) == 1, offsets


def test_the_shared_database_is_emitted_once_above_the_whole_group(dag):
    lay = dag.layout()
    rows = {n.name: n.row for n in lay.nodes}
    first = min(min(r.values()) for r in _blocks(lay).values())
    assert rows["ref::gtdb"] < first
    assert rows["3 downloadGtdbDB"] == rows["ref::gtdb"] - 1
    assert first - rows["ref::gtdb"] == 1  # immediately above, not at the top


def test_the_shared_outputs_sit_below_every_block_they_join(dag):
    # `taxonomy::gtdbtk` used to land in the middle of the third block, because
    # the walk had it ready while that block still had a node left over
    lay = dag.layout()
    rows = {n.name: n.row for n in lay.nodes}
    last = max(max(r.values()) for r in _blocks(lay).values())
    for sink in ("taxonomy::gtdbtk", "taxonomy::checkm_stats"):
        assert rows[sink] > last, sink


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
