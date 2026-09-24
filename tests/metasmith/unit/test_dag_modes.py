import pytest

from metasmith.models.dag_renderer import DagMode, DagRenderer, Label, NodeKind
from metasmith.models.solver import Endpoint


def _type(**props) -> Endpoint:
    return Endpoint.Unpack({"properties": props})


ENV = _type(e2="env")


def _plan() -> dict:
    """One bipartite plan in the shape `plan.BuildDAG` emits.

    Two steps in series, a resource given that stands outside every lineage,
    a sample given that starts the chain under a parent type, one intermediate
    and one target. The parent edge is what `BuildDAG` draws between a given
    and the given it is a subtype of, and it is the only thing separating a
    read from an environment.
    """
    return dict(
        nodes=[
            (NodeKind.TRANSFORM, "given", None),
            (NodeKind.DATA, "e2::read_metadata", None),
            (NodeKind.DATA, "e2::reads", None),
            (NodeKind.DATA, "e2::fastp.env", None),
            (NodeKind.TRANSFORM, "s0", Label(name="fastp")),
            (NodeKind.DATA, "e2::trimmed", None),
            (NodeKind.TRANSFORM, "s1", Label(name="megahit")),
            (NodeKind.TARGET, "e2::contigs", None),
        ],
        edges=[
            ("given", "e2::read_metadata"),
            ("given", "e2::reads"),
            ("given", "e2::fastp.env"),
            ("e2::read_metadata", "e2::reads"),
            ("e2::reads", "s0"),
            ("e2::fastp.env", "s0"),
            ("s0", "e2::trimmed"),
            ("e2::trimmed", "s1"),
            ("s1", "e2::contigs"),
        ],
    )


DTYPES = {
    "e2::read_metadata": _type(e2="read_metadata"),
    "e2::reads": _type(e2="reads", platform="illumina"),
    "e2::fastp.env": _type(e2="env", provides="fastp"),
    "e2::trimmed": _type(e2="trimmed"),
    "e2::contigs": _type(e2="contigs"),
}


def _build(**kwargs) -> DagRenderer:
    plan = _plan()
    r = DagRenderer(**kwargs)
    for kind, name, label in plan["nodes"]:
        r.add_node(kind, name, label, dtype=DTYPES.get(name))
    for src, dst in plan["edges"]:
        r.add_edge(src, dst)
    return r


def _graph(**kwargs) -> tuple[set[str], set[tuple[str, str]]]:
    nodes, edges = _build(**kwargs)._graph()
    return set(nodes), set(edges)


def test_plain_keeps_every_node():
    nodes, edges = _graph()
    assert len(nodes) == 8
    assert ("e2::trimmed", "s1") in edges


def test_collapsed_absorbs_the_intermediate_but_keeps_both_ends():
    nodes, edges = _graph(mode=DagMode.COLLAPSED)
    assert "e2::trimmed" not in nodes
    assert {"e2::reads", "e2::fastp.env", "e2::contigs"} <= nodes
    assert ("s0", "s1") in edges


def test_a_blacklisted_type_cuts_every_subtype_and_wires_nothing_around_it():
    nodes, edges = _graph(blacklist=[ENV])
    assert "e2::fastp.env" not in nodes
    assert {"e2::read_metadata", "e2::reads", "s0"} <= nodes
    assert not any("e2::fastp.env" in e for e in edges)
    assert ("given", "s0") not in edges


def test_a_blacklist_leaves_a_type_that_is_not_its_subtype():
    nodes, _ = _graph(blacklist=[_type(e2="env", provides="megahit")])
    assert "e2::fastp.env" in nodes


def test_a_given_with_nothing_left_under_it_goes_too():
    nodes, _ = _graph(blacklist=[_type(e2="read_metadata"), _type(e2="reads"), ENV])
    assert "given" not in nodes


def test_steps_mode_on_a_blacklisted_graph_bypasses_what_is_left():
    nodes, edges = _graph(mode=DagMode.STEPS, blacklist=[ENV])
    assert nodes == {"s0", "s1"}
    assert edges == {("s0", "s1")}


def test_steps_mode_leaves_the_steps_and_wires_them_through():
    nodes, edges = _graph(mode=DagMode.STEPS)
    assert nodes == {"s0", "s1"}
    # `given` reaches s0 only through data, so dropping the chain wholesale has
    # to bypass a run of dropped nodes rather than one at a time
    assert edges == {("s0", "s1")}


def test_the_legend_draws_one_block_per_named_transform():
    r = _build(mode=DagMode.LEGEND)
    blocks, _ = r._legend_blocks()
    assert len(blocks) == 2
    assert "given" not in r.to_svg()


def test_legend_needs_no_flag_beyond_legend_columns():
    r = _build(mode=DagMode.LEGEND, legend_columns=2)
    assert r.to_svg().startswith("<?xml")


def test_hide_data_is_no_longer_a_public_keyword():
    # STEPS absorbed it; a caller now asks for the mode, not the flag.
    with pytest.raises(TypeError):
        DagRenderer(mode=DagMode.COLLAPSED, hide_data=True)


def test_monochrome_suppresses_hues_without_losing_the_scheme_choice():
    hued = _build(colour="module")
    mono = _build(colour="module", monochrome=True)
    assert hued.colouring()
    assert not mono.colouring()
    # same graph, same scheme setting, only the flag differs -- the drawing
    # comes out identical to asking for no scheme at all
    assert mono.to_svg() == _build(colour="none").to_svg()


def test_module_overrides_pin_one_module_and_leave_the_rest_on_the_built_in_palette():
    default = _build(colour="module").colouring().nodes
    overridden = _build(
        colour="module", colour_overrides={"s0": "#ABCDEF"},
    ).colouring().nodes
    assert overridden["s0"] == "#ABCDEF"
    for name in default:
        if name != "s0":
            assert overridden[name] == default[name]


def test_custom_module_palette_replaces_the_built_in_one():
    custom = ("#101010", "#202020")
    hues = set(_build(colour="module", colour_palette=custom).colouring().nodes.values())
    assert hues
    assert hues <= set(custom)


def _isolated_step_plan() -> DagRenderer:
    """The E2 STEPS motivating case: a step touching only givens and an
    unconsumed target, beside a real two-step pipeline.
    """
    r = DagRenderer(mode=DagMode.STEPS)
    r.add_node(NodeKind.TRANSFORM, "given")
    r.add_node(NodeKind.TRANSFORM, "s0", Label(name="fastp"))
    r.add_node(NodeKind.TRANSFORM, "s1", Label(name="megahit"))
    r.add_node(NodeKind.TRANSFORM, "fastqc_raw", Label(name="fastqc_raw"))
    r.add_edge("given", "e2::reads")
    r.add_edge("e2::reads", "s0")
    r.add_edge("s0", "e2::trimmed")
    r.add_edge("e2::trimmed", "s1")
    r.add_edge("s1", "e2::contigs")
    r.mark(NodeKind.TARGET, "e2::contigs")
    r.add_edge("given", "e2::raw_reads")
    r.add_edge("e2::raw_reads", "fastqc_raw")
    r.add_edge("fastqc_raw", "e2::qc_report")
    r.mark(NodeKind.TARGET, "e2::qc_report")
    return r


def test_a_component_of_one_step_is_drawn_before_the_bigger_pipeline():
    lay = _isolated_step_plan().layout()
    assert lay.row["fastqc_raw"] < lay.row["s0"] < lay.row["s1"]
