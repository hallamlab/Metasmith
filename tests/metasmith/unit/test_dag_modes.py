import pytest

from metasmith.models.dag_renderer import DagMode, DagRenderer, Label, NodeKind


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


def _build(**kwargs) -> DagRenderer:
    plan = _plan()
    r = DagRenderer(**kwargs)
    for kind, name, label in plan["nodes"]:
        r.add_node(kind, name, label)
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


def test_hide_resources_keeps_a_given_that_sits_in_a_type_lineage():
    # An environment has no data on either side of it and goes; a read has its
    # parent type above it and stays. That lineage edge is the whole of the
    # distinction -- without it the rule would take the reads too.
    nodes, _ = _graph(mode=DagMode.COLLAPSED, hide_resources=True)
    assert "e2::fastp.env" not in nodes
    assert {"e2::read_metadata", "e2::reads"} <= nodes


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
