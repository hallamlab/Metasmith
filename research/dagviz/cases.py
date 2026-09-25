"""Small graphs that each isolate one thing the layout has to get right.

Keep them minimal. A case earns its place by being the smallest graph on which
two lane strategies disagree, so that when a panel cell looks wrong it is
obvious what made it wrong. Real plans go at the end, as the check that a rule
tuned on toys survives contact.
"""

from metasmith.models.dag_renderer import NodeKind

T, D = NodeKind.TRANSFORM, NodeKind.DATA


def _chain(*names):
    return [(a, b) for a, b in zip(names, names[1:])]


CASES: dict[str, tuple[str, list, list]] = {}


def case(name: str, note: str, nodes, edges):
    CASES[name] = (note, nodes, edges)


case(
    "chain", "nothing to decide: one column, no jogs",
    [(T, "a"), (D, "b"), (T, "c"), (D, "d")],
    _chain("a", "b", "c", "d"),
)

case(
    "fanout", "one source, four leaves — each should hang off the stem",
    [(T, "given")] + [(D, f"input_{i}") for i in range(4)],
    [("given", f"input_{i}") for i in range(4)],
)

case(
    "k22", "a,b each feed c,d — two buses that must cross two funnels",
    [(T, "a"), (T, "b"), (T, "c"), (T, "d")],
    [("a", "c"), ("a", "d"), ("b", "c"), ("b", "d")],
)

case(
    "fanin", "four leaves into one — the mirror of fanout, joining the stem early",
    [(D, f"part_{i}") for i in range(4)] + [(T, "merge"), (D, "whole")],
    [(f"part_{i}", "merge") for i in range(4)] + [("merge", "whole")],
)

case(
    "diamond", "the smallest branch-and-join: does the lane come back",
    [(D, "a"), (T, "l"), (T, "r"), (D, "j")],
    [("a", "l"), ("a", "r"), ("l", "j"), ("r", "j")],
)

case(
    "two_diamonds", "stacked joins: does width ratchet or recycle",
    [(D, "a"), (T, "l1"), (T, "r1"), (D, "m"), (T, "l2"), (T, "r2"), (D, "z")],
    [("a", "l1"), ("a", "r1"), ("l1", "m"), ("r1", "m"),
     ("m", "l2"), ("m", "r2"), ("l2", "z"), ("r2", "z")],
)

case(
    "binning", "four tools: a real bus and two real funnels — bundling wins here",
    [(T, "megahit"), (D, "assembly"), (T, "bowtie2"), (D, "bam"),
     (T, "metabat2"), (D, "bins"), (T, "das_tool"), (D, "mags")],
    [("megahit", "assembly"),
     ("assembly", "bowtie2"), ("assembly", "metabat2"), ("assembly", "das_tool"),
     ("bowtie2", "bam"), ("bam", "metabat2"),
     ("metabat2", "bins"), ("bins", "das_tool"), ("das_tool", "mags")],
)

case(
    "three_binners", "the shape E2 actually has: one assembly, three binners, one merge",
    [(D, "assembly"), (D, "bam")]
    + [(T, b) for b in ("metabat2", "semibin2", "comebin")]
    + [(D, f"{b}_bins") for b in ("metabat2", "semibin2", "comebin")]
    + [(T, "das_tool"), (D, "mags")],
    [("assembly", b) for b in ("metabat2", "semibin2", "comebin")]
    + [("bam", b) for b in ("metabat2", "semibin2", "comebin")]
    + [(b, f"{b}_bins") for b in ("metabat2", "semibin2", "comebin")]
    + [(f"{b}_bins", "das_tool") for b in ("metabat2", "semibin2", "comebin")]
    + [("das_tool", "mags")],
)

case(
    "resources", "fan-out straight into fan-in — the two stems want opposite sides",
    [(T, "given")] + [(D, f"env_{i}") for i in range(6)] + [(T, "step"), (D, "out")],
    [("given", f"env_{i}") for i in range(6)]
    + [(f"env_{i}", "step") for i in range(6)] + [("step", "out")],
)

case(
    "resources_mixed", "tool envs and a database fanned out from given, each into a different step",
    [(T, "given"), (D, "reads"), (D, "env_qc"), (D, "env_asm"), (D, "env_bin"), (D, "db"),
     (T, "qc"), (D, "clean"), (T, "asm"), (D, "contigs"), (T, "bin"), (D, "bins")],
    [("given", x) for x in ("reads", "env_qc", "env_asm", "env_bin", "db")]
    + [("reads", "qc"), ("env_qc", "qc"), ("qc", "clean"),
       ("clean", "asm"), ("env_asm", "asm"), ("asm", "contigs"),
       ("contigs", "bin"), ("clean", "bin"), ("env_bin", "bin"), ("db", "bin"),
       ("bin", "bins")],
)

case(
    "straight_costs", "the smallest graph where no bends costs a crossing: n6 must sit under n0, so n4's bar passes n0",
    [(D, f"n{i}") for i in range(8)],
    [("n0", "n2"), ("n0", "n4"), ("n0", "n5"), ("n0", "n6"), ("n1", "n3"), ("n1", "n7"),
     ("n2", "n4"), ("n2", "n7"), ("n6", "n7")],
)


def _plan(arm: str):
    """`python render_e2.py graphs` writes these from the current solve."""
    import json
    from pathlib import Path

    from metasmith.models.dag_renderer import DagMode, DagRenderer, Label, NodeKind

    g = json.loads((Path(__file__).parent / f"graphs/e2_{arm}.graph.json").read_text())
    nodes = [(NodeKind[n["kind"]], n["name"], Label(**n["label"])) for n in g["nodes"]]
    edges = [tuple(e) for e in g["edges"]]
    case(f"e2_{arm}", f"the real E2 {arm}-read plan: {g['n_steps']} steps,"
         " tool environments blacklisted", nodes, edges)

    r = DagRenderer(mode=DagMode.STEPS)
    for kind, name, label in nodes:
        r.add_node(kind, name, label)
    for a, b in edges:
        r.add_edge(a, b)
    kept, bypassed = r._graph()
    case(f"e2_{arm}_steps", "the same plan, steps only: data bypassed",
         [(k, n, r.labels[n]) for n, k in kept.items()], bypassed)


_plan("short")


def _demo():
    from dataclasses import replace

    note, nodes, edges = CASES["e2_short"]
    drop = {name for _, name, label in nodes
            if label.name in ("cami_gold_standard.py", "read_truth", "gold_standard",
                              "contig_gold_standard", "amber", "amber_results",
                              "amber_bin_metrics")}

    def relabel(label):
        if label.namespace != "e2":
            return label
        return replace(label, namespace="metagenomics", full=f"metagenomics::{label.name}")

    case("e2_short_demo", "the E2 short-read plan with no truth, gold standard or AMBER,"
         " in the metagenomics namespace",
         [(k, n, relabel(l)) for k, n, l in nodes if n not in drop],
         [e for e in edges if not drop & set(e)])


_demo()
_plan("long")
