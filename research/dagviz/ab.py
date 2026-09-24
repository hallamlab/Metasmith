#!/usr/bin/env python3
"""Draw one graph through both layout engines, side by side.

The bundling work changes what a lane is allowed to carry, and no single
drawing shows whether that was an improvement -- only the pair does. `baseline/`
holds the engine as it stood at b7c2a2e8; this renders the same graph through
that and through the working tree, in the text form, so the lanes are countable.

    python research/dagviz/ab.py            # the four-tool binning core
    python research/dagviz/ab.py fanout     # one source, four consumers

Both engines share `dag_renderer`, which binds `layout` at import; swapping the
module attribute is what picks the engine, so the two runs differ in the layout
and in nothing else.
"""

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import metasmith.models.dag_draw as live_draw  # noqa: E402
import metasmith.models.dag_renderer as DR  # noqa: E402
from baseline import BASELINE_COMMIT, dag_draw as base_draw, dag_layout as base_layout  # noqa: E402
from metasmith.models.dag_layout import layout as new_layout  # noqa: E402
from metasmith.models.dag_renderer import DagRenderer, Label, NodeKind  # noqa: E402


def _base(nodes, edges, order=None, blocks=None):
    return base_layout.layout(nodes, edges, order)


T, D = NodeKind.TRANSFORM, NodeKind.DATA

# The shape the rule was argued over: megahit's assembly and bowtie2's BAM both
# reach metabat2, and the assembly also reaches the two tools either side of it.
# Four tools is the smallest graph carrying both a real bus (the assembly, three
# consumers) and two real funnels (metabat2 and das_tool, two parents each).
BINNING = (
    [(T, "megahit"), (D, "assembly"), (T, "bowtie2"), (D, "bam"),
     (T, "metabat2"), (D, "bins"), (T, "das_tool"), (D, "mags")],
    [("megahit", "assembly"),
     ("assembly", "bowtie2"), ("assembly", "metabat2"), ("assembly", "das_tool"),
     ("bowtie2", "bam"), ("bam", "metabat2"),
     ("metabat2", "bins"), ("bins", "das_tool"), ("das_tool", "mags")],
)

FANOUT = (
    [(T, "given")] + [(D, f"input_{i}") for i in range(4)],
    [("given", f"input_{i}") for i in range(4)],
)

CASES = {"binning": BINNING, "fanout": FANOUT}


def build(nodes, edges) -> DagRenderer:
    r = DagRenderer(colour="none")
    for kind, name in nodes:
        r.add_node(kind, name, Label(name=name))
    for a, b in edges:
        r.add_edge(a, b)
    return r


def side_by_side(name: str) -> str:
    r = build(*CASES[name])

    DR.layout = _base
    DR.render_text = lambda *a, kinds=None, **k: base_draw.render_text(*a, **k)
    old, old_m = r.to_text(), base_layout.measure(base_layout.layout(*r._graph()))

    DR.layout = new_layout
    DR.render_text = live_draw.render_text
    new, new_m = r.to_text(), DR.layout(*r._graph())
    new_m = __import__(
        "metasmith.models.dag_layout", fromlist=["measure"]
    ).measure(new_m)

    left = old.splitlines()
    right = new.splitlines()
    head_l = f"a lane per edge ({BASELINE_COMMIT})"
    head_r = "solved (working tree)"
    width = max([len(x) for x in left] + [len(head_l)]) + 4

    out = [f"{head_l:<{width}}{head_r}",
           f"{'-' * (width - 4):<{width}}{'-' * len(head_r)}"]
    for i in range(max(len(left), len(right))):
        a = left[i] if i < len(left) else ""
        b = right[i] if i < len(right) else ""
        out.append(f"{a:<{width}}{b}")
    out += ["", f"{str(old_m):<{width}}", str(new_m)]
    return "\n".join(out)


def render_pairs(out: Path) -> None:
    """Both engines, both cases, both themes, as SVG.

    The text form cannot settle this. `render_text` has to spend a whole ROW on
    every sideways jog, because a character cell is the smallest thing it can
    draw; the SVG folds that same jog into the gap it already has between two
    rows. So a bundled fan-out looks half as tall again in text as it is on the
    page, and a comparison drawn in text is a comparison of the text renderer.
    """
    out.mkdir(parents=True, exist_ok=True)
    for case in CASES:
        for engine, lay, draw in (
            ("old", _base, base_draw),
            ("new", new_layout, live_draw),
        ):
            for theme in ("light", "dark"):
                r = build(*CASES[case])
                r._theme = __import__(
                    "metasmith.models.dag_renderer", fromlist=["THEMES"]
                ).THEMES[theme]
                r._theme = r._theme.__class__(
                    plate=r._theme.plate.__class__(
                        **{**vars(r._theme.plate), "paint_background": False}
                    ),
                    styles=r._theme.styles,
                )
                DR.layout = lay
                DR.render_svg = (draw.render_svg if draw is live_draw else
                                 lambda *a, kinds=None, **k: base_draw.render_svg(*a, **k))
                path = out / f"minimal_{case}_{engine}_{theme}.svg"
                path.write_text(r.to_svg(), encoding="utf-8")
                head = path.read_text().split(">", 1)[0]
                print(f"{path.name}  {head.split('viewBox=')[-1][:14]}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if args and args[0] == "svg":
        render_pairs(HERE / "svg")
    else:
        for case in args or ["binning"]:
            print(f"\n=== {case} ===\n")
            print(side_by_side(case))
