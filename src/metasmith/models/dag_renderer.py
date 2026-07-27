"""DAG rendering — the abstract node/edge API shared by every metasmith caller
that needs to draw a transform/data graph (currently `WorkflowPlan` and solver
`Solution`).

Consumers declare TRANSFORM and DATA nodes plus directed edges. Placement is
metasmith's own (`dag_layout`); the backends in `dag_draw` turn that placement
into text, SVG, or — for raster formats only — pre-placed DOT that graphviz
rasterizes without laying anything out. `to_dot()` stays a plain description of
the graph with no positions, for consumers that want to run their own graphviz.
"""
from __future__ import annotations

from enum import Enum, auto
from pathlib import Path

from .dag_draw import (
    Label, LabelMode, Style, default_label, dot_escape,
    raster_dot, render_raster, render_svg, render_text,
)
from .dag_layout import Layout, layout


class NodeKind(Enum):
    TRANSFORM = auto()
    DATA      = auto()
    TARGET    = auto()  # a data node that is a requested output


# Kind is only ever a visual distinction — layout treats every node the same.
# An open circle is a step, a filled square is a thing: the two read apart at a
# glance even at marker size, where a shape difference is all that survives.
STYLES: dict[NodeKind, Style] = {
    NodeKind.TRANSFORM: Style(
        marker="○", ascii_marker="o",
        fill="#FFFFFF", stroke="#2B2B2B", rx=0,
        shape="circle", gv_style="filled", ansi="\033[1;36m",
        svg_shape="circle", marker_scale=1.0, stroke_width=1.7,
    ),
    NodeKind.DATA: Style(
        marker="■", ascii_marker="#",
        fill="#2B2B2B", stroke="#2B2B2B", rx=1,
        shape="box", gv_style="filled", ansi="\033[0;37m",
        svg_shape="square", marker_scale=0.82, stroke_width=1.0,
    ),
    # marking the requested outputs on the nodes themselves is what lets the
    # drawing skip the synthetic sink that collects them, and that sink is the
    # single most expensive thing in the layout: every target holds a lane from
    # wherever it is produced down to the last row
    NodeKind.TARGET: Style(
        marker="▣", ascii_marker="@",
        fill="#2B2B2B", stroke="#2B2B2B", rx=1,
        shape="box", gv_attrs="peripheries=2", gv_style="filled",
        ansi="\033[1;37m",
        svg_shape="ringed_square", marker_scale=0.82, stroke_width=1.0,
    ),
}

TEXT_FORMATS = {"text", "txt"}


class DagRenderer:
    """Build a directed graph of transform/data nodes and draw it.

    Nodes are keyed by an id the caller owns; what gets drawn beside a node is
    a separate `Label`, so a caller can shorten what the reader sees without
    merging two things the graph must keep apart.
    """

    def __init__(
        self,
        *,
        font: str = "Arial",
        rankdir: str = "TB",
        label_mode: LabelMode = LabelMode.COLUMN,
    ):
        self._font    = font
        self._rankdir = rankdir
        self._label_mode = label_mode
        self._nodes: dict[str, NodeKind] = {}
        self._labels: dict[str, Label] = {}
        self._edges: list[tuple[str, str]] = []
        self._seen_edges: set[tuple[str, str]] = set()

    def add_node(self, kind: NodeKind, name: str, label: Label | None = None) -> None:
        """`name` identifies the node; `label` is only what gets drawn.

        Two steps running the same transform are distinguished only by the step
        number in their id. Passing a label that drops the number keeps them two
        nodes; putting the shortened form in `name` would silently fold them
        into one and the layout's cycle-breaker would cut edges to compensate.
        """
        self._nodes.setdefault(name, kind)
        if label is not None:
            self._labels.setdefault(name, label)

    def mark(self, kind: NodeKind, name: str) -> None:
        """Retype a node that already exists.

        `add_node` is first-wins so that an `add_edge` cannot downgrade a
        declared transform; marking is the deliberate exception, for a property
        only known after the node has been drawn into the graph — a data node
        turning out to be one of the requested outputs.
        """
        if name in self._nodes:
            self._nodes[name] = kind

    def add_edge(self, src: str, dst: str) -> None:
        key = (src, dst)
        if key in self._seen_edges:
            return
        self._seen_edges.add(key)
        self._edges.append(key)
        self._nodes.setdefault(src, NodeKind.DATA)
        self._nodes.setdefault(dst, NodeKind.DATA)

    def layout(self) -> Layout:
        return layout(self._nodes, self._edges)

    @property
    def labels(self) -> dict[str, Label]:
        """Every node's drawn label, defaulted from its id where unset."""
        return {n: self._labels.get(n) or default_label(n) for n in self._nodes}

    def to_dot(self) -> str:
        """Plain DOT: the graph, no positions. Nothing here invokes graphviz."""
        lines = ["digraph G {"]
        lines += [
            f'graph [fontname="{self._font}", rankdir="{self._rankdir}"];',
            f'node  [fontname="{self._font}"];',
            f'edge  [fontname="{self._font}"];',
        ]
        labels = self.labels
        for name, kind in self._nodes.items():
            lines.append(self._render_node(kind, name, labels[name]))
        for src, dst in self._edges:
            lines.append(f'    "{src}" -> "{dst}";')
        lines.append("}")
        return "\n".join(lines)

    def to_text(self, *, unicode: bool = True, color: bool = False) -> str:
        return render_text(
            self.layout(), STYLES, labels=self.labels, unicode=unicode, color=color
        )

    def to_svg(self) -> str:
        return render_svg(
            self.layout(), STYLES, labels=self.labels,
            label_mode=self._label_mode, font=self._font,
        )

    def to_raster_dot(self) -> str:
        return raster_dot(
            self.layout(), STYLES, labels=self.labels,
            label_mode=self._label_mode, font=self._font,
        )

    def render(self, path_base: Path | str, format: str = "svg") -> Path:
        path_base = Path(path_base)
        ext = path_base.suffix
        if ext:
            format    = ext.lstrip(".")
            path_base = path_base.with_suffix("")
        format = format.lower()
        out = path_base.parent / f"{path_base.name}.{format}"
        out.parent.mkdir(parents=True, exist_ok=True)
        if format == "dot":
            out.write_text(self.to_dot() + "\n", encoding="utf-8")
        elif format in TEXT_FORMATS:
            out.write_text(self.to_text(), encoding="utf-8")
        elif format == "svg":
            out.write_text(self.to_svg(), encoding="utf-8")
        else:
            render_raster(self.to_raster_dot(), out, format)
        return out

    @staticmethod
    def _render_node(kind: NodeKind, name: str, label: Label | None = None) -> str:
        # the id is the DOT node name, so an id that already reads as its own
        # label needs no attribute — that keeps this output stable for the
        # consumers that run their own graphviz over it
        if kind is NodeKind.TRANSFORM:
            attrs = ['shape="oval"', 'style="filled"', 'fillcolor="#CCCCCC"']
        else:
            attrs = ['shape="box"']
            if kind is NodeKind.TARGET:
                attrs.append("peripheries=2")
        if label is not None and label.full != name:
            attrs.append(f'label="{dot_escape(label.full)}"')
        return f'"{name}" [{", ".join(attrs)}]'
