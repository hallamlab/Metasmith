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

from .dag_draw import Style, raster_dot, render_raster, render_svg, render_text
from .dag_layout import Layout, layout


class NodeKind(Enum):
    TRANSFORM = auto()
    DATA      = auto()


# Kind is only ever a visual distinction — layout treats every node the same.
STYLES: dict[NodeKind, Style] = {
    NodeKind.TRANSFORM: Style(
        marker="●", ascii_marker="*",
        fill="#CCCCCC", stroke="#555555", rx=12,
        gv_style="filled,rounded", ansi="\033[1;36m",
    ),
    NodeKind.DATA: Style(
        marker="○", ascii_marker="o",
        fill="#FFFFFF", stroke="#777777", rx=3,
        gv_style="filled", ansi="\033[0;37m",
    ),
}

TEXT_FORMATS = {"text", "txt"}


class DagRenderer:
    """Build a directed graph of transform/data nodes and draw it."""

    def __init__(self, *, font: str = "Arial", rankdir: str = "TB"):
        self._font    = font
        self._rankdir = rankdir
        self._nodes: dict[str, NodeKind] = {}
        self._edges: list[tuple[str, str]] = []
        self._seen_edges: set[tuple[str, str]] = set()

    def add_node(self, kind: NodeKind, name: str) -> None:
        self._nodes.setdefault(name, kind)

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

    def to_dot(self) -> str:
        """Plain DOT: the graph, no positions. Nothing here invokes graphviz."""
        lines = ["digraph G {"]
        lines += [
            f'graph [fontname="{self._font}", rankdir="{self._rankdir}"];',
            f'node  [fontname="{self._font}"];',
            f'edge  [fontname="{self._font}"];',
        ]
        for name, kind in self._nodes.items():
            lines.append(self._render_node(kind, name))
        for src, dst in self._edges:
            lines.append(f'    "{src}" -> "{dst}";')
        lines.append("}")
        return "\n".join(lines)

    def to_text(self, *, unicode: bool = True, color: bool = False) -> str:
        return render_text(self.layout(), STYLES, unicode=unicode, color=color)

    def to_svg(self) -> str:
        return render_svg(self.layout(), STYLES, font=self._font)

    def to_raster_dot(self) -> str:
        return raster_dot(self.layout(), STYLES, font=self._font)

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
    def _render_node(kind: NodeKind, name: str) -> str:
        if kind is NodeKind.TRANSFORM:
            return f'"{name}" [shape="oval", style="filled", fillcolor="#CCCCCC"]'
        return f'"{name}" [shape="box"]'
