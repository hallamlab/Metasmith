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

from dataclasses import dataclass, replace
from enum import Enum, auto
from pathlib import Path

from .dag_colour import SCHEMES, Colouring, colour_layout
from .dag_draw import (
    Label, LabelMode, Plate, Style, default_label, dot_escape,
    raster_dot, render_raster, render_svg, render_text,
)
from .dag_layout import Layout, layout


class NodeKind(Enum):
    TRANSFORM = auto()
    DATA      = auto()
    TARGET    = auto()  # a data node that is a requested output


# Kind is only ever a visual distinction — layout treats every node the same.
# A downward triangle is a step; a circle is a thing it made. A requested output
# is the *same* circle drawn solid rather than a shape of its own: a target is a
# piece of data, and giving it a third outline said it was a third kind of
# thing. Fill is what marks it, so the one kind a reader is looking for is the
# one that is solid, and it is also where a colour scheme's hue lands (`solid`
# on the style, not the shape, is what tells `tint` that). A step's label is
# greyed for the same reason — the file names are the content.
#
# `marker_scale` is one number for all of them, because it means a *width* and
# the shapes have to read as one family. They were 1.25/0.90/0.82 of three
# different quantities, which drew a 17px triangle beside a 10px circle.
STYLES: dict[NodeKind, Style] = {
    NodeKind.TRANSFORM: Style(
        marker="▽", ascii_marker="v",
        fill="#FFFFFF", stroke="#2B2B2B", rx=0,
        text="#7A7A7A", muted="#A8A8A8",
        shape="triangle", gv_attrs="orientation=180",
        gv_style="filled", ansi="\033[1;36m",
        svg_shape="triangle_down", marker_scale=1.0, stroke_width=1.5,
    ),
    NodeKind.DATA: Style(
        marker="○", ascii_marker="o",
        fill="#FFFFFF", stroke="#2B2B2B", rx=0,
        shape="circle", gv_style="filled", ansi="\033[0;37m",
        svg_shape="circle", marker_scale=1.0, stroke_width=3.0,
    ),
    # marking the requested outputs on the nodes themselves is what lets the
    # drawing skip the synthetic sink that collects them, and that sink is the
    # single most expensive thing in the layout: every target holds a lane from
    # wherever it is produced down to the last row
    NodeKind.TARGET: Style(
        marker="●", ascii_marker="*",
        fill="#212121", stroke="#2B2B2B",
        shape="circle", gv_style="filled", ansi="\033[1;37m",
        svg_shape="circle", marker_scale=1.0, stroke_width=3.0,
        solid=True,
    ),
}

@dataclass(frozen=True)
class Theme:
    """A plate and the three node styles that sit on it.

    Keyed by `NodeKind`, which is why a theme is here and not beside `Style`:
    `dag_draw` is deliberately ignorant of what a kind means, and that is the
    same reason `STYLES` is here.
    """
    plate: Plate
    styles: dict[NodeKind, Style]


# the light theme *is* `STYLES`, not a copy of it: a drawing already on disk
# and one drawn today have to be the same file, and the module-level name is
# what the tests and any outside reader mean by "how a node is drawn"
LIGHT = Theme(plate=Plate(), styles=STYLES)

# only the colours are restated; every other field is taken from the light
# style, so a marker shape, a scale or a stroke weight cannot drift between the
# two — the drawings have to be one drawing in two inks. A hollow marker's fill
# is the plate's own background rather than a colour of its own, since hollow
# reads hollow only where the fill and the ground agree.
DARK = Theme(
    plate=Plate(background="#1B1E24", edge="#8D97A8"),
    styles={
        NodeKind.TRANSFORM: replace(
            STYLES[NodeKind.TRANSFORM],
            fill="#1B1E24", stroke="#DFE3EA", text="#8D97A8", muted="#5F6877",
        ),
        NodeKind.DATA: replace(
            STYLES[NodeKind.DATA],
            fill="#1B1E24", stroke="#DFE3EA", text="#DFE3EA", muted="#6B7484",
        ),
        NodeKind.TARGET: replace(
            STYLES[NodeKind.TARGET],
            fill="#DFE3EA", stroke="#DFE3EA", text="#DFE3EA", muted="#6B7484",
        ),
    },
)

THEMES: dict[str, Theme] = {"light": LIGHT, "dark": DARK}

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
        colour: str = "none",
        theme: str = "light",
        background: bool = True,
    ):
        if colour not in SCHEMES:
            raise ValueError(
                f"unknown colour scheme {colour!r};"
                f" expected one of {', '.join(SCHEMES)}"
            )
        if theme not in THEMES:
            raise ValueError(
                f"unknown theme {theme!r};"
                f" expected one of {', '.join(THEMES)}"
            )
        self._font    = font
        self._rankdir = rankdir
        self._label_mode = label_mode
        # monochrome by default: colour is a thing a caller asks for, and which
        # of the schemes is worth defaulting to is a question for a reader
        # looking at them, not for this constructor
        self._colour = colour
        # light by default for the same reason: every artifact already on disk
        # is one, and asking for the other is the caller's move
        theme_obj = THEMES[theme]
        # a plate is shared across every renderer of one theme (`LIGHT`/`DARK`
        # are module-level), so an off toggle replaces this instance's copy
        # rather than mutating the frozen original out from under every other
        # caller
        if not background:
            theme_obj = replace(theme_obj, plate=replace(theme_obj.plate, paint_background=False))
        self._theme = theme_obj
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

    def colouring(self, lay: Layout | None = None) -> Colouring:
        """The scheme applied to this graph; empty unless one was asked for."""
        return colour_layout(lay or self.layout(), self._colour)

    def to_text(self, *, unicode: bool = True, color: bool = False) -> str:
        lay = self.layout()
        # no plate: the only colour this backend emits is `Style.ansi`, chosen
        # against whatever the reader's palette is, and a terminal owns its own
        # background — so a theme has nothing here it could change.
        return render_text(
            lay, self._theme.styles, labels=self.labels,
            unicode=unicode, color=color,
            colour=self.colouring(lay),
        )

    def to_svg(self) -> str:
        lay = self.layout()
        return render_svg(
            lay, self._theme.styles, labels=self.labels,
            label_mode=self._label_mode, font=self._font,
            colour=self.colouring(lay), plate=self._theme.plate,
        )

    def to_raster_dot(self) -> str:
        lay = self.layout()
        return raster_dot(
            lay, self._theme.styles, labels=self.labels,
            label_mode=self._label_mode, font=self._font,
            colour=self.colouring(lay), plate=self._theme.plate,
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
