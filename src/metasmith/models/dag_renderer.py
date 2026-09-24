from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum, auto
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .dag_colour import SCHEMES, Colouring, colour_layout
from .dag_draw import (
    DEFAULT_LABEL_CHARS, Geometry, Label, LabelMode, Plate, Style,
    default_label, dot_escape, raster_dot, render_legend, render_raster,
    render_svg, render_text,
)
from .dag_draw import geometry as _geometry
from .dag_layout import Layout, layout


class NodeKind(Enum):
    TRANSFORM = auto()
    DATA      = auto()
    TARGET    = auto()


class DagMode(Enum):
    """Which picture of the same graph to draw.

    The graph a renderer holds is bipartite -- data nodes and transform nodes
    alternate -- and all four modes read off that one structure, so a caller
    builds the DAG once and switches mode at render time.
    """

    PLAIN     = "plain"      # every node, as built
    COLLAPSED = "collapsed"  # intermediate data absorbed into its producer
    STEPS     = "steps"      # collapsed further: no data survives at all
    LEGEND    = "legend"     # one detached block per transform, in a grid


# The synthetic transform nodes `plan.BuildDAG` adds to root and cap the graph.
# They stand for no tool, so the legend has no block for them.
SYNTHETIC = ("given", "target")


def step_blocks(
    nodes: Mapping[str, NodeKind], edges: Iterable[tuple[str, str]]
) -> list[list[str]]:
    """Each step with the data only it produces, for the rows under it. Data
    with two producers cannot sit under both, so it stands alone."""
    producers: dict[str, set[str]] = {}
    for src, dst in edges:
        producers.setdefault(dst, set()).add(src)
    blocks = {n: [n] for n, kind in nodes.items() if kind is NodeKind.TRANSFORM}
    for name, kind in nodes.items():
        made_by = producers.get(name, ())
        if kind is not NodeKind.TRANSFORM and len(made_by) == 1:
            (step,) = made_by
            if step in blocks:
                blocks[step].append(name)
    return list(blocks.values())


# The transform carries the emphasis and the data recedes: a plan is a sequence
# of things done, and the types are what they are done to. Everything here is
# one half of that -- the darker ink, the weight, the heavier stroke on one
# side; the greyer ink and the lighter stroke on the other.
STYLES: dict[NodeKind, Style] = {
    NodeKind.TRANSFORM: Style(
        marker="▽", ascii_marker="v",
        fill="#FFFFFF", stroke="#2B2B2B", rx=0,
        text="#111111", muted="#8A8A8A", weight="600",
        shape="triangle", gv_attrs="orientation=180",
        gv_style="filled", ansi="\033[1;36m",
        svg_shape="triangle_down", marker_scale=1.0, stroke_width=2.0,
    ),
    NodeKind.DATA: Style(
        marker="○", ascii_marker="o",
        fill="#FFFFFF", stroke="#6E6E6E", rx=0,
        text="#7A7A7A", muted="#A8A8A8",
        shape="circle", gv_style="filled", ansi="\033[0;37m",
        svg_shape="circle", marker_scale=0.88, stroke_width=1.3,
    ),
    NodeKind.TARGET: Style(
        marker="●", ascii_marker="*",
        fill="#212121", stroke="#2B2B2B",
        shape="circle", gv_style="filled", ansi="\033[0;97m",
        svg_shape="circle", marker_scale=1.0, stroke_width=1.3,
        solid=True,
    ),
}

@dataclass(frozen=True)
class Theme:
    plate: Plate
    styles: dict[NodeKind, Style]


LIGHT = Theme(plate=Plate(), styles=STYLES)

DARK = Theme(
    plate=Plate(background="#1B1E24", edge="#8D97A8", rule="#2F343D"),
    styles={
        NodeKind.TRANSFORM: replace(
            STYLES[NodeKind.TRANSFORM],
            fill="#1B1E24", stroke="#DFE3EA", text="#DFE3EA", muted="#6B7484",
        ),
        NodeKind.DATA: replace(
            STYLES[NodeKind.DATA],
            fill="#1B1E24", stroke="#8D97A8", text="#8D97A8", muted="#5F6877",
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
    def __init__(
        self,
        *,
        font: str = "Arial",
        rankdir: str = "TB",
        label_mode: LabelMode = LabelMode.COLUMN,
        colour: str = "none",
        theme: str = "light",
        background: bool = True,
        mode: DagMode = DagMode.PLAIN,
        blacklist: Iterable[Any] = (),
        legend_columns: int = 0,
        monochrome: bool = False,
        colour_palette: Sequence[str] | None = None,
        colour_overrides: Mapping[str, str] | None = None,
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
        self._colour = colour
        theme_obj = THEMES[theme]
        if not background:
            theme_obj = replace(theme_obj, plate=replace(theme_obj.plate, paint_background=False))
        self._theme = theme_obj
        self._mode = mode
        self._blacklist = tuple(blacklist)
        self._legend_columns = legend_columns
        self._monochrome = monochrome
        self._colour_palette = colour_palette
        self._colour_overrides = colour_overrides
        self._nodes: dict[str, NodeKind] = {}
        self._labels: dict[str, Label] = {}
        self._dtypes: dict[str, Any] = {}
        self._edges: list[tuple[str, str]] = []
        self._seen_edges: set[tuple[str, str]] = set()
        self._declared: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {}

    def add_node(
        self, kind: NodeKind, name: str, label: Label | None = None, dtype: Any = None,
    ) -> None:
        self._nodes.setdefault(name, kind)
        if label is not None:
            self._labels.setdefault(name, label)
        if dtype is not None:
            self._dtypes.setdefault(name, dtype)

    def mark(self, kind: NodeKind, name: str) -> None:
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

    def out_degree(self, name: str) -> int:
        return sum(1 for src, _ in self._edges if src == name)

    def remove_node(self, name: str) -> None:
        self._nodes.pop(name, None)
        self._labels.pop(name, None)
        self._dtypes.pop(name, None)
        self._edges = [e for e in self._edges if name not in e]
        self._seen_edges = {e for e in self._seen_edges if name not in e}

    def declare(
        self, name: str, requires: Sequence[str], produces: Sequence[str],
        dtypes: Mapping[str, Any] | None = None,
    ) -> None:
        """Record what a transform asks for and makes *as declared*, not as bound.

        The legend wants the tool's own signature. A step binds a requirement to
        one concrete type, so four `checkm2` steps reading four bin types are one
        transform requiring `e2::bin` -- which only the declaration says.
        """
        for n, t in (dtypes or {}).items():
            self._dtypes.setdefault(n, t)
        self._declared[name] = (
            tuple(n for n in requires if not self._blacklisted(n)),
            tuple(n for n in produces if not self._blacklisted(n)),
        )

    def layout(self, order: Sequence[str] | None = None) -> Layout:
        nodes, edges = self._graph()
        return layout(nodes, edges, order, step_blocks(nodes, edges))

    def _graph(self) -> tuple[dict[str, NodeKind], list[tuple[str, str]]]:
        nodes, edges = self._cut()
        if self._mode in (DagMode.COLLAPSED, DagMode.STEPS):
            return self._collapse(nodes, edges)
        return nodes, edges

    def _blacklisted(self, name: str) -> bool:
        dtype = self._dtypes.get(name)
        return dtype is not None and any(dtype.IsA(t) for t in self._blacklist)

    def _cut(self) -> tuple[dict[str, NodeKind], list[tuple[str, str]]]:
        """Delete every node whose type is a blacklisted type, with its edges.

        Nothing is wired around a cut node. A synthetic root left with no
        edges goes as well, since it stands for nothing on its own.
        """
        nodes = {n: k for n, k in self._nodes.items() if not self._blacklisted(n)}
        edges = [(a, b) for a, b in self._edges if a in nodes and b in nodes]
        touched = {n for e in edges for n in e}
        for n in SYNTHETIC:
            if n in nodes and n not in touched and len(nodes) > 1:
                del nodes[n]
        return nodes, edges

    @staticmethod
    def _neighbours(
        nodes: Mapping[str, NodeKind], edges: Sequence[tuple[str, str]],
    ) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
        preds: dict[str, list[str]] = {n: [] for n in nodes}
        succs: dict[str, list[str]] = {n: [] for n in nodes}
        for src, dst in edges:
            succs[src].append(dst)
            preds[dst].append(src)
        return preds, succs

    def _is_data(self, name: str) -> bool:
        return self._nodes.get(name) in (NodeKind.DATA, NodeKind.TARGET)

    def _collapse(
        self, nodes: dict[str, NodeKind], edges: list[tuple[str, str]],
    ) -> tuple[dict[str, NodeKind], list[tuple[str, str]]]:
        """Absorb intermediate data into the step that produced it.

        In COLLAPSED mode what survives is the steps, plus the two ends: the
        givens the run starts from and the targets it was asked for. STEPS
        mode drops the data nodes wholesale, ends included, so only the steps
        remain. A dropped node's producers are wired straight to its
        consumers, so the shape of the pipeline is unchanged; only the rungs
        between steps go.
        """
        hide_data = self._mode is DagMode.STEPS
        preds, succs = self._neighbours(nodes, edges)
        drop: set[str] = set()
        for name, kind in nodes.items():
            if not self._is_data(name):
                # with the data gone, the synthetic roots connect nothing to
                # nothing: `given` would fan out to every step that reads an
                # environment, which is every step
                if hide_data and name in SYNTHETIC:
                    drop.add(name)
                continue
            if hide_data:
                drop.add(name)
            elif kind is NodeKind.TARGET:
                continue
            elif "given" not in preds[name]:
                drop.add(name)                      # an intermediate product

        kept = {n: k for n, k in nodes.items() if n not in drop}
        bypassed: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()

        # Hiding all data drops whole chains at once (given -> data -> step), so
        # bypassing has to walk through runs of dropped nodes rather than step
        # over one at a time.
        def survivors(start: str) -> list[str]:
            out: list[str] = []
            walked: set[str] = set()

            def walk(n: str) -> None:
                for s in succs[n]:
                    if s in walked:
                        continue
                    walked.add(s)
                    if s in drop:
                        walk(s)
                    else:
                        out.append(s)

            walk(start)
            return out

        for src in nodes:
            if src in drop:
                continue
            for dst in survivors(src):
                if src == dst or (src, dst) in seen:
                    continue
                seen.add((src, dst))
                bypassed.append((src, dst))
        return kept, bypassed

    def _legend_blocks(
        self,
    ) -> tuple[list[tuple[Layout, dict[str, Label], dict[str, NodeKind]]], Colouring]:
        nodes, edges = self._cut()
        preds, succs = self._neighbours(nodes, edges)
        labels = self.labels
        order: list[str] = []
        sig: dict[str, tuple[set[str], set[str]]] = {}
        hue_of: dict[str, str] = {}
        whole = layout(nodes, edges, blocks=step_blocks(nodes, edges))
        base = self.colouring(whole)

        for name, kind in nodes.items():
            if kind is not NodeKind.TRANSFORM or name in SYNTHETIC:
                continue
            key = labels[name].name
            declared = self._declared.get(name)
            if declared is not None:
                ins, outs = set(declared[0]), set(declared[1])
            else:
                ins = {n for n in preds[name] if self._is_data(n)}
                outs = {n for n in succs[name] if self._is_data(n)}
            if key in sig:
                sig[key][0].update(ins)
                sig[key][1].update(outs)
                continue
            sig[key] = (ins, outs)
            hue_of.setdefault(key, base.nodes.get(name, ""))
            order.append(key)

        blocks: list[tuple[Layout, dict[str, Label], dict[str, NodeKind]]] = []
        nodes_hue: dict[str, str] = dict(base.nodes)
        for key in order:
            ins, outs = sig[key]
            tid = f"\0{key}"          # cannot collide with a type name
            block: dict[str, NodeKind] = {}
            edges: list[tuple[str, str]] = []
            lab: dict[str, Label] = {tid: Label(name=key)}
            for name in sorted(ins):
                block[name] = NodeKind.DATA
                lab[name] = labels.get(name) or default_label(name)
                edges.append((name, tid))
            block[tid] = NodeKind.TRANSFORM
            for name in sorted(outs):
                # A block is a signature, not a run. An output that happens to
                # be a target of this particular plan is still just an output
                # of the transform, so it draws like every other one.
                block[name] = NodeKind.DATA
                lab[name] = labels.get(name) or default_label(name)
                edges.append((tid, name))
            if hue_of.get(key):
                nodes_hue[tid] = hue_of[key]
            blocks.append((layout(block, edges, blocks=step_blocks(block, edges)), lab, block))

        tinted = Colouring(
            nodes=nodes_hue,
            edges={
                (a, b): nodes_hue[a]
                for lay, _, _ in blocks
                for a, b in lay.edges
                if a in nodes_hue
            },
        )
        return blocks, tinted

    @property
    def labels(self) -> dict[str, Label]:
        return {n: self._labels.get(n) or default_label(n) for n in self._nodes}

    def to_dot(self) -> str:
        nodes, edges = self._graph()
        lines = ["digraph G {"]
        lines += [
            f'graph [fontname="{self._font}", rankdir="{self._rankdir}"];',
            f'node  [fontname="{self._font}"];',
            f'edge  [fontname="{self._font}"];',
        ]
        labels = self.labels
        for name, kind in nodes.items():
            lines.append(self._render_node(kind, name, labels[name]))
        for src, dst in edges:
            lines.append(f'    "{src}" -> "{dst}";')
        lines.append("}")
        return "\n".join(lines)

    def colouring(self, lay: Layout | None = None) -> Colouring:
        if self._monochrome:
            return Colouring()
        return colour_layout(
            lay or self.layout(), self._colour,
            palette=self._colour_palette, overrides=self._colour_overrides,
        )

    def to_text(self, *, unicode: bool = True, color: bool = False) -> str:
        self._reject_legend("text")
        lay = self.layout()
        return render_text(
            lay, self._theme.styles, kinds=self._nodes, labels=self.labels,
            unicode=unicode, color=color,
            colour=self.colouring(lay),
        )

    def geometry(
        self,
        lay: Layout | None = None,
        *,
        font_size: float = 13.0,
        max_label_chars: int = DEFAULT_LABEL_CHARS,
        min_lanes: int = 0,
        rows_y: Sequence[float] = (),
    ) -> Geometry:
        lay = lay or self.layout()
        return _geometry(
            lay, self._theme.styles, kinds=self._nodes, labels=self.labels,
            label_mode=self._label_mode,
            font_size=font_size, max_label_chars=max_label_chars,
            min_lanes=min_lanes, rows_y=rows_y,
        )

    def to_svg(self) -> str:
        if self._mode is DagMode.LEGEND:
            blocks, colour = self._legend_blocks()
            return render_legend(
                blocks, self._theme.styles, label_mode=self._label_mode,
                font=self._font, colour=colour, plate=self._theme.plate,
                columns=self._legend_columns,
            )
        lay = self.layout()
        return render_svg(
            lay, self._theme.styles, kinds=self._nodes, labels=self.labels,
            label_mode=self._label_mode, font=self._font,
            colour=self.colouring(lay), plate=self._theme.plate,
        )

    def _reject_legend(self, what: str) -> None:
        if self._mode is DagMode.LEGEND:
            raise ValueError(
                f"the legend mode composes many small drawings and has no {what}"
                " form; render it as svg"
            )

    def to_raster_dot(self) -> str:
        self._reject_legend("raster")
        lay = self.layout()
        return raster_dot(
            lay, self._theme.styles, kinds=self._nodes, labels=self.labels,
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
        if kind is NodeKind.TRANSFORM:
            attrs = ['shape="oval"', 'style="filled"', 'fillcolor="#CCCCCC"']
        else:
            attrs = ['shape="box"']
            if kind is NodeKind.TARGET:
                attrs.append("peripheries=2")
        if label is not None and label.full != name:
            attrs.append(f'label="{dot_escape(label.full)}"')
        return f'"{name}" [{", ".join(attrs)}]'
