from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum, auto
from pathlib import Path
from typing import Sequence

from .dag_colour import SCHEMES, Colouring, colour_layout
from .dag_draw import (
    DEFAULT_LABEL_CHARS, Geometry, Label, LabelMode, Plate, Style,
    default_label, dot_escape, raster_dot, render_raster, render_svg, render_text,
)
from .dag_draw import geometry as _geometry
from .dag_layout import Layout, layout


class NodeKind(Enum):
    TRANSFORM = auto()
    DATA      = auto()
    TARGET    = auto()


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
        svg_shape="circle", marker_scale=1.0, stroke_width=1.5,
    ),
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
    plate: Plate
    styles: dict[NodeKind, Style]


LIGHT = Theme(plate=Plate(), styles=STYLES)

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
        self._colour = colour
        theme_obj = THEMES[theme]
        if not background:
            theme_obj = replace(theme_obj, plate=replace(theme_obj.plate, paint_background=False))
        self._theme = theme_obj
        self._nodes: dict[str, NodeKind] = {}
        self._labels: dict[str, Label] = {}
        self._edges: list[tuple[str, str]] = []
        self._seen_edges: set[tuple[str, str]] = set()

    def add_node(self, kind: NodeKind, name: str, label: Label | None = None) -> None:
        self._nodes.setdefault(name, kind)
        if label is not None:
            self._labels.setdefault(name, label)

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
        self._edges = [e for e in self._edges if name not in e]
        self._seen_edges = {e for e in self._seen_edges if name not in e}

    def layout(self, order: Sequence[str] | None = None) -> Layout:
        return layout(self._nodes, self._edges, order)

    @property
    def labels(self) -> dict[str, Label]:
        return {n: self._labels.get(n) or default_label(n) for n in self._nodes}

    def to_dot(self) -> str:
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
        return colour_layout(lay or self.layout(), self._colour)

    def to_text(self, *, unicode: bool = True, color: bool = False) -> str:
        lay = self.layout()
        return render_text(
            lay, self._theme.styles, labels=self.labels,
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
            lay, self._theme.styles, labels=self.labels, label_mode=self._label_mode,
            font_size=font_size, max_label_chars=max_label_chars,
            min_lanes=min_lanes, rows_y=rows_y,
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
        if kind is NodeKind.TRANSFORM:
            attrs = ['shape="oval"', 'style="filled"', 'fillcolor="#CCCCCC"']
        else:
            attrs = ['shape="box"']
            if kind is NodeKind.TARGET:
                attrs.append("peripheries=2")
        if label is not None and label.full != name:
            attrs.append(f'label="{dot_escape(label.full)}"')
        return f'"{name}" [{", ".join(attrs)}]'
