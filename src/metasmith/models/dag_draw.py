from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass, field
from enum import Enum
from math import atan2, cos, pi, sin
from pathlib import Path
from typing import Any, Mapping, Sequence
from xml.sax.saxutils import escape

from .dag_colour import Colouring
from .dag_layout import Layout

__all__ = [
    "Style", "Plate", "Label", "LabelMode", "default_label", "dot_escape",
    "marker_size", "tint", "render_text", "render_svg", "render_legend",
    "raster_dot", "render_raster", "geometry", "Geometry", "NodeGeometry",
    "EdgeGeometry",
]

_NO_COLOUR = Colouring()

DEFAULT_LABEL_CHARS = 32
# where a node's band sits in the gap above it, as a fraction of the gap
BAND = 0.5


class LabelMode(Enum):
    COLUMN = "column"
    BESIDE = "beside"


@dataclass(frozen=True)
class Style:
    marker: str = "o"
    ascii_marker: str = "o"
    fill: str = "#FFFFFF"
    stroke: str = "#555555"
    text: str = "#111111"
    muted: str = "#8A8A8A"
    weight: str = "normal"
    rx: int = 3
    shape: str = "box"
    gv_style: str = "filled"
    gv_attrs: str = ""
    ansi: str = ""
    svg_shape: str = "circle"
    marker_scale: float = 1.0
    stroke_width: float = 1.6
    solid: bool = False
    # label column only: `indent` shifts the label right, in ems; `heading`
    # rules the full label column above the label
    indent: float = 0.0
    heading: bool = False


@dataclass(frozen=True)
class Plate:
    background: str = "#FFFFFF"
    edge: str = "#666666"
    paint_background: bool = True
    # only the legend draws a rule: the frame around each detached block
    rule: str = "#E3E3E3"


_DEFAULT_PLATE = Plate()


@dataclass(frozen=True)
class Label:
    name: str
    namespace: str = ""
    full: str = ""

    def __post_init__(self):
        if not self.full:
            joined = f"{self.namespace}::{self.name}" if self.namespace else self.name
            object.__setattr__(self, "full", joined)


def default_label(node_id: str) -> Label:
    if "::" in node_id:
        ns, name = node_id.split("::", maxsplit=1)
        return Label(name=name, namespace=ns, full=node_id)
    return Label(name=node_id, full=node_id)


_DEFAULT_STYLE = Style()

_TRIANGLE_H = 0.8660254037844386


def marker_size(st: Style, marker_d: float) -> tuple[float, float]:
    w = marker_d * st.marker_scale
    return w, (w * _TRIANGLE_H if st.svg_shape == "triangle_down" else w)


def _labels_for(lay: Layout, labels: Mapping[str, Label] | None) -> dict[str, Label]:
    labels = labels or {}
    return {n: labels.get(n) or default_label(n) for n in lay.order}


_UP, _DOWN, _LEFT, _RIGHT = 1, 2, 4, 8

_GLYPHS = {
    0: " ", 1: "│", 2: "│", 3: "│", 4: "─", 5: "┘", 6: "┐", 7: "┤",
    8: "─", 9: "└", 10: "┌", 11: "├", 12: "─", 13: "┴", 14: "┬", 15: "┼",
}
_GLYPHS_ASCII = {
    0: " ", 1: "|", 2: "|", 3: "|", 4: "-", 5: "+", 6: "+", 7: "+",
    8: "-", 9: "+", 10: "+", 11: "+", 12: "-", 13: "+", 14: "+", 15: "+",
}


def _paint(mask: list[int], lo: int, hi: int) -> None:
    mask[2 * lo] |= _RIGHT
    mask[2 * hi] |= _LEFT
    for c in range(2 * lo + 1, 2 * hi):
        mask[c] |= _LEFT | _RIGHT


def render_text(
    lay: Layout,
    style: Mapping[Any, Style] | None = None,
    *,
    kinds: Mapping[str, Any] | None = None,
    labels: Mapping[str, Label] | None = None,
    unicode: bool = True,
    color: bool = False,
    colour: Colouring | None = None,
) -> str:
    if lay.height == 0:
        return ""
    style = style or {}
    kinds = kinds or {}
    colour = colour or _NO_COLOUR
    lab = _labels_for(lay, labels)
    glyphs = _GLYPHS if unicode else _GLYPHS_ASCII
    columns = 2 * lay.width - 1
    arrow = "↺" if unicode else "^"

    back_from: dict[str, list[str]] = {}
    for src, dst in lay.back:
        back_from.setdefault(src, []).append(dst)

    out: list[str] = []
    for r, name in enumerate(lay.order):
        bar = lay.bars.get(name)
        if bar is not None and bar.span[0] < bar.span[1]:
            out.append("".join(glyphs[m] for m in _connector(lay, bar, columns)).rstrip())
        elif bar is None and r and lay.col[lay.order[r - 1]] == lay.col[name]:
            mask = [0] * columns
            for u in lay.live(2 * r - 1):
                mask[2 * u.col] |= _UP | _DOWN
            out.append("".join(glyphs[m] for m in mask).rstrip())
        st = style.get(kinds.get(name), _DEFAULT_STYLE)
        mask = [0] * columns
        for u in lay.live(2 * r):
            mask[2 * u.col] |= _UP | _DOWN
        cells = [glyphs[m] for m in mask]
        c = 2 * lay.col[name]
        cells[c] = st.marker if unicode else st.ascii_marker
        escape_ = _ansi(colour.nodes.get(name)) or st.ansi
        if color and escape_:
            cells[c] = f"{escape_}{cells[c]}\033[0m"
        label = lab[name].full
        if name in back_from:
            label += "  " + " ".join(f"{arrow} {lab[d].full}" for d in sorted(back_from[name]))
        out.append(("".join(cells) + "  " + label).rstrip())
    return "\n".join(out) + "\n"


def _ansi(hex_colour: str | None) -> str:
    if not hex_colour:
        return ""
    h = hex_colour.lstrip("#")
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return f"\033[38;2;{r};{g};{b}m"


def _connector(lay: Layout, bar, columns: int) -> list[int]:
    mask = [0] * columns
    feeding = {f.src for f in bar.feeds}
    for u in lay.live(bar.band):
        if u.node != bar.node and u.node not in feeding:
            mask[2 * u.col] |= _UP | _DOWN
    for f in bar.feeds:
        mask[2 * f.col] |= _UP if f.turns else _UP | _DOWN
    mask[2 * bar.col] |= _DOWN
    _paint(mask, *bar.span)
    return mask


@dataclass(frozen=True)
class _Grid:
    col_x: tuple[float, ...]
    label_x: tuple[float, ...]
    anchor: str
    row_pitch: float
    lane_pitch: float
    marker_d: float
    font_size: float
    margin: float
    width: float
    height: float
    rows_y: tuple[float, ...] = ()
    label_right: float = 0.0

    def x(self, col: int) -> float:
        return self.col_x[col]

    def y(self, row: int) -> float:
        if not self.rows_y:
            return self.margin + (row + 0.5) * self.row_pitch
        return self.rows_y[row]

    def half(self, half_row: int) -> float:
        """y of a half-row: a node's row when even, the band above it when odd."""
        if half_row % 2 == 0:
            return self.y(half_row // 2)
        below = (half_row + 1) // 2
        return self.y(below) - BAND * (self.y(below) - self.y(below - 1))


@dataclass(frozen=True)
class _Arc:
    radius: float
    sweep: int
    centre: tuple[float, float]


@dataclass(frozen=True)
class _Drawn:
    namespace: str
    name: str
    full: str
    width: float
    truncated: bool


def _clip(text: str, max_chars: int) -> tuple[str, bool]:
    if len(text) <= max_chars:
        return text, False
    return text[: max(max_chars - 1, 0)] + "…", True


def _grid(
    lay: Layout,
    *,
    font_size: float,
    labels: Mapping[str, Label] | None = None,
    mode: LabelMode = LabelMode.COLUMN,
    max_chars: int = DEFAULT_LABEL_CHARS,
    min_lanes: int = 0,
    rows_y: Sequence[float] = (),
    indents: Mapping[str, float] | None = None,
) -> tuple[_Grid, dict[str, _Drawn]]:
    indents = indents or {}
    lab = _labels_for(lay, labels)
    cols = max(lay.width, min_lanes)
    shift = cols - lay.width
    char_w = font_size * 0.58
    marker_d = 0.82 * font_size
    lane_pitch = 1.30 * font_size
    label_pad = 0.55 * font_size

    drawn: dict[str, _Drawn] = {}
    for n in lay.order:
        L = lab[n]
        ns, ns_cut = _clip(L.namespace, 2 * max_chars)
        name, name_cut = _clip(L.name, max_chars)
        drawn[n] = _Drawn(
            namespace=ns,
            name=name,
            full=L.full,
            width=char_w * max(len(name), len(ns) / 2),
            truncated=ns_cut or name_cut,
        )

    margin = 1.5 * font_size
    row_pitch = max(2.4 * font_size, lane_pitch + marker_d)
    if mode is LabelMode.BESIDE:
        col_w = [lane_pitch] * cols
        for n in lay.order:
            c = lay.col[n] + shift
            col_w[c] = max(col_w[c], marker_d + label_pad + drawn[n].width)
        col_x, label_x, cursor = [], [], margin
        for w in col_w:
            col_x.append(cursor + w - marker_d / 2)
            label_x.append(col_x[-1] - marker_d / 2 - label_pad)
            cursor += w
        anchor = "end"
        width = cursor + margin
    else:
        col_x = [margin + marker_d / 2 + c * lane_pitch for c in range(cols)]
        column = margin + marker_d + (cols - 1) * lane_pitch + label_pad
        label_x = [column] * cols
        anchor = "start"
        width = column + max(
            (indents.get(n, 0.0) * font_size + d.width for n, d in drawn.items()),
            default=0.0,
        ) + margin

    return (
        _Grid(
            col_x=tuple(col_x[shift:]),
            label_x=tuple(label_x[shift:]),
            anchor=anchor,
            row_pitch=row_pitch,
            lane_pitch=lane_pitch,
            marker_d=marker_d,
            font_size=font_size,
            margin=margin,
            width=max(width, 2 * margin),
            height=(
                rows_y[-1] + margin if len(rows_y) else 2 * margin + lay.height * row_pitch
            ),
            rows_y=tuple(rows_y),
            label_right=width - margin,
        ),
        drawn,
    )


def _pixel_path(
    lay: Layout,
    src: str,
    dst: str,
    g: _Grid,
    style: Mapping[Any, Style],
    kinds: Mapping[str, Any],
) -> tuple[list[tuple[float, float]], dict[int, _Arc]]:
    points = [(g.x(c), g.half(h)) for h, c in lay.route(src, dst)]
    top = style.get(kinds.get(src), _DEFAULT_STYLE)
    bot = style.get(kinds.get(dst), _DEFAULT_STYLE)
    points[0] = (points[0][0], points[0][1] + marker_size(top, g.marker_d)[1] / 2)
    points[-1] = (points[-1][0], points[-1][1] - marker_size(bot, g.marker_d)[1] / 2)
    return _round_corners(points, g.lane_pitch / 2)


def _round_corners(
    points: list[tuple[float, float]], bevel: float
) -> tuple[list[tuple[float, float]], dict[int, _Arc]]:
    pts: list[tuple[float, float]] = []
    for p in points:
        if not pts or p != pts[-1]:
            pts.append(p)
    if len(pts) < 3:
        return pts, {}

    legs = [
        ((b[0] - a[0]) ** 2 + (b[1] - a[1]) ** 2) ** 0.5
        for a, b in zip(pts, pts[1:])
    ]
    cut = [0.0] + [bevel] * (len(pts) - 2) + [0.0]
    for i, leg in enumerate(legs):
        want = cut[i] + cut[i + 1]
        if want > leg:
            cut[i] *= leg / want
            cut[i + 1] *= leg / want

    out = [pts[0]]
    arcs: dict[int, _Arc] = {}
    for i in range(1, len(pts) - 1):
        (ax, ay), (bx, by), (cx, cy) = pts[i - 1], pts[i], pts[i + 1]
        d, la, lc = cut[i], legs[i - 1], legs[i]
        start = (bx + (ax - bx) * d / la, by + (ay - by) * d / la)
        end = (bx + (cx - bx) * d / lc, by + (cy - by) * d / lc)
        if start != out[-1]:
            out.append(start)
        if end == out[-1]:
            continue
        turn = (bx - ax) * (cy - by) - (by - ay) * (cx - bx)
        arcs[len(out) - 1] = _Arc(
            radius=d,
            sweep=1 if turn > 0 else 0,
            centre=(start[0] + end[0] - bx, start[1] + end[1] - by),
        )
        out.append(end)
    out.append(pts[-1])
    return out, arcs


def _flatten(
    points: list[tuple[float, float]], arcs: dict[int, _Arc], steps: int = 4
) -> list[tuple[float, float]]:
    out = [points[0]]
    for i, end in enumerate(points[1:]):
        arc = arcs.get(i)
        if arc is None:
            out.append(end)
            continue
        (cx, cy), start = arc.centre, points[i]
        a0 = atan2(start[1] - cy, start[0] - cx)
        a1 = atan2(end[1] - cy, end[0] - cx)
        while a1 - a0 > pi:
            a1 -= 2 * pi
        while a0 - a1 > pi:
            a1 += 2 * pi
        for s in range(1, steps + 1):
            a = a0 + (a1 - a0) * s / steps
            out.append((cx + arc.radius * cos(a), cy + arc.radius * sin(a)))
    return out


@dataclass(frozen=True)
class NodeGeometry:
    name: str
    kind: Any
    row: int
    col: int
    cx: float
    cy: float
    label_x: float
    marker_w: float
    marker_h: float
    namespace: str
    label: str
    full: str
    truncated: bool
    indent: float = 0.0


@dataclass(frozen=True)
class EdgeGeometry:
    src: str
    dst: str
    back: bool
    d: str
    path: tuple = field(default=(), compare=False, repr=False)


@dataclass(frozen=True)
class Geometry:
    width: float
    height: float
    font_size: float
    marker_d: float
    row_pitch: float
    lane_pitch: float
    anchor: str
    nodes: tuple[NodeGeometry, ...]
    edges: tuple[EdgeGeometry, ...]
    label_right: float = 0.0


def geometry(
    lay: Layout,
    style: Mapping[Any, Style] | None = None,
    *,
    kinds: Mapping[str, Any] | None = None,
    labels: Mapping[str, Label] | None = None,
    label_mode: LabelMode = LabelMode.COLUMN,
    max_label_chars: int = DEFAULT_LABEL_CHARS,
    font_size: float = 13.0,
    min_lanes: int = 0,
    rows_y: Sequence[float] = (),
) -> Geometry:
    style = style or {}
    kinds = kinds or {}
    indents = _indents(lay, style, kinds, label_mode)
    g, drawn = _grid(
        lay, font_size=font_size, labels=labels,
        mode=label_mode, max_chars=max_label_chars,
        min_lanes=min_lanes, rows_y=rows_y, indents=indents,
    )
    nodes = []
    for name in lay.order:
        kind = kinds.get(name)
        d = drawn[name]
        mw, mh = marker_size(style.get(kind, _DEFAULT_STYLE), g.marker_d)
        c, r = lay.col[name], lay.row[name]
        nodes.append(NodeGeometry(
            name=name, kind=kind, row=r, col=c,
            cx=g.x(c), cy=g.y(r), label_x=g.label_x[c],
            marker_w=mw, marker_h=mh,
            namespace=d.namespace, label=d.name, full=d.full, truncated=d.truncated,
            indent=indents.get(name, 0.0) * g.font_size,
        ))
    edges = []
    for src, dst in lay.edges:
        points, arcs = _pixel_path(lay, src, dst, g, style, kinds)
        edges.append(EdgeGeometry(
            src=src, dst=dst, back=False, d=_svg_path(points, arcs), path=(points, arcs),
        ))
    edges += [EdgeGeometry(src=src, dst=dst, back=True, d="") for src, dst in lay.back]
    return Geometry(
        width=g.width, height=g.height, font_size=g.font_size,
        marker_d=g.marker_d, row_pitch=g.row_pitch, lane_pitch=g.lane_pitch,
        anchor=g.anchor, nodes=tuple(nodes), edges=tuple(edges),
        label_right=g.label_right,
    )


def _indents(
    lay: Layout, style: Mapping[Any, Style], kinds: Mapping[str, Any], mode: LabelMode,
) -> dict[str, float]:
    if mode is not LabelMode.COLUMN:
        return {}
    return {n: style.get(kinds.get(n), _DEFAULT_STYLE).indent for n in lay.order}


def render_svg(
    lay: Layout,
    style: Mapping[Any, Style] | None = None,
    *,
    kinds: Mapping[str, Any] | None = None,
    labels: Mapping[str, Label] | None = None,
    label_mode: LabelMode = LabelMode.COLUMN,
    max_label_chars: int = DEFAULT_LABEL_CHARS,
    font: str = "Arial",
    font_size: float = 13.0,
    colour: Colouring | None = None,
    plate: Plate | None = None,
) -> str:
    style = style or {}
    colour = colour or _NO_COLOUR
    plate = plate or _DEFAULT_PLATE
    g = geometry(
        lay, style, kinds=kinds, labels=labels, label_mode=label_mode,
        max_label_chars=max_label_chars, font_size=font_size,
    )
    parts = [
        '<?xml version="1.0" encoding="UTF-8" standalone="no"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink"'
        f' width="{g.width:.0f}" height="{g.height:.0f}"'
        f' viewBox="0 0 {g.width:.0f} {g.height:.0f}">',
        *([f'<rect width="{g.width:.0f}" height="{g.height:.0f}" fill="{plate.background}"/>']
          if plate.paint_background else []),
        *_svg_body(g, style, font=font, font_size=font_size, colour=colour, plate=plate),
        "</svg>",
    ]
    return "\n".join(parts) + "\n"


def _svg_body(
    g: Geometry,
    style: Mapping[Any, Style],
    *,
    font: str,
    font_size: float,
    colour: Colouring,
    plate: Plate,
) -> list[str]:
    parts = [
        f'<g fill="none" stroke="{plate.edge}" stroke-width="1.4"'
        ' stroke-linejoin="round" stroke-linecap="butt">',
    ]
    for hue, d in _strokes(
        (colour.edges.get((e.src, e.dst)), *e.path) for e in g.edges if not e.back
    ):
        stroke = f' stroke="{hue}"' if hue else ""
        parts.append(f'<path d="{d}"{stroke}/>')
    parts.append("</g>")

    for n in g.nodes:
        st = style.get(n.kind, _DEFAULT_STYLE)
        cx, cy, lx = n.cx, n.cy, n.label_x + n.indent
        parts.append(f'<g><title>{escape(n.full)}</title>')
        parts.append(_svg_marker(st, cx, cy, g.marker_d, colour.nodes.get(n.name)))
        if st.heading and g.anchor == "start":
            top = cy - (0.78 if n.namespace else 0.36) * font_size
            y = top - 0.3 * font_size
            parts.append(
                f'<line x1="{lx:.1f}" y1="{y:.1f}" x2="{g.label_right:.1f}" y2="{y:.1f}"'
                f' stroke="{st.text}" stroke-width="1"/>'
            )
        if n.namespace:
            parts.append(
                f'<text x="{lx:.1f}" y="{cy - 0.42 * font_size:.1f}"'
                f' font-family="{escape(font)}" font-size="{font_size / 2:.1f}"'
                f' fill="{st.muted}" text-anchor="{g.anchor}">{escape(n.namespace)}</text>'
            )
        parts.append(
            f'<text x="{lx:.1f}" y="{cy + 0.36 * font_size:.1f}"'
            f' font-family="{escape(font)}" font-size="{font_size:.0f}" fill="{st.text}"'
            f' font-weight="{st.weight}"'
            f' text-anchor="{g.anchor}">{escape(n.label)}</text></g>'
        )
    return parts


def render_legend(
    blocks: Sequence[tuple[Layout, Mapping[str, Label], Mapping[str, Any]]],
    style: Mapping[Any, Style] | None = None,
    *,
    label_mode: LabelMode = LabelMode.COLUMN,
    max_label_chars: int = DEFAULT_LABEL_CHARS,
    font: str = "Arial",
    font_size: float = 13.0,
    colour: Colouring | None = None,
    plate: Plate | None = None,
    columns: int = 0,
) -> str:
    """Compose small detached drawings into a grid of identically sized boxes.

    Every block is laid out against the widest block's column count, so the label
    column falls at the same x in all of them and the grid reads as one table
    rather than as a row of unrelated pictures.
    """
    style = style or {}
    colour = colour or _NO_COLOUR
    plate = plate or _DEFAULT_PLATE
    if not blocks:
        return (
            '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n'
            '<svg xmlns="http://www.w3.org/2000/svg" width="0" height="0"'
            ' viewBox="0 0 0 0"/>\n'
        )

    cols = max(lay.width for lay, _, _ in blocks)
    geos = [
        geometry(
            lay, style, kinds=kinds, labels=labels, label_mode=label_mode,
            max_label_chars=max_label_chars, font_size=font_size, min_lanes=cols,
        )
        for lay, labels, kinds in blocks
    ]
    box_w = max(g.width for g in geos)
    box_h = max(g.height for g in geos)

    per_row = columns if columns > 0 else max(1, round(len(geos) ** 0.5))
    rows = -(-len(geos) // per_row)
    gap = font_size
    width = 2 * gap + per_row * box_w + (per_row - 1) * gap
    height = 2 * gap + rows * box_h + (rows - 1) * gap

    parts = [
        '<?xml version="1.0" encoding="UTF-8" standalone="no"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink"'
        f' width="{width:.0f}" height="{height:.0f}"'
        f' viewBox="0 0 {width:.0f} {height:.0f}">',
        *([f'<rect width="{width:.0f}" height="{height:.0f}" fill="{plate.background}"/>']
          if plate.paint_background else []),
    ]
    for i, g in enumerate(geos):
        x = gap + (i % per_row) * (box_w + gap)
        y = gap + (i // per_row) * (box_h + gap)
        parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{box_w:.1f}" height="{box_h:.1f}"'
            f' rx="{0.5 * font_size:.1f}" fill="none" stroke="{plate.rule}"/>'
        )
        # left-aligned so the label columns line up down the grid; centred
        # vertically so a two-node block does not hang from the top edge
        parts.append(f'<g transform="translate({x:.1f},{y + (box_h - g.height) / 2:.1f})">')
        parts += _svg_body(g, style, font=font, font_size=font_size, colour=colour, plate=plate)
        parts.append("</g>")
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


def _strokes(edges) -> list[tuple[str | None, str]]:
    """Every edge's path as pieces drawn once each, rejoined into paths.

    Edges retrace each other: every child of a node runs down the same
    column, and every parent of a node shares its bar. An antialiased
    stroke drawn twice darkens its own edge, so a shared stretch looks
    thicker than a lone one. Straight pieces are cut at every point any
    piece touches and painted in draw order, the last edge over a stretch
    giving its colour, as when every edge was stroked. Pieces then rejoin
    where they meet in pairs, and through a junction wherever two carry
    straight on, so a corner is a join inside one path and never two
    strokes meeting end to end.
    """
    def q(p):
        return round(p[0], 2), round(p[1], 2)

    lines: dict[tuple[str, float], list[tuple[float, float, int]]] = {}
    arcs: dict[tuple, tuple[int, _Arc]] = {}
    hues: list[str | None] = []
    ends: set[tuple[float, float]] = set()
    loose: list[tuple[int, tuple, tuple]] = []
    for order, (hue, points, curves) in enumerate(edges):
        hues.append(hue)
        for i, (a, b) in enumerate(zip(points, points[1:])):
            a, b = q(a), q(b)
            if a == b:
                continue
            ends.update((a, b))
            if i in curves:
                arcs[(a, b, round(curves[i].radius, 2), curves[i].sweep)] = (order, curves[i])
            elif a[0] == b[0]:
                lines.setdefault(("v", a[0]), []).append((*sorted((a[1], b[1])), order))
            elif a[1] == b[1]:
                lines.setdefault(("h", a[1]), []).append((*sorted((a[0], b[0])), order))
            else:
                loose.append((order, a, b))

    pieces: list[tuple[tuple, tuple, str | None, _Arc | None]] = []
    for (axis, at), spans in lines.items():
        cuts = {c for lo, hi, _ in spans for c in (lo, hi)}
        cuts |= {p[1] if axis == "v" else p[0] for p in ends if p[0 if axis == "v" else 1] == at}
        cuts = sorted(cuts)
        for lo, hi in zip(cuts, cuts[1:]):
            owner = max((o for a, b, o in spans if a <= lo and hi <= b), default=None)
            if owner is not None:
                pa, pb = ((at, lo), (at, hi)) if axis == "v" else ((lo, at), (hi, at))
                pieces.append((pa, pb, hues[owner], None))
    for (a, b, _, _), (order, arc) in arcs.items():
        pieces.append((a, b, hues[order], arc))
    for order, a, b in loose:
        pieces.append((a, b, hues[order], None))

    def heading(k, at):
        a, b, _, arc = pieces[k]
        other = b if at == a else a
        if arc is None:
            dx, dy = other[0] - at[0], other[1] - at[1]
        else:
            rx, ry = at[0] - arc.centre[0], at[1] - arc.centre[1]
            dx, dy = -ry, rx
            if dx * (other[0] - at[0]) + dy * (other[1] - at[1]) < 0:
                dx, dy = -dx, -dy
        norm = (dx * dx + dy * dy) ** 0.5 or 1.0
        return dx / norm, dy / norm

    touching: dict[tuple, list[int]] = {}
    for k, (a, b, _, _) in enumerate(pieces):
        touching.setdefault(a, []).append(k)
        touching.setdefault(b, []).append(k)
    onward: dict[tuple[int, tuple], int] = {}
    for at, ks in touching.items():
        if len(ks) == 2 and pieces[ks[0]][2] == pieces[ks[1]][2]:
            onward[(ks[0], at)], onward[(ks[1], at)] = ks[1], ks[0]
            continue
        free = list(ks)
        while free:
            k = free.pop(0)
            hk = heading(k, at)
            for m in free:
                hm = heading(m, at)
                if pieces[m][2] == pieces[k][2] and hk[0] * hm[0] + hk[1] * hm[1] < -0.999:
                    free.remove(m)
                    onward[(k, at)], onward[(m, at)] = m, k
                    break

    def step(k, start):
        a, b, _, arc = pieces[k]
        end = b if start == a else a
        if arc is None:
            return end, f"L {end[0]:.1f},{end[1]:.1f}"
        sweep = arc.sweep if start == a else 1 - arc.sweep
        return end, f"A {arc.radius:.1f},{arc.radius:.1f} 0 0 {sweep} {end[0]:.1f},{end[1]:.1f}"

    drawn = [False] * len(pieces)
    out = []
    starts = [(k, p) for k, (a, b, _, _) in enumerate(pieces) for p in (a, b) if (k, p) not in onward]
    starts += [(k, pieces[k][0]) for k in range(len(pieces))]
    for k, at in starts:
        if drawn[k]:
            continue
        hue = pieces[k][2]
        d = [f"M {at[0]:.1f},{at[1]:.1f}"]
        while k is not None and not drawn[k]:
            drawn[k] = True
            at, move = step(k, at)
            d.append(move)
            k = onward.get((k, at))
        out.append((hue, " ".join(d)))
    return out


def _svg_path(points: list[tuple[float, float]], arcs: dict[int, _Arc]) -> str:
    d = [f"M {points[0][0]:.1f},{points[0][1]:.1f}"]
    for i, (x, y) in enumerate(points[1:]):
        arc = arcs.get(i)
        if arc is None:
            d.append(f"L {x:.1f},{y:.1f}")
        else:
            d.append(
                f"A {arc.radius:.1f},{arc.radius:.1f} 0 0 {arc.sweep} {x:.1f},{y:.1f}"
            )
    return " ".join(d)


def tint(st: Style, hue: str | None) -> tuple[str, str]:
    if not hue:
        return st.fill, st.stroke
    if st.solid:
        return hue, st.stroke
    return st.fill, hue


def _svg_marker(
    st: Style, cx: float, cy: float, marker_d: float, hue: str | None = None
) -> str:
    w, h = marker_size(st, marker_d)
    fill, stroke = tint(st, hue)
    if st.svg_shape == "circle":
        return (
            f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{w / 2:.1f}"'
            f' fill="{fill}" stroke="{stroke}" stroke-width="{st.stroke_width}"/>'
        )
    if st.svg_shape == "triangle_down":
        pts = (
            f"{cx - w / 2:.1f},{cy - h / 2:.1f} {cx + w / 2:.1f},{cy - h / 2:.1f}"
            f" {cx:.1f},{cy + h / 2:.1f}"
        )
        return (
            f'<polygon points="{pts}" fill="{fill}" stroke="{stroke}"'
            f' stroke-width="{st.stroke_width}" stroke-linejoin="round"/>'
        )

    return (
        f'<rect x="{cx - w / 2:.1f}" y="{cy - h / 2:.1f}" width="{w:.1f}"'
        f' height="{h:.1f}" rx="{st.rx}"'
        f' fill="{fill}" stroke="{stroke}" stroke-width="{st.stroke_width}"/>'
    )


def raster_dot(
    lay: Layout,
    style: Mapping[Any, Style] | None = None,
    *,
    kinds: Mapping[str, Any] | None = None,
    labels: Mapping[str, Label] | None = None,
    label_mode: LabelMode = LabelMode.COLUMN,
    max_label_chars: int = DEFAULT_LABEL_CHARS,
    font: str = "Arial",
    font_size: float = 13.0,
    colour: Colouring | None = None,
    plate: Plate | None = None,
) -> str:
    style = style or {}
    colour = colour or _NO_COLOUR
    plate = plate or _DEFAULT_PLATE
    indents = _indents(lay, style, kinds or {}, label_mode)
    g, drawn = _grid(
        lay, font_size=font_size, labels=labels,
        mode=label_mode, max_chars=max_label_chars, indents=indents,
    )
    lines = [
        "digraph G {",
        f'graph [fontname="{font}", outputorder="edgesfirst",'
        f' bgcolor="{plate.background if plate.paint_background else "transparent"}"];',
        f'node  [fontname="{font}", fontsize={font_size:.0f}, fixedsize=true];',
        f'edge  [fontname="{font}", color="{plate.edge}", dir="none"];',
    ]
    prefix = _label_prefix(lay)
    kinds = kinds or {}
    for name in lay.order:
        st = style.get(kinds.get(name), _DEFAULT_STYLE)
        d = drawn[name]
        c = lay.col[name]
        x, y = g.x(c), g.height - g.y(lay.row[name])
        w, h = marker_size(st, g.marker_d)
        fill, stroke = tint(st, colour.nodes.get(name))
        lines.append(
            f'  "{name}" [pos="{x:.1f},{y:.1f}!", label="",'
            f' width={w / 72:.3f}, height={h / 72:.3f},'
            f' penwidth={st.stroke_width:g},'
            f' shape="{st.shape}", style="{st.gv_style}",'
            f' fillcolor="{fill}", color="{stroke}"'
            f'{", " + st.gv_attrs if st.gv_attrs else ""}];'
        )
        box = max(d.width, 1.0)
        half = box / 2 if g.anchor == "start" else -box / 2
        lx = g.label_x[c] + indents.get(name, 0.0) * font_size
        lines.append(
            f'  "{prefix}{name}" [pos="{lx + half:.1f},'
            f'{y:.1f}!", shape="plaintext", style="", width={box / 72:.3f},'
            f' height={2.0 * font_size / 72:.3f},'
            f" label=<{_html_label(d, st, font_size, box, g.anchor)}>];"
        )
    for src, dst in lay.edges:
        pts = [
            (x, g.height - y)
            for x, y in _flatten(*_pixel_path(lay, src, dst, g, style, kinds))
        ]
        hue = colour.edges.get((src, dst))
        tone = f', color="{hue}"' if hue else ""
        lines.append(f'  "{src}" -> "{dst}" [pos="{_spline(pts)}"{tone}];')
    for src, dst in lay.back:
        lines.append(f'  "{src}" -> "{dst}" [style="dashed"];')
    lines.append("}")
    return "\n".join(lines)


def _label_prefix(lay: Layout) -> str:
    prefix = "__label__"
    while any(n.startswith(prefix) for n in lay.order):
        prefix = "_" + prefix
    return prefix


def _html_label(
    d: _Drawn, st: Style, font_size: float, width: float, anchor: str
) -> str:
    side = "LEFT" if anchor == "start" else "RIGHT"
    lines = []
    if d.namespace:
        lines.append(
            f'<FONT POINT-SIZE="{font_size / 2:.1f}" COLOR="{st.muted}">'
            f'{escape(d.namespace)}</FONT><BR ALIGN="{side}"/>'
        )
    name = escape(d.name)
    if st.weight != "normal":
        name = f"<B>{name}</B>"
    lines.append(
        f'<FONT POINT-SIZE="{font_size:.1f}" COLOR="{st.text}">'
        f'{name}</FONT><BR ALIGN="{side}"/>'
    )
    return (
        '<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="0"'
        f' FIXEDSIZE="TRUE" WIDTH="{width:.0f}" HEIGHT="{2.0 * font_size:.0f}">'
        f'<TR><TD ALIGN="{side}" BALIGN="{side}">{"".join(lines)}</TD></TR></TABLE>'
    )


def dot_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _spline(points: list[tuple[float, float]]) -> str:
    def _fmt(p):
        return f"{p[0]:.1f},{p[1]:.1f}"

    end = points[-1]
    out = [f"e,{_fmt(end)}", _fmt(points[0])]
    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        dx, dy = (x1 - x0) / 3, (y1 - y0) / 3
        out += [_fmt((x0 + dx, y0 + dy)), _fmt((x0 + 2 * dx, y0 + 2 * dy)), _fmt((x1, y1))]
    return " ".join(out)


def render_raster(dot: str, out: Path, format: str) -> Path:
    neato = shutil.which("neato")
    if neato is None:
        raise RuntimeError(
            f"rendering {format} needs the graphviz `neato` binary on PATH"
            " (layout is done by metasmith; neato only rasterizes)"
        )
    proc = subprocess.run(
        [neato, "-n2", f"-T{format}", "-o", str(out)],
        input=dot.encode(),
        capture_output=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"neato failed rendering {format}: {proc.stderr.decode(errors='replace').strip()}"
        )
    return out
