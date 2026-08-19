from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from enum import Enum
from math import atan2, cos, pi, sin
from pathlib import Path
from typing import Any, Mapping, Sequence
from xml.sax.saxutils import escape

from .dag_colour import Colouring
from .dag_layout import Layout

__all__ = [
    "Style", "Plate", "Label", "LabelMode", "default_label", "dot_escape",
    "marker_size", "tint", "render_text", "render_svg", "raster_dot",
    "render_raster", "geometry", "Geometry", "NodeGeometry", "EdgeGeometry",
]

_NO_COLOUR = Colouring()

DEFAULT_LABEL_CHARS = 32
BAND = 0.10


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
    rx: int = 3
    shape: str = "box"
    gv_style: str = "filled"
    gv_attrs: str = ""
    ansi: str = ""
    svg_shape: str = "circle"
    marker_scale: float = 1.0
    stroke_width: float = 1.6
    solid: bool = False


@dataclass(frozen=True)
class Plate:
    background: str = "#FFFFFF"
    edge: str = "#666666"
    paint_background: bool = True


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
    return {n.name: labels.get(n.name) or default_label(n.name) for n in lay.nodes}


_UP, _DOWN, _LEFT, _RIGHT = 1, 2, 4, 8

_GLYPHS = {
    0: " ", 1: "│", 2: "│", 3: "│", 4: "─", 5: "┘", 6: "┐", 7: "┤",
    8: "─", 9: "└", 10: "┌", 11: "├", 12: "─", 13: "┴", 14: "┬", 15: "┼",
}
_GLYPHS_ASCII = {
    0: " ", 1: "|", 2: "|", 3: "|", 4: "-", 5: "+", 6: "+", 7: "+",
    8: "-", 9: "+", 10: "+", 11: "+", 12: "-", 13: "+", 14: "+", 15: "+",
}


def _column(lane: int, columns: int) -> int:
    return columns - 2 - 2 * lane


def _paint(pairs, columns: int) -> list[int]:
    mask = [0] * columns
    for a, b in pairs:
        ca, cb = _column(a, columns), _column(b, columns)
        if ca == cb:
            mask[ca] |= _UP | _DOWN
            continue
        lo, hi = (ca, cb) if cb > ca else (cb, ca)
        if cb > ca:
            mask[ca] |= _UP | _RIGHT
            mask[cb] |= _DOWN | _LEFT
        else:
            mask[ca] |= _UP | _LEFT
            mask[cb] |= _DOWN | _RIGHT
        for c in range(lo + 1, hi):
            mask[c] |= _LEFT | _RIGHT
    return mask


def render_text(
    lay: Layout,
    style: Mapping[Any, Style] | None = None,
    *,
    labels: Mapping[str, Label] | None = None,
    unicode: bool = True,
    color: bool = False,
    colour: Colouring | None = None,
) -> str:
    if lay.height == 0:
        return ""
    style = style or {}
    colour = colour or _NO_COLOUR
    lab = _labels_for(lay, labels)
    glyphs = _GLYPHS if unicode else _GLYPHS_ASCII
    columns = 2 * lay.width
    label_col = columns + 1
    arrow = "↺" if unicode else "^"

    back_from: dict[str, list[str]] = {}
    for e in lay.edges:
        if e.back:
            back_from.setdefault(e.src, []).append(e.dst)

    out: list[str] = []
    for node in lay.nodes:
        st = style.get(node.kind, _DEFAULT_STYLE)
        mask = _paint([(c, c) for c in sorted(lay.crossing_lanes(node.row))], columns)
        cells = [glyphs[m] for m in mask]
        cells[_column(node.lane, columns)] = st.marker if unicode else st.ascii_marker
        line = "".join(cells) + " " * (label_col - columns)
        escape_ = _ansi(colour.nodes.get(node.name)) or st.ansi
        if color and escape_:
            c = _column(node.lane, columns)
            line = f"{line[:c]}{escape_}{line[c]}\033[0m{line[c + 1:]}"
        label = lab[node.name].full
        if node.name in back_from:
            label += "  " + " ".join(
                f"{arrow} {lab[d].full}" for d in sorted(back_from[node.name])
            )
        out.append((line + label).rstrip())

        if node.row == lay.height - 1:
            continue
        for connector in _connectors(lay, node.row, columns):
            out.append("".join(glyphs[m] for m in connector).rstrip())
    return "\n".join(out) + "\n"


def _ansi(hex_colour: str | None) -> str:
    if not hex_colour:
        return ""
    h = hex_colour.lstrip("#")
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return f"\033[38;2;{r};{g};{b}m"


def _connectors(lay: Layout, row: int, columns: int) -> list[list[int]]:
    segments = []
    for e in lay.gap_edges(row):
        src, dst = lay[e.src], lay[e.dst]
        top = src.lane if src.row == row else e.lane
        bottom = dst.lane if dst.row == row + 1 else e.lane
        segments.append((top, e.lane, bottom))

    fan_out = any(top != mid for top, mid, _ in segments)
    fan_in = any(mid != bottom for _, mid, bottom in segments)
    if fan_out and fan_in:
        return [
            _paint([(top, mid) for top, mid, _ in segments], columns),
            _paint([(mid, bottom) for _, mid, bottom in segments], columns),
        ]
    if fan_out:
        return [_paint([(top, mid) for top, mid, _ in segments], columns)]
    if fan_in:
        return [_paint([(mid, bottom) for _, mid, bottom in segments], columns)]

    here, below = lay.nodes[row].lane, lay.nodes[row + 1].lane
    if here == below and here not in {mid for _, mid, _ in segments}:
        return [_paint([(mid, mid) for _, mid, _ in segments], columns)]
    return []


@dataclass(frozen=True)
class _Grid:
    lane_x: tuple[float, ...]
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

    def x(self, lane: float) -> float:
        return self.lane_x[int(lane)]

    def y(self, row: float) -> float:
        if not self.rows_y:
            return self.margin + (row + 0.5) * self.row_pitch
        lo = int(row)
        frac = row - lo
        top = self.rows_y[min(lo, len(self.rows_y) - 1)]
        return top + frac * (self.rows_y[min(lo + 1, len(self.rows_y) - 1)] - top)

    def gap(self, row: float) -> float:
        if not self.rows_y:
            return self.row_pitch
        lo = int(row)
        top = self.rows_y[min(lo, len(self.rows_y) - 1)]
        return self.rows_y[min(lo + 1, len(self.rows_y) - 1)] - top or self.row_pitch


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
) -> tuple[_Grid, dict[str, _Drawn]]:
    lab = _labels_for(lay, labels)
    lanes = max(lay.width, min_lanes)
    char_w = font_size * 0.58
    marker_d = 0.82 * font_size
    lane_pitch = 1.30 * font_size
    label_pad = 0.55 * font_size

    drawn: dict[str, _Drawn] = {}
    for n in lay.nodes:
        L = lab[n.name]
        ns, ns_cut = _clip(L.namespace, 2 * max_chars)
        name, name_cut = _clip(L.name, max_chars)
        drawn[n.name] = _Drawn(
            namespace=ns,
            name=name,
            full=L.full,
            width=char_w * max(len(name), len(ns) / 2),
            truncated=ns_cut or name_cut,
        )

    margin = 1.5 * font_size
    row_pitch = max(2.4 * font_size, (lane_pitch + marker_d) / (1 - 2 * BAND))
    if mode is LabelMode.BESIDE:
        col_w = [lane_pitch] * lanes
        for n in lay.nodes:
            need = marker_d + label_pad + drawn[n.name].width
            col_w[n.lane] = max(col_w[n.lane], need)
        lane_x = [0.0] * lanes
        label_x = [0.0] * lanes
        cursor = margin
        for lane in range(lanes - 1, -1, -1):
            lane_x[lane] = cursor + col_w[lane] - marker_d / 2
            label_x[lane] = lane_x[lane] - marker_d / 2 - label_pad
            cursor += col_w[lane]
        anchor = "end"
        width = cursor + margin
    else:
        lane_x = [
            margin + marker_d / 2 + (lanes - 1 - i) * lane_pitch
            for i in range(lanes)
        ]
        column = margin + marker_d + (lanes - 1) * lane_pitch + label_pad
        label_x = [column] * lanes
        anchor = "start"
        width = column + max((d.width for d in drawn.values()), default=0.0) + margin

    return (
        _Grid(
            lane_x=tuple(lane_x),
            label_x=tuple(label_x),
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
        ),
        drawn,
    )


def _jog_roles(lay: Layout, edge) -> list[int]:
    roles = [0] * len(edge.points)
    for i, ((row, lane), (next_row, next_lane)) in enumerate(
        zip(edge.points, edge.points[1:])
    ):
        if row != next_row:
            continue
        roles[i] = roles[i + 1] = -1 if next_lane > lane else 1
    return roles


def _pixel_path(
    lay: Layout,
    edge,
    g: _Grid,
    style: Mapping[Any, Style] | None = None,
) -> list[tuple[float, float]]:
    style = style or {}
    roles = _jog_roles(lay, edge)
    points = [
        (g.x(lane), g.y(row) + roles[i] * BAND * g.gap(row))
        for i, (row, lane) in enumerate(edge.points)
    ]
    top = style.get(lay[edge.src].kind, _DEFAULT_STYLE)
    bot = style.get(lay[edge.dst].kind, _DEFAULT_STYLE)
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
    lane: int
    cx: float
    cy: float
    label_x: float
    marker_w: float
    marker_h: float
    namespace: str
    label: str
    full: str
    truncated: bool


@dataclass(frozen=True)
class EdgeGeometry:
    src: str
    dst: str
    back: bool
    d: str


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


def geometry(
    lay: Layout,
    style: Mapping[Any, Style] | None = None,
    *,
    labels: Mapping[str, Label] | None = None,
    label_mode: LabelMode = LabelMode.COLUMN,
    max_label_chars: int = DEFAULT_LABEL_CHARS,
    font_size: float = 13.0,
    min_lanes: int = 0,
    rows_y: Sequence[float] = (),
) -> Geometry:
    style = style or {}
    g, drawn = _grid(
        lay, font_size=font_size, labels=labels,
        mode=label_mode, max_chars=max_label_chars,
        min_lanes=min_lanes, rows_y=rows_y,
    )
    nodes = []
    for node in lay.nodes:
        st = style.get(node.kind, _DEFAULT_STYLE)
        d = drawn[node.name]
        mw, mh = marker_size(st, g.marker_d)
        nodes.append(NodeGeometry(
            name=node.name, kind=node.kind, row=node.row, lane=node.lane,
            cx=g.x(node.lane), cy=g.y(node.row), label_x=g.label_x[node.lane],
            marker_w=mw, marker_h=mh,
            namespace=d.namespace, label=d.name, full=d.full, truncated=d.truncated,
        ))
    edges = [
        EdgeGeometry(
            src=e.src, dst=e.dst, back=e.back,
            d="" if e.back else _svg_path(*_pixel_path(lay, e, g, style)),
        )
        for e in lay.edges
    ]
    return Geometry(
        width=g.width, height=g.height, font_size=g.font_size,
        marker_d=g.marker_d, row_pitch=g.row_pitch, lane_pitch=g.lane_pitch,
        anchor=g.anchor, nodes=tuple(nodes), edges=tuple(edges),
    )


def render_svg(
    lay: Layout,
    style: Mapping[Any, Style] | None = None,
    *,
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
        lay, style, labels=labels, label_mode=label_mode,
        max_label_chars=max_label_chars, font_size=font_size,
    )
    parts = [
        '<?xml version="1.0" encoding="UTF-8" standalone="no"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink"'
        f' width="{g.width:.0f}" height="{g.height:.0f}"'
        f' viewBox="0 0 {g.width:.0f} {g.height:.0f}">',
        *([f'<rect width="{g.width:.0f}" height="{g.height:.0f}" fill="{plate.background}"/>']
          if plate.paint_background else []),
        f'<g fill="none" stroke="{plate.edge}" stroke-width="1.4"'
        ' stroke-linejoin="round" stroke-linecap="round">',
    ]
    for e in g.edges:
        if e.back:
            continue
        hue = colour.edges.get((e.src, e.dst))
        stroke = f' stroke="{hue}"' if hue else ""
        parts.append(f'<path d="{e.d}"{stroke}/>')
    parts.append("</g>")

    for n in g.nodes:
        st = style.get(n.kind, _DEFAULT_STYLE)
        cx, cy, lx = n.cx, n.cy, n.label_x
        parts.append(f'<g><title>{escape(n.full)}</title>')
        parts.append(_svg_marker(st, cx, cy, g.marker_d, colour.nodes.get(n.name)))
        if n.namespace:
            parts.append(
                f'<text x="{lx:.1f}" y="{cy - 0.42 * font_size:.1f}"'
                f' font-family="{escape(font)}" font-size="{font_size / 2:.1f}"'
                f' fill="{st.muted}" text-anchor="{g.anchor}">{escape(n.namespace)}</text>'
            )
        parts.append(
            f'<text x="{lx:.1f}" y="{cy + 0.36 * font_size:.1f}"'
            f' font-family="{escape(font)}" font-size="{font_size:.0f}" fill="{st.text}"'
            f' text-anchor="{g.anchor}">{escape(n.label)}</text></g>'
        )
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


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
    g, drawn = _grid(
        lay, font_size=font_size, labels=labels,
        mode=label_mode, max_chars=max_label_chars,
    )
    lines = [
        "digraph G {",
        f'graph [fontname="{font}", outputorder="edgesfirst",'
        f' bgcolor="{plate.background if plate.paint_background else "transparent"}"];',
        f'node  [fontname="{font}", fontsize={font_size:.0f}, fixedsize=true];',
        f'edge  [fontname="{font}", color="{plate.edge}", dir="none"];',
    ]
    prefix = _label_prefix(lay)
    for node in lay.nodes:
        st = style.get(node.kind, _DEFAULT_STYLE)
        d = drawn[node.name]
        x, y = g.x(node.lane), g.height - g.y(node.row)
        w, h = marker_size(st, g.marker_d)
        fill, stroke = tint(st, colour.nodes.get(node.name))
        lines.append(
            f'  "{node.name}" [pos="{x:.1f},{y:.1f}!", label="",'
            f' width={w / 72:.3f}, height={h / 72:.3f},'
            f' penwidth={st.stroke_width:g},'
            f' shape="{st.shape}", style="{st.gv_style}",'
            f' fillcolor="{fill}", color="{stroke}"'
            f'{", " + st.gv_attrs if st.gv_attrs else ""}];'
        )
        box = max(d.width, 1.0)
        half = box / 2 if g.anchor == "start" else -box / 2
        lines.append(
            f'  "{prefix}{node.name}" [pos="{g.label_x[node.lane] + half:.1f},'
            f'{y:.1f}!", shape="plaintext", style="", width={box / 72:.3f},'
            f' height={2.0 * font_size / 72:.3f},'
            f" label=<{_html_label(d, st, font_size, box, g.anchor)}>];"
        )
    for e in lay.edges:
        if e.back:
            lines.append(f'  "{e.src}" -> "{e.dst}" [style="dashed"];')
            continue
        pts = [
            (x, g.height - y) for x, y in _flatten(*_pixel_path(lay, e, g, style))
        ]
        hue = colour.edges.get((e.src, e.dst))
        tone = f', color="{hue}"' if hue else ""
        lines.append(f'  "{e.src}" -> "{e.dst}" [pos="{_spline(pts)}"{tone}];')
    lines.append("}")
    return "\n".join(lines)


def _label_prefix(lay: Layout) -> str:
    prefix = "__label__"
    while any(n.name.startswith(prefix) for n in lay.nodes):
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
    lines.append(
        f'<FONT POINT-SIZE="{font_size:.1f}" COLOR="{st.text}">'
        f'{escape(d.name)}</FONT><BR ALIGN="{side}"/>'
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
