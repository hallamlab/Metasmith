"""Backends that draw a `dag_layout.Layout`.

Three of them, all reading the same (row, lane) grid and the same routed
polylines, so a graph looks the same whichever one you ask for:

- `render_text`  — one row per node, rails on the left, for terminals and logs.
- `render_svg`   — the same grid in pixels, written directly; no graphviz.
- `raster_dot`   — DOT carrying explicit `pos` coordinates, for `neato -n2`,
                   which consumes them and lays nothing out itself.

Styling is supplied by the caller as a kind -> `Style` map, so this module
stays independent of what a "transform" or a "data" node means.
"""
from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from xml.sax.saxutils import escape

from .dag_layout import Layout

__all__ = ["Style", "render_text", "render_svg", "raster_dot", "render_raster"]


@dataclass(frozen=True)
class Style:
    marker: str = "o"  # terminal glyph, unicode
    ascii_marker: str = "o"  # terminal glyph, 7-bit fallback
    fill: str = "#FFFFFF"
    stroke: str = "#555555"
    text: str = "#111111"
    rx: int = 3  # svg corner radius
    shape: str = "box"  # graphviz shape, raster path only
    gv_style: str = "filled"  # graphviz style, raster path only
    ansi: str = ""  # colour, only used when writing to a tty


_DEFAULT_STYLE = Style()

# --- character grid ---------------------------------------------------------

_UP, _DOWN, _LEFT, _RIGHT = 1, 2, 4, 8

_GLYPHS = {
    0: " ", 1: "│", 2: "│", 3: "│", 4: "─", 5: "┘", 6: "┐", 7: "┤",
    8: "─", 9: "└", 10: "┌", 11: "├", 12: "─", 13: "┴", 14: "┬", 15: "┼",
}
_GLYPHS_ASCII = {
    0: " ", 1: "|", 2: "|", 3: "|", 4: "-", 5: "+", 6: "+", 7: "+",
    8: "-", 9: "+", 10: "+", 11: "+", 12: "-", 13: "+", 14: "+", 15: "+",
}


def _paint(pairs, columns: int) -> list[int]:
    """OR line segments into a per-column bitmask.

    Each pair is (lane entered from above, lane left towards below). Merging as
    a mask rather than writing characters is what makes a rail crossing a jog
    come out as a cross instead of clobbering one of the two.
    """
    mask = [0] * columns
    for a, b in pairs:
        ca, cb = 2 * a, 2 * b
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
    unicode: bool = True,
    color: bool = False,
) -> str:
    if lay.height == 0:
        return ""
    style = style or {}
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
        cells[2 * node.lane] = st.marker if unicode else st.ascii_marker
        line = "".join(cells) + " " * (label_col - columns)
        if color and st.ansi:
            c = 2 * node.lane
            line = f"{line[:c]}{st.ansi}{line[c]}\033[0m{line[c + 1:]}"
        label = node.name
        if node.name in back_from:
            label += "  " + " ".join(f"{arrow} {d}" for d in sorted(back_from[node.name]))
        out.append((line + label).rstrip())

        if node.row == lay.height - 1:
            continue
        for connector in _connectors(lay, node.row, columns):
            out.append("".join(glyphs[m] for m in connector).rstrip())
    return "\n".join(out) + "\n"


def _connectors(lay: Layout, row: int, columns: int) -> list[list[int]]:
    """Mask rows for the gap under `row`.

    A gap needs a fan-out row when edges leave a node sideways into their rail
    and a fan-in row when rails converge on the next node; when both happen the
    gap is two rows tall, because collapsing them would overwrite one with the
    other.
    """
    segments = []
    for e in lay.gap_edges(row):
        src, dst = lay[e.src], lay[e.dst]
        top = src.lane if src.row == row else e.lane
        bottom = dst.lane if dst.row == row + 1 else e.lane
        segments.append((top, e.lane, bottom))
    if not segments:
        return [[0] * columns]

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
    return [_paint([(mid, mid) for _, mid, _ in segments], columns)]


# --- pixel grid -------------------------------------------------------------


@dataclass(frozen=True)
class _Grid:
    lane_x: tuple[float, ...]  # centre of each lane's column
    row_pitch: float
    box_h: float
    margin: float
    width: float
    height: float

    def x(self, lane: float) -> float:
        return self.lane_x[int(lane)]

    def y(self, row: float) -> float:
        return self.margin + (row + 0.5) * self.row_pitch


def _grid(lay: Layout, *, font_size: float) -> tuple[_Grid, dict[str, float]]:
    """Columns are sized to what is actually in them.

    A single pitch taken from the widest label anywhere pushes the right-hand
    branches an absurd distance away when one node happens to have a long name,
    so each lane gets only the room its own nodes need.
    """
    char_w = font_size * 0.58  # Arial-ish advance; only needs to be close
    box_w = {n.name: 2 * font_size + char_w * len(n.name) for n in lay.nodes}
    minimum, gap = 4 * font_size, 2.0 * font_size
    col_w = [minimum] * lay.width
    for n in lay.nodes:
        col_w[n.lane] = max(col_w[n.lane], box_w[n.name])

    margin = 1.5 * font_size
    lane_x, cursor = [], margin
    for w in col_w:
        lane_x.append(cursor + w / 2)
        cursor += w + gap
    return (
        _Grid(
            lane_x=tuple(lane_x),
            row_pitch=3.6 * font_size,
            box_h=2.1 * font_size,
            margin=margin,
            width=max(cursor - gap + margin, 2 * margin),
            height=2 * margin + lay.height * 3.6 * font_size,
        ),
        box_w,
    )


def _pixel_path(lay: Layout, edge, g: _Grid) -> list[tuple[float, float]]:
    points = [(g.x(lane), g.y(row)) for row, lane in edge.points]
    points[0] = (points[0][0], points[0][1] + g.box_h / 2)
    points[-1] = (points[-1][0], points[-1][1] - g.box_h / 2)
    return points


def render_svg(
    lay: Layout,
    style: Mapping[Any, Style] | None = None,
    *,
    font: str = "Arial",
    font_size: float = 13.0,
) -> str:
    style = style or {}
    g, box_w = _grid(lay, font_size=font_size)
    parts = [
        '<?xml version="1.0" encoding="UTF-8" standalone="no"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink"'
        f' width="{g.width:.0f}" height="{g.height:.0f}"'
        f' viewBox="0 0 {g.width:.0f} {g.height:.0f}">',
        '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5"'
        ' markerWidth="6" markerHeight="6" orient="auto-start-reverse">'
        '<path d="M 0 0 L 10 5 L 0 10 z" fill="#666666"/></marker></defs>',
        f'<rect width="{g.width:.0f}" height="{g.height:.0f}" fill="#FFFFFF"/>',
        '<g fill="none" stroke="#666666" stroke-width="1.4"'
        ' stroke-linejoin="round" stroke-linecap="round">',
    ]
    for e in lay.edges:
        if e.back:
            continue
        pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in _pixel_path(lay, e, g))
        parts.append(f'<polyline points="{pts}" marker-end="url(#arrow)"/>')
    parts.append("</g>")

    for node in lay.nodes:
        st = style.get(node.kind, _DEFAULT_STYLE)
        w = box_w[node.name]
        x, y = g.x(node.lane) - w / 2, g.y(node.row) - g.box_h / 2
        label = escape(node.name)
        parts.append(
            f'<g><title>{label}</title>'
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{g.box_h:.1f}"'
            f' rx="{st.rx}" fill="{st.fill}" stroke="{st.stroke}" stroke-width="1.2"/>'
            f'<text x="{g.x(node.lane):.1f}" y="{g.y(node.row):.1f}"'
            f' font-family="{escape(font)}" font-size="{font_size:.0f}" fill="{st.text}"'
            f' text-anchor="middle" dominant-baseline="central">{label}</text></g>'
        )
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


def raster_dot(
    lay: Layout,
    style: Mapping[Any, Style] | None = None,
    *,
    font: str = "Arial",
    font_size: float = 13.0,
) -> str:
    """DOT with every node pinned by `pos`, for `neato -n2`.

    Coordinates are in points with the y axis flipped, because graphviz counts
    upward from the bottom left and everything else here counts downward from
    the top left.
    """
    style = style or {}
    g, box_w = _grid(lay, font_size=font_size)
    lines = [
        "digraph G {",
        f'graph [fontname="{font}", outputorder="edgesfirst"];',
        f'node  [fontname="{font}", fontsize={font_size:.0f}, fixedsize=true,'
        " penwidth=1.2];",
        f'edge  [fontname="{font}", color="#666666"];',
    ]
    for node in lay.nodes:
        st = style.get(node.kind, _DEFAULT_STYLE)
        x, y = g.x(node.lane), g.height - g.y(node.row)
        lines.append(
            f'  "{node.name}" [pos="{x:.1f},{y:.1f}!",'
            f' width={box_w[node.name] / 72:.3f}, height={g.box_h / 72:.3f},'
            f' shape="{st.shape}", style="{st.gv_style}",'
            f' fillcolor="{st.fill}", color="{st.stroke}"];'
        )
    for e in lay.edges:
        if e.back:
            lines.append(f'  "{e.src}" -> "{e.dst}" [style="dashed"];')
            continue
        pts = [(x, g.height - y) for x, y in _pixel_path(lay, e, g)]
        lines.append(f'  "{e.src}" -> "{e.dst}" [pos="{_spline(pts)}"];')
    lines.append("}")
    return "\n".join(lines)


def _spline(points: list[tuple[float, float]], head: float = 10.0) -> str:
    """Encode a polyline as the B-spline `pos` graphviz expects.

    The format is a start point followed by (control, control, end) triplets;
    putting the controls on the straight line between the endpoints turns each
    cubic back into the segment we routed. The arrow tip goes in the leading
    `e,` field and the path itself stops short of it — graphviz orients the
    head along the gap, so a path ending exactly on the tip gets a head
    pointing nowhere.
    """

    def _fmt(p):
        return f"{p[0]:.1f},{p[1]:.1f}"

    end = points[-1]
    points = list(points)
    (px, py), (ex, ey) = points[-2], end
    span = ((ex - px) ** 2 + (ey - py) ** 2) ** 0.5
    if span > 0:
        t = min(head, span / 2) / span
        points[-1] = (ex - (ex - px) * t, ey - (ey - py) * t)

    out = [f"e,{_fmt(end)}", _fmt(points[0])]
    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        dx, dy = (x1 - x0) / 3, (y1 - y0) / 3
        out += [_fmt((x0 + dx, y0 + dy)), _fmt((x0 + 2 * dx, y0 + 2 * dy)), _fmt((x1, y1))]
    return " ".join(out)


def render_raster(dot: str, out: Path, format: str) -> Path:
    """Hand pre-placed DOT to neato, which honours `pos` and lays nothing out."""
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
