"""Backends that draw a `dag_layout.Layout`.

Three of them, all reading the same (row, lane) grid and the same routed
polylines, so a graph looks the same whichever one you ask for:

- `render_text`  — one row per node, rails on the left, for terminals and logs.
- `render_svg`   — the same grid in pixels, written directly; no graphviz.
- `raster_dot`   — DOT carrying explicit `pos` coordinates, for `neato -n2`,
                   which consumes them and lays nothing out itself.

A node draws as a *marker* with its label beside it, never as a box holding
its own text — which is what keeps lane pitch independent of label length.

Styling is supplied by the caller as a kind -> `Style` map and text as a
node-id -> `Label` map, so this module stays independent both of what a
"transform" or a "data" node means and of how a caller names things.
"""
from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping
from xml.sax.saxutils import escape

from .dag_layout import Layout

__all__ = [
    "Style", "Label", "LabelMode", "default_label", "dot_escape",
    "render_text", "render_svg", "raster_dot", "render_raster",
]

DEFAULT_LABEL_CHARS = 32  # bound on the drawn name line; the rest is on hover


class LabelMode(Enum):
    """Where a node's label goes.

    COLUMN puts every label at one x, clear of the rails, so lane pitch is
    fixed and the drawing's width is (lanes x pitch) + one text column.
    BESIDE puts each label immediately right of its own marker, which reads
    more directly but makes every lane as wide as its widest label.
    """
    COLUMN = "column"
    BESIDE = "beside"


@dataclass(frozen=True)
class Style:
    """How one kind of node is drawn. A node is a marker, never a text box —
    the label lives beside it, so nothing about the label affects the marker."""
    marker: str = "o"  # terminal glyph, unicode
    ascii_marker: str = "o"  # terminal glyph, 7-bit fallback
    fill: str = "#FFFFFF"
    stroke: str = "#555555"
    text: str = "#111111"
    muted: str = "#8A8A8A"  # the half-size namespace line
    rx: int = 3  # svg corner radius
    shape: str = "box"  # graphviz shape, raster path only
    gv_style: str = "filled"  # graphviz style, raster path only
    gv_attrs: str = ""  # extra graphviz attributes, raster path only
    ansi: str = ""  # colour, only used when writing to a tty
    svg_shape: str = "circle"  # "circle" | "square" | "ringed_square"
    marker_scale: float = 1.0  # of the grid's marker diameter
    stroke_width: float = 1.6


@dataclass(frozen=True)
class Label:
    """What is drawn next to a node — never what identifies it.

    Node identity is the key the caller registered; two steps running the same
    transform differ only by a step number that the label is free to drop. Keep
    the two apart or the graph silently loses nodes.

    `namespace` is the de-emphasised half-size line above `name`; `full` is the
    untruncated one-line form kept for the SVG `<title>` (hover) when `name` is
    too long for its column.
    """
    name: str
    namespace: str = ""
    full: str = ""

    def __post_init__(self):
        if not self.full:
            joined = f"{self.namespace}::{self.name}" if self.namespace else self.name
            object.__setattr__(self, "full", joined)


def default_label(node_id: str) -> Label:
    """Split a `namespace::name` id into its two lines; anything else is one."""
    if "::" in node_id:
        ns, name = node_id.split("::", maxsplit=1)
        return Label(name=name, namespace=ns, full=node_id)
    return Label(name=node_id, full=node_id)


_DEFAULT_STYLE = Style()


def _labels_for(lay: Layout, labels: Mapping[str, Label] | None) -> dict[str, Label]:
    labels = labels or {}
    return {n.name: labels.get(n.name) or default_label(n.name) for n in lay.nodes}

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
    labels: Mapping[str, Label] | None = None,
    unicode: bool = True,
    color: bool = False,
) -> str:
    """One row per node: rails, marker, then the label in a single column.

    There is no `label_mode` here and no truncation. A terminal has one font
    size, so the half-size namespace line has no representation and the label
    stays the one-line `full` form; and the point of bounding a label is
    reclaiming horizontal pixels, which a text dump does not pay for. Per-lane
    label columns would need variable-width lanes, which the fixed two-column
    mask painter cannot express.
    """
    if lay.height == 0:
        return ""
    style = style or {}
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
        cells[2 * node.lane] = st.marker if unicode else st.ascii_marker
        line = "".join(cells) + " " * (label_col - columns)
        if color and st.ansi:
            c = 2 * node.lane
            line = f"{line[:c]}{st.ansi}{line[c]}\033[0m{line[c + 1:]}"
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


def _connectors(lay: Layout, row: int, columns: int) -> list[list[int]]:
    """Mask rows for the gap under `row` — none at all when nothing happens.

    A gap needs a fan-out row when edges leave a node sideways into their rail
    and a fan-in row when rails converge on the next node; when both happen the
    gap is two rows tall, because collapsing them would overwrite one with the
    other.

    A gap where every rail runs straight through gets no row at all, and that
    loses nothing: a lane running straight from `row` to `row + 1` either holds
    a marker at one of those rows or is reported by `crossing_lanes` at both,
    so it is drawn at both ends of the gap either way. Paying a row of `│` to
    say so again is most of the drawing's height on a long pipeline.

    One case does need the row back. Two consecutive rows can land in the same
    lane without an edge between them — the second node reusing a lane the
    first one closed, at a component boundary — and dropping the gap there
    would draw two unrelated nodes as a chain.
    """
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

    # every remaining segment runs straight through, so painting them leaves
    # the disputed lane blank — which is the whole point of keeping this row
    here, below = lay.nodes[row].lane, lay.nodes[row + 1].lane
    if here == below and here not in {mid for _, mid, _ in segments}:
        return [_paint([(mid, mid) for _, mid, _ in segments], columns)]
    return []


# --- pixel grid -------------------------------------------------------------


@dataclass(frozen=True)
class _Grid:
    lane_x: tuple[float, ...]  # centre of each lane's rail
    label_x: tuple[float, ...]  # left edge of the label block, per lane
    row_pitch: float
    lane_pitch: float
    marker_d: float
    font_size: float
    margin: float
    width: float
    height: float

    def x(self, lane: float) -> float:
        return self.lane_x[int(lane)]

    def y(self, row: float) -> float:
        return self.margin + (row + 0.5) * self.row_pitch


@dataclass(frozen=True)
class _Drawn:
    """A label as it will actually appear: possibly clipped, and how wide."""
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
) -> tuple[_Grid, dict[str, _Drawn]]:
    """Lane pitch and label placement.

    Lane pitch no longer depends on any label: a node is a marker, so a long
    type name cannot push the branches to its right halfway across the page.
    What the label costs is one text column (COLUMN) or one widening of its own
    lane (BESIDE).

    Text widths are a character-count estimate against a nominal advance, not
    font metrics — good enough to size a column, and the reason the raster
    backend's label x is approximate.
    """
    lab = _labels_for(lay, labels)
    char_w = font_size * 0.58  # Arial-ish advance; only needs to be close
    marker_d = 0.82 * font_size
    lane_pitch = 1.30 * font_size
    label_pad = 0.55 * font_size

    drawn: dict[str, _Drawn] = {}
    for n in lay.nodes:
        L = lab[n.name]
        # the namespace is half size, so it fits twice as many characters in
        # the same column as the name line
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
    # sized by the two-line label block, not by a text box; the chamfer needs
    # a vertical leg at least half a lane long at each end of a jog, which is
    # what this inequality buys
    row_pitch = 2.4 * font_size
    assert row_pitch >= lane_pitch + marker_d, "45 degree corners need the room"
    if mode is LabelMode.BESIDE:
        col_w = [lane_pitch] * lay.width
        for n in lay.nodes:
            need = marker_d + label_pad + drawn[n.name].width
            col_w[n.lane] = max(col_w[n.lane], need)
        lane_x, label_x, cursor = [], [], margin
        for w in col_w:
            lane_x.append(cursor + marker_d / 2)
            label_x.append(cursor + marker_d + label_pad)
            cursor += w
        width = cursor + margin
    else:
        lane_x = [margin + marker_d / 2 + i * lane_pitch for i in range(lay.width)]
        column = margin + marker_d + (lay.width - 1) * lane_pitch + label_pad
        label_x = [column] * lay.width
        width = column + max((d.width for d in drawn.values()), default=0.0) + margin

    return (
        _Grid(
            lane_x=tuple(lane_x),
            label_x=tuple(label_x),
            row_pitch=row_pitch,
            lane_pitch=lane_pitch,
            marker_d=marker_d,
            font_size=font_size,
            margin=margin,
            width=max(width, 2 * margin),
            height=2 * margin + lay.height * row_pitch,
        ),
        drawn,
    )


def _pixel_path(lay: Layout, edge, g: _Grid) -> list[tuple[float, float]]:
    points = [(g.x(lane), g.y(row)) for row, lane in edge.points]
    points[0] = (points[0][0], points[0][1] + g.marker_d / 2)
    points[-1] = (points[-1][0], points[-1][1] - g.marker_d / 2)
    return _chamfer(points, g.lane_pitch / 2)


def _chamfer(
    points: list[tuple[float, float]], bevel: float
) -> list[tuple[float, float]]:
    """Replace each right-angle corner with a 45 degree cut.

    This is a pixel-space treatment, deliberately not a change to the routed
    corridor: `_polyline` stays axis-aligned, and cutting a corner can only
    move points inside the right angle it replaces, so the guarantee that a
    rail never crosses a node cell survives untouched.

    Each corner asks for `bevel`, then any leg whose two corners together want
    more than its length shares it out between them. Splitting per leg rather
    than capping each corner at half a leg matters at the ends: a leg running
    from a marker to the first corner is consumed by one corner, not two, so
    halving it there would shrink the cut for nothing.

    With `bevel` at half a lane, a one-lane jog's two cuts meet exactly in the
    middle and the horizontal run disappears — one clean diagonal, as git draws
    it — while a wider jog keeps a flat run between two fixed diagonals.
    """
    pts: list[tuple[float, float]] = []
    for p in points:
        # an edge spanning adjacent rows can route both of its jogs at the same
        # half-row, emitting a repeated point; a zero-length leg has no
        # direction to bevel along
        if not pts or p != pts[-1]:
            pts.append(p)
    if len(pts) < 3:
        return pts

    legs = [
        ((b[0] - a[0]) ** 2 + (b[1] - a[1]) ** 2) ** 0.5
        for a, b in zip(pts, pts[1:])
    ]
    cut = [0.0] + [bevel] * (len(pts) - 2) + [0.0]  # the ends are not corners
    for i, leg in enumerate(legs):
        want = cut[i] + cut[i + 1]
        if want > leg:
            cut[i] *= leg / want
            cut[i + 1] *= leg / want

    out = [pts[0]]
    for i in range(1, len(pts) - 1):
        (ax, ay), (bx, by), (cx, cy) = pts[i - 1], pts[i], pts[i + 1]
        d, la, lc = cut[i], legs[i - 1], legs[i]
        for p in (
            (bx + (ax - bx) * d / la, by + (ay - by) * d / la),
            (bx + (cx - bx) * d / lc, by + (cy - by) * d / lc),
        ):
            if p != out[-1]:  # the two cuts coincide when a jog fully collapses
                out.append(p)
    out.append(pts[-1])
    return out


def render_svg(
    lay: Layout,
    style: Mapping[Any, Style] | None = None,
    *,
    labels: Mapping[str, Label] | None = None,
    label_mode: LabelMode = LabelMode.COLUMN,
    max_label_chars: int = DEFAULT_LABEL_CHARS,
    font: str = "Arial",
    font_size: float = 13.0,
) -> str:
    style = style or {}
    g, drawn = _grid(
        lay, font_size=font_size, labels=labels,
        mode=label_mode, max_chars=max_label_chars,
    )
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
        d = drawn[node.name]
        cx, cy = g.x(node.lane), g.y(node.row)
        lx = g.label_x[node.lane]
        parts.append(f'<g><title>{escape(d.full)}</title>')
        parts.append(_svg_marker(st, cx, cy, g.marker_d))
        if d.namespace:
            parts.append(
                f'<text x="{lx:.1f}" y="{cy - 0.42 * font_size:.1f}"'
                f' font-family="{escape(font)}" font-size="{font_size / 2:.1f}"'
                f' fill="{st.muted}" text-anchor="start">{escape(d.namespace)}</text>'
            )
        parts.append(
            f'<text x="{lx:.1f}" y="{cy + 0.36 * font_size:.1f}"'
            f' font-family="{escape(font)}" font-size="{font_size:.0f}" fill="{st.text}"'
            f' text-anchor="start">{escape(d.name)}</text></g>'
        )
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


def _svg_marker(st: Style, cx: float, cy: float, marker_d: float) -> str:
    r = marker_d * st.marker_scale / 2
    if st.svg_shape == "circle":
        return (
            f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{r:.1f}"'
            f' fill="{st.fill}" stroke="{st.stroke}" stroke-width="{st.stroke_width}"/>'
        )

    def _square(half: float, fill: str) -> str:
        return (
            f'<rect x="{cx - half:.1f}" y="{cy - half:.1f}" width="{2 * half:.1f}"'
            f' height="{2 * half:.1f}" rx="{st.rx}"'
            f' fill="{fill}" stroke="{st.stroke}" stroke-width="{st.stroke_width}"/>'
        )

    if st.svg_shape == "ringed_square":
        return _square(r * 0.55, st.fill) + _square(r, "none")
    return _square(r, st.fill)


def raster_dot(
    lay: Layout,
    style: Mapping[Any, Style] | None = None,
    *,
    labels: Mapping[str, Label] | None = None,
    label_mode: LabelMode = LabelMode.COLUMN,
    max_label_chars: int = DEFAULT_LABEL_CHARS,
    font: str = "Arial",
    font_size: float = 13.0,
) -> str:
    """DOT with every node pinned by `pos`, for `neato -n2`.

    Coordinates are in points with the y axis flipped, because graphviz counts
    upward from the bottom left and everything else here counts downward from
    the top left.

    Each node becomes two: the marker, and an edge-free `plaintext` node
    carrying the label. That split is forced — `xlabel` cannot be positioned
    and `\\l` cannot change font size per line, so an HTML-like label on a
    separate pinned node is the only way to get the half-size namespace line
    left-aligned above the name. Its x is approximate, because `pos` pins a
    centre and the width is our character estimate; this is the one place the
    three backends stop agreeing, and it only affects raster previews.
    """
    style = style or {}
    g, drawn = _grid(
        lay, font_size=font_size, labels=labels,
        mode=label_mode, max_chars=max_label_chars,
    )
    lines = [
        "digraph G {",
        f'graph [fontname="{font}", outputorder="edgesfirst"];',
        f'node  [fontname="{font}", fontsize={font_size:.0f}, fixedsize=true,'
        " penwidth=1.2];",
        f'edge  [fontname="{font}", color="#666666"];',
    ]
    prefix = _label_prefix(lay)
    for node in lay.nodes:
        st = style.get(node.kind, _DEFAULT_STYLE)
        d = drawn[node.name]
        x, y = g.x(node.lane), g.height - g.y(node.row)
        side = g.marker_d * st.marker_scale
        lines.append(
            f'  "{node.name}" [pos="{x:.1f},{y:.1f}!", label="",'
            f' width={side / 72:.3f}, height={side / 72:.3f},'
            f' shape="{st.shape}", style="{st.gv_style}",'
            f' fillcolor="{st.fill}", color="{st.stroke}"'
            f'{", " + st.gv_attrs if st.gv_attrs else ""}];'
        )
        box = max(d.width, 1.0)
        lines.append(
            f'  "{prefix}{node.name}" [pos="{g.label_x[node.lane] + box / 2:.1f},'
            f'{y:.1f}!", shape="plaintext", style="", width={box / 72:.3f},'
            f' height={2.0 * font_size / 72:.3f},'
            f" label=<{_html_label(d, st, font_size, box)}>];"
        )
    for e in lay.edges:
        if e.back:
            lines.append(f'  "{e.src}" -> "{e.dst}" [style="dashed"];')
            continue
        pts = [(x, g.height - y) for x, y in _pixel_path(lay, e, g)]
        lines.append(f'  "{e.src}" -> "{e.dst}" [pos="{_spline(pts)}"];')
    lines.append("}")
    return "\n".join(lines)


def _label_prefix(lay: Layout) -> str:
    """A node-id prefix no caller id starts with: graphviz unifies nodes by
    name, so a collision would replace a real marker with a label."""
    prefix = "__label__"
    while any(n.name.startswith(prefix) for n in lay.nodes):
        prefix = "_" + prefix
    return prefix


def _html_label(d: _Drawn, st: Style, font_size: float, width: float) -> str:
    """The left-aligned two-line block, as graphviz HTML-like markup.

    `<BR ALIGN="LEFT"/>` justifies a line within the label block, but the block
    itself is centred in its node — so two labels of different lengths pinned
    at the same x would start at different x. A fixed-width table cell pins the
    block, and then the lines inside it, to one left edge.
    """
    lines = []
    if d.namespace:
        lines.append(
            f'<FONT POINT-SIZE="{font_size / 2:.1f}" COLOR="{st.muted}">'
            f'{escape(d.namespace)}</FONT><BR ALIGN="LEFT"/>'
        )
    lines.append(
        f'<FONT POINT-SIZE="{font_size:.1f}" COLOR="{st.text}">'
        f'{escape(d.name)}</FONT><BR ALIGN="LEFT"/>'
    )
    return (
        '<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="0"'
        f' FIXEDSIZE="TRUE" WIDTH="{width:.0f}" HEIGHT="{2.0 * font_size:.0f}">'
        f'<TR><TD ALIGN="LEFT" BALIGN="LEFT">{"".join(lines)}</TD></TR></TABLE>'
    )


def dot_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


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
