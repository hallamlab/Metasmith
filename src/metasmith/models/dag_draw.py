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
from math import atan2, cos, pi, sin
from pathlib import Path
from typing import Any, Mapping
from xml.sax.saxutils import escape

from .dag_colour import Colouring
from .dag_layout import Layout

__all__ = [
    "Style", "Plate", "Label", "LabelMode", "default_label", "dot_escape",
    "marker_size", "tint", "render_text", "render_svg", "raster_dot",
    "render_raster", "geometry", "Geometry", "NodeGeometry", "EdgeGeometry",
]

_NO_COLOUR = Colouring()

DEFAULT_LABEL_CHARS = 32  # bound on the drawn name line; the rest is on hover
BAND = 0.10  # of a row pitch: how far apart the two directions of travel sit


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
    svg_shape: str = "circle"  # "circle" | "square" | "triangle_down"
    marker_scale: float = 1.0  # *width*, in units of the grid's marker diameter
    stroke_width: float = 1.6
    # is this marker drawn *filled*? Which is a different question from its
    # shape, and the two were one thing while the only solid marker was also
    # the only square. It decides where a colour scheme's hue lands: on the
    # fill of a solid marker, on the outline of a hollow one.
    solid: bool = False


@dataclass(frozen=True)
class Plate:
    """The surface a drawing sits on — the two colours this module invents.

    Every other colour a backend paints arrives on a `Style` or a `Colouring`;
    these two had nowhere to come from and were literals. The defaults are the
    light plate, so a caller that passes nothing gets the drawing this module
    has always made. A plate is deliberately kind-agnostic: `dag_draw` does not
    know what a node kind means, which is why the per-kind half of a theme
    lives beside `STYLES` in `dag_renderer` instead.
    """
    background: str = "#FFFFFF"
    edge: str = "#666666"
    # on by default -- every artifact already on disk is painted, and a caller
    # embedding the drawing on its own ground (a GUI card, a themed page) is
    # the one that asks for the other. Off skips the SVG `<rect>` outright
    # rather than making it `fill="none"` on top of one, and raster's `bgcolor`
    # becomes graphviz's own `"transparent"` keyword -- so a PNG stays exactly
    # as opaque or as see-through as an SVG viewer would render the SVG.
    paint_background: bool = True


_DEFAULT_PLATE = Plate()


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

# an equilateral triangle's height, as a fraction of its width
_TRIANGLE_H = 0.8660254037844386


def marker_size(st: Style, marker_d: float) -> tuple[float, float]:
    """One marker's drawn (width, height) in pixels.

    `marker_scale` is a *width*, so the three shapes are one family: a
    triangle, a circle and a square with the same scale occupy the same
    horizontal space. Height follows the shape — only the triangle differs,
    at 0.866 of its width, because an equilateral one drawn as tall as it is
    wide stops looking equilateral.
    """
    w = marker_d * st.marker_scale
    return w, (w * _TRIANGLE_H if st.svg_shape == "triangle_down" else w)


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


def _column(lane: int, columns: int) -> int:
    """Screen column of a lane. Lane 0 is drawn *rightmost*, next to the label
    column, because that is where most of the nodes are — see `_grid`."""
    return columns - 2 - 2 * lane


def _paint(pairs, columns: int) -> list[int]:
    """OR line segments into a per-column bitmask.

    Each pair is (lane entered from above, lane left towards below). Merging as
    a mask rather than writing characters is what makes a rail crossing a jog
    come out as a cross instead of clobbering one of the two.

    Lanes are mirrored into columns before anything else, so the `_LEFT` and
    `_RIGHT` bits below are already in screen terms.
    """
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
    """One row per node: rails, marker, then the label in a single column.

    There is no `label_mode` here and no truncation. A terminal has one font
    size, so the half-size namespace line has no representation and the label
    stays the one-line `full` form; and the point of bounding a label is
    reclaiming horizontal pixels, which a text dump does not pay for. Per-lane
    label columns would need variable-width lanes, which the fixed two-column
    mask painter cannot express.

    A colour scheme reaches the marker as a 24-bit escape and outranks the
    style's own `ansi`, but only when `color` is on: anything written to a file
    or a pipe stays exactly the plain text the golden tests pin.
    """
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
    """A `#rrggbb` as a 24-bit foreground escape; "" when there is no colour."""
    if not hex_colour:
        return ""
    h = hex_colour.lstrip("#")
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return f"\033[38;2;{r};{g};{b}m"


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
    label_x: tuple[float, ...]  # where the label starts, per lane
    anchor: str  # "start" or "end" — which end of the label `label_x` pins
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
class _Arc:
    """A quarter circle standing in for a right-angle corner."""
    radius: float
    sweep: int  # svg sweep-flag; 1 is clockwise on screen, where y grows down
    centre: tuple[float, float]


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

    Lanes run right to left, so lane 0 — where most of a plan's nodes sit,
    because every chain that inherits its parent's lane stays in it — is the one
    next to the labels. Drawn the other way the busiest lane is the furthest
    thing on the page from the names of what is in it.

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
    # sized by the two-line label block, not by a text box. A corner arc needs a
    # vertical leg half a lane long at each end of a jog or it comes out as a
    # smaller radius with a flat stub between; the bands eat into that leg from
    # both sides, so the row has to be tall enough to give it back
    row_pitch = max(2.4 * font_size, (lane_pitch + marker_d) / (1 - 2 * BAND))
    if mode is LabelMode.BESIDE:
        # each lane is its own column of [label][marker], laid out from the
        # highest lane on the left down to lane 0 on the right; the label sits
        # left of its marker and is right-aligned against it
        col_w = [lane_pitch] * lay.width
        for n in lay.nodes:
            need = marker_d + label_pad + drawn[n.name].width
            col_w[n.lane] = max(col_w[n.lane], need)
        lane_x = [0.0] * lay.width
        label_x = [0.0] * lay.width
        cursor = margin
        for lane in range(lay.width - 1, -1, -1):
            lane_x[lane] = cursor + col_w[lane] - marker_d / 2
            label_x[lane] = lane_x[lane] - marker_d / 2 - label_pad
            cursor += col_w[lane]
        anchor = "end"
        width = cursor + margin
    else:
        lane_x = [
            margin + marker_d / 2 + (lay.width - 1 - i) * lane_pitch
            for i in range(lay.width)
        ]
        column = margin + marker_d + (lay.width - 1) * lane_pitch + label_pad
        label_x = [column] * lay.width
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
            height=2 * margin + lay.height * row_pitch,
        ),
        drawn,
    )


def _jog_roles(lay: Layout, edge) -> list[int]:
    """Which band each point of `edge.points` belongs in: -1 up, +1 down, 0 none.

    A jog gets its band from what it is *doing*, not from which half-row it
    happens to sit on. `_polyline` emits the departure pair only when the rail
    lane differs from the source's and the arrival pair only when it differs
    from the target's, so the pairs can be identified by index; a departure
    hugs the row it left and an arrival hugs the row it feeds.

    The one case the row alone gets wrong is an edge between adjacent rows,
    where `src + 0.5` and `dst - 0.5` are the same number. Both jogs live in
    the one gap there, and reading it positionally makes every one of them a
    departure — which is why a fan-in arriving from one lane over used to read
    as though it were leaving the node above it.
    """
    src, dst = lay[edge.src].row, lay[edge.dst].row
    has_dep = edge.lane != lay[edge.src].lane
    has_arr = edge.lane != lay[edge.dst].lane
    roles = [0] * len(edge.points)
    i = 1
    if has_dep:
        # with no arrival pair the rail *is* the target's lane, so on adjacent
        # rows this single jog is the arrival and belongs under, not over
        roles[1] = roles[2] = 1 if (not has_arr and dst - src == 1) else -1
        i = 3
    if has_arr:
        roles[i] = roles[i + 1] = 1
    return roles


def _pixel_path(
    lay: Layout,
    edge,
    g: _Grid,
    style: Mapping[Any, Style] | None = None,
) -> list[tuple[float, float]]:
    """The edge in pixels, with its two jogs pulled into separate bands.

    Both a jog leaving a node at one row and a jog merging into the node at the
    next land on the same half-row, so with a single band they overlay each
    other and there is no telling which way either is going. Splitting them
    gives the gap a direction: the outgoing band sits just under the row that
    emitted it, the merging band just over the row it feeds.

    That way round and not the other: it is the order the character grid already
    draws (fan-out row, then fan-in row), and it is the one where the two bands
    do not have to cross to reach each other.

    Both ends are trimmed by the marker's own half-height, not by one radius
    for all three shapes — a triangle is shorter than it is wide, so a fixed
    trim leaves a gap under it and overshoots into a square.
    """
    style = style or {}
    offset = BAND * g.row_pitch
    roles = _jog_roles(lay, edge)
    points = [
        (g.x(lane), g.y(row) + roles[i] * offset)
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
    """Trim each right-angle corner back by `bevel` along both of its legs.

    Returns the trimmed polyline, and an `_Arc` for each segment that replaced a
    corner. For a right angle the tangent length and the radius are the same
    number, so `bevel` is both, and the two trimmed points and the corner they
    replaced are three corners of a square — which is why the centre is just
    `start + end - corner`. Keeping one description for all three backends is
    the point: the SVG turns a marked segment into an `A`, and the raster path
    subdivides it.

    This is a pixel-space treatment, deliberately not a change to the routed
    corridor: `_polyline` stays axis-aligned, and trimming a corner can only
    move points inside the right angle it replaces, so the guarantee that a rail
    never crosses a node cell survives untouched.

    Each corner asks for `bevel`, then any leg whose two corners together want
    more than its length shares it out between them. Splitting per leg rather
    than capping each corner at half a leg matters at the ends: a leg running
    from a marker to the first corner is consumed by one corner, not two, so
    halving it there would shrink the cut for nothing.

    With `bevel` at half a lane, a one-lane jog's two arcs meet exactly in the
    middle and the horizontal run between them disappears — one clean S — while
    a wider jog keeps a flat run between two fixed quarter-circles.
    """
    pts: list[tuple[float, float]] = []
    for p in points:
        # an edge spanning adjacent rows can route both of its jogs at the same
        # half-row, emitting a repeated point; a zero-length leg has no
        # direction to trim along
        if not pts or p != pts[-1]:
            pts.append(p)
    if len(pts) < 3:
        return pts, {}

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
    arcs: dict[int, _Arc] = {}
    for i in range(1, len(pts) - 1):
        (ax, ay), (bx, by), (cx, cy) = pts[i - 1], pts[i], pts[i + 1]
        d, la, lc = cut[i], legs[i - 1], legs[i]
        start = (bx + (ax - bx) * d / la, by + (ay - by) * d / la)
        end = (bx + (cx - bx) * d / lc, by + (cy - by) * d / lc)
        if start != out[-1]:  # already there when the flat run between two
            out.append(start)  # corners has collapsed to nothing
        if end == out[-1]:
            continue
        # y grows downward, so an SVG sweep of 1 is a clockwise turn on screen
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
    """The same path as a plain polyline, with each arc subdivided.

    For the raster backend, which hands graphviz a point list. One chord per
    quarter circle would draw the chamfer this used to be, and the three
    backends are supposed to agree.
    """
    out = [points[0]]
    for i, end in enumerate(points[1:]):
        arc = arcs.get(i)
        if arc is None:
            out.append(end)
            continue
        (cx, cy), start = arc.centre, points[i]
        a0 = atan2(start[1] - cy, start[0] - cx)
        a1 = atan2(end[1] - cy, end[0] - cx)
        # always the short way round: a trimmed right angle is a quarter circle
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
    """One node's placement, in pixels, with the text as it will be drawn."""
    name: str
    kind: Any
    row: int
    lane: int
    cx: float
    cy: float
    label_x: float
    marker_w: float
    marker_h: float
    namespace: str  # the half-size line, already clipped
    label: str  # the name line, already clipped
    full: str  # untruncated, for a tooltip
    truncated: bool


@dataclass(frozen=True)
class EdgeGeometry:
    """One edge as an SVG path, already routed, jogged and corner-rounded."""
    src: str
    dst: str
    back: bool
    d: str


@dataclass(frozen=True)
class Geometry:
    """Everything a backend needs to draw a `Layout`, and nothing about colour.

    Extracted from `render_svg` so a second consumer can draw the same placement
    with its own materials — the GUI's info panel replaces markers and text with
    clickable buttons over the same edge layer. `render_svg` is written in terms
    of this, which is what stops the two from drifting.
    """
    width: float
    height: float
    font_size: float
    marker_d: float
    row_pitch: float
    lane_pitch: float
    anchor: str  # "start" or "end" — which end of the label `label_x` pins
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
) -> Geometry:
    style = style or {}
    g, drawn = _grid(
        lay, font_size=font_size, labels=labels,
        mode=label_mode, max_chars=max_label_chars,
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
        # no arrowheads: every edge runs down the page, so a head at the end of
        # each of a hundred of them says only what the geometry already does
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
    """One `path`, with the trimmed corners drawn as quarter circles."""
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
    """A marker's (fill, stroke) once a colour scheme has had its say.

    For a hollow marker the hue goes on the outline, since the fill has
    nothing of its own to say. The target is the one marker drawn solid —
    there the hue goes on the fill instead, and the outline stays the style's
    own, so a requested output still reads "solid" rather than "outlined"
    once colour is on.
    """
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
        # equilateral on its point, centred on the cell so a rail entering from
        # above meets the flat side and one leaving meets the tip
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
    colour = colour or _NO_COLOUR
    plate = plate or _DEFAULT_PLATE
    g, drawn = _grid(
        lay, font_size=font_size, labels=labels,
        mode=label_mode, max_chars=max_label_chars,
    )
    lines = [
        "digraph G {",
        # bgcolor is pinned rather than left to graphviz: unset, a PNG's ground
        # is whatever the local build defaults to, which is the one way a
        # raster preview can disagree with the SVG about what it is drawn on
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
        # the same two numbers the SVG draws from, and the same stroke weight:
        # a global penwidth pinned here is how the PNG used to disagree with
        # the SVG about how heavy an outline was
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
    """A node-id prefix no caller id starts with: graphviz unifies nodes by
    name, so a collision would replace a real marker with a label."""
    prefix = "__label__"
    while any(n.name.startswith(prefix) for n in lay.nodes):
        prefix = "_" + prefix
    return prefix


def _html_label(
    d: _Drawn, st: Style, font_size: float, width: float, anchor: str
) -> str:
    """The two-line block, as graphviz HTML-like markup.

    `<BR ALIGN=…/>` justifies a line within the label block, but the block
    itself is centred in its node — so two labels of different lengths pinned
    at the same x would start at different x. A fixed-width table cell pins the
    block, and then the lines inside it, to one edge: the left one where labels
    run rightwards from a shared column, the right one where each label is
    tucked against the marker on its right.
    """
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
    """Encode a polyline as the B-spline `pos` graphviz expects.

    The format is a start point followed by (control, control, end) triplets;
    putting the controls on the straight line between the endpoints turns each
    cubic back into the segment we routed. The leading `e,` field is where an
    arrowhead would go; edges are drawn `dir="none"` so it only has to be the
    real endpoint, and the path runs all the way to it.
    """

    def _fmt(p):
        return f"{p[0]:.1f},{p[1]:.1f}"

    end = points[-1]
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
