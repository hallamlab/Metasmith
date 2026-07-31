"""What `ops.workflow.dag_geometry` puts on the wire, and whether it is enough.

The GUI draws the same graph three times -- the plan, the info panel, and the
recipe's lineage rails -- and only the first of those can use a baked path: the
rails' rows are not a uniform height, so their y positions are measured off the
DOM and the edges have to be re-baked against them. That re-bake is a port of
`dag_draw`'s last three functions, living in `frontend/src/lib/dagpaths.js`,
where there is no test runner to hold it honest.

So it is held honest here. `_rebake` below is a second, independent
implementation of the port, written against *only* the fields that cross the
wire, and pinned to the path `render_svg` actually emits. It fails if the wire
payload loses a field the port needs, if the port's maths is wrong, and if the
drawing's own maths moves without the wire following.
"""
from __future__ import annotations

import math

import pytest

from metasmith.models.dag_draw import BAND
from metasmith.ops.workflow import dag_geometry

# a fan-out into a side lane, a fan-in back out of it, and an adjacent-row pair
# -- the three shapes `_jog_roles` distinguishes
NODES = [
    {"id": "given", "kind": "transform"},
    {"id": "ns::reads", "kind": "data"},
    {"id": "1 assemble", "kind": "transform"},
    {"id": "ns::contigs", "kind": "data"},
    {"id": "2 annotate", "kind": "transform"},
    {"id": "ns::report", "kind": "target"},
]
EDGES = [
    {"from": "given", "to": "ns::reads"},
    {"from": "ns::reads", "to": "1 assemble"},
    {"from": "1 assemble", "to": "ns::contigs"},
    {"from": "ns::contigs", "to": "2 annotate"},
    {"from": "ns::reads", "to": "2 annotate"},
    {"from": "2 annotate", "to": "ns::report"},
]


@pytest.fixture(scope="module")
def geo() -> dict:
    return dag_geometry(NODES, EDGES)


# -- the port, rewritten from the wire payload alone -------------------------


def _roles(edge, src, dst):
    has_dep = edge["lane"] != src["lane"]
    has_arr = edge["lane"] != dst["lane"]
    roles = [0] * len(edge["points"])
    i = 1
    if has_dep:
        roles[1] = roles[2] = 1 if (not has_arr and dst["row"] - src["row"] == 1) else -1
        i = 3
    if has_arr:
        roles[i] = roles[i + 1] = 1
    return roles


def _round(points, bevel):
    pts = []
    for p in points:
        if not pts or p != pts[-1]:
            pts.append(p)
    if len(pts) < 3:
        return pts, {}
    legs = [math.dist(a, b) for a, b in zip(pts, pts[1:])]
    cut = [0.0] + [bevel] * (len(pts) - 2) + [0.0]
    for i, leg in enumerate(legs):
        want = cut[i] + cut[i + 1]
        if want > leg:
            cut[i] *= leg / want
            cut[i + 1] *= leg / want
    out = [pts[0]]
    arcs = {}
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
        arcs[len(out) - 1] = (d, 1 if turn > 0 else 0)
        out.append(end)
    out.append(pts[-1])
    return out, arcs


def _emit(points, arcs) -> str:
    d = [f"M {points[0][0]:.1f},{points[0][1]:.1f}"]
    for i, (x, y) in enumerate(points[1:]):
        arc = arcs.get(i)
        if arc is None:
            d.append(f"L {x:.1f},{y:.1f}")
        else:
            d.append(f"A {arc[0]:.1f},{arc[0]:.1f} 0 0 {arc[1]} {x:.1f},{y:.1f}")
    return " ".join(d)


def _rebake(geo: dict, edge: dict, *, row_y=None) -> str:
    """The browser's job, from the wire payload and nothing else."""
    by_id = {n["id"]: n for n in geo["nodes"]}
    src, dst = by_id[edge["from"]], by_id[edge["to"]]

    def y(row: float) -> float:
        if row_y is None:
            return geo["margin"] + (row + 0.5) * geo["row_pitch"]
        lo = math.floor(row)
        frac = row - lo
        return row_y[lo] + frac * (row_y[min(lo + 1, len(row_y) - 1)] - row_y[lo])

    def gap(row: float) -> float:
        if row_y is None:
            return geo["row_pitch"]
        lo = math.floor(row)
        return row_y[min(lo + 1, len(row_y) - 1)] - row_y[lo]

    roles = _roles(edge, src, dst)
    pts = [
        (geo["lane_x"][int(lane)], y(row) + roles[i] * BAND * gap(row))
        for i, (row, lane) in enumerate(edge["points"])
    ]
    pts[0] = (pts[0][0], pts[0][1] + src["marker_h"] / 2)
    pts[-1] = (pts[-1][0], pts[-1][1] - dst["marker_h"] / 2)
    return _emit(*_round(pts, geo["lane_pitch"] / 2))


# -- the assertions ----------------------------------------------------------


def test_the_wire_carries_the_grid_the_pixels_were_baked_on(geo):
    assert geo["lane_x"] and geo["margin"] > 0
    assert len(geo["lane_x"]) >= 1
    for e in geo["edges"]:
        if e["back"]:
            continue
        assert e["points"], (e["from"], e["to"])
        assert isinstance(e["lane"], int)
        # grid coordinates, with half-steps at the jogs -- not pixels
        for row, lane in e["points"]:
            assert 0 <= lane < len(geo["lane_x"])
            assert row * 2 == int(row * 2)


def test_every_edge_rebakes_to_the_path_the_drawing_emits(geo):
    """The one guard the browser port has. If this drifts, the rails and the
    plan stop being the same drawing and nothing else says so."""
    drawn = [e for e in geo["edges"] if not e["back"]]
    assert drawn
    for e in drawn:
        assert _rebake(geo, e) == e["d"], (e["from"], e["to"])


def test_a_row_override_moves_the_path_without_changing_its_shape(geo):
    """What the recipe's rails do: the same routing at measured y positions.

    The engine's own row positions must round-trip exactly -- otherwise the
    override is not an override but a second placement -- and a stretched set
    must move every point and still land on the markers.
    """
    nominal = [geo["margin"] + (r + 0.5) * geo["row_pitch"] for r in range(len(geo["nodes"]))]
    e = next(e for e in geo["edges"] if not e["back"] and len(e["points"]) > 2)
    assert _rebake(geo, e, row_y=nominal) == e["d"]

    stretched = [y * 2 for y in nominal]
    moved = _rebake(geo, e, row_y=stretched)
    assert moved != e["d"]
    # it still starts on its source marker's edge and ends on its target's
    by_id = {n["id"]: n for n in geo["nodes"]}
    head = moved.split()[1]
    x, y = (float(v) for v in head.split(","))
    src = by_id[e["from"]]
    # the emitter writes one decimal, so that is the tolerance on reading it back
    assert x == pytest.approx(geo["lane_x"][src["lane"]], abs=0.05)
    assert y == pytest.approx(stretched[src["row"]] + src["marker_h"] / 2, abs=0.05)


def test_a_marker_shape_reaches_the_wire(geo):
    """The three the engine draws, so a client can pick one. `target` used to be
    flattened to `data` here, which is why nothing outside the plan could draw a
    requested output solid."""
    kinds = {n["id"]: n["kind"] for n in geo["nodes"]}
    assert kinds["1 assemble"] == "transform"
    assert kinds["ns::reads"] == "data"
    assert kinds["ns::report"] == "target"
    by_id = {n["id"]: n for n in geo["nodes"]}
    # a triangle is shorter than it is wide; the circles are not
    assert by_id["1 assemble"]["marker_h"] < by_id["1 assemble"]["marker_w"]
    assert by_id["ns::reads"]["marker_h"] == by_id["ns::reads"]["marker_w"]


def test_colour_is_asked_for_rather_than_assumed(geo):
    """Only the plan is coloured by module; the panel and the rails are not, and
    that is expressed by not asking rather than by throwing hues away."""
    assert all("hue" not in n for n in geo["nodes"])
    coloured = dag_geometry(NODES, EDGES, colour="module")
    assert any("hue" in n for n in coloured["nodes"])
