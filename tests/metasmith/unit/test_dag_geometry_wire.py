from __future__ import annotations

import pytest

from metasmith.ops.workflow import GEOMETRY_VERSION, dag_geometry

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


def _rows(geo: dict) -> list[str]:
    return [n["id"] for n in sorted(geo["nodes"], key=lambda n: n["row"])]


def _nominal(geo: dict, margin: float = 1.5 * 13.0) -> dict[str, float]:
    return {
        n["id"]: margin + (n["row"] + 0.5) * geo["row_pitch"] for n in geo["nodes"]
    }


def test_an_edge_is_a_path_and_nothing_else(geo):
    assert "lane_x" not in geo and "margin" not in geo
    for e in geo["edges"]:
        assert set(e) <= {"from", "to", "back", "d", "hue"}
        assert e["back"] or e["d"]


def test_the_payload_says_which_shape_it_is(geo):
    assert geo["v"] == GEOMETRY_VERSION


FORK = [{"id": f"#{i}", "kind": "data"} for i in "abcd"]
FORK_EDGES = [{"from": "#a", "to": "#d"}, {"from": "#b", "to": "#d"}]


def test_a_caller_can_fix_the_rows_it_already_has():
    own = _rows(dag_geometry(FORK, FORK_EDGES))
    mine = ["#c", "#b", "#a", "#d"]
    assert mine != own
    assert _rows(dag_geometry(FORK, FORK_EDGES, order=mine)) == mine

    for bad in (["#d", "#a", "#b", "#c"], mine[:-1], mine + ["#e"]):
        assert _rows(dag_geometry(FORK, FORK_EDGES, order=bad)) == own


def test_measured_rows_are_where_the_drawing_lands(geo):
    order = _rows(geo)
    same = dag_geometry(NODES, EDGES, order=order, row_y=_nominal(geo))
    for a, b in zip(geo["nodes"], same["nodes"]):
        assert a["cy"] == pytest.approx(b["cy"])
    assert [e["d"] for e in same["edges"]] == [e["d"] for e in geo["edges"]]

    stretched = {k: v * 2 for k, v in _nominal(geo).items()}
    moved = dag_geometry(NODES, EDGES, order=order, row_y=stretched)
    by_id = {n["id"]: n for n in moved["nodes"]}
    for nid, y in stretched.items():
        assert by_id[nid]["cy"] == pytest.approx(y)
    assert [e["d"] for e in moved["edges"]] != [e["d"] for e in geo["edges"]]
    assert moved["height"] > geo["height"]


def test_an_incomplete_row_map_is_not_half_applied(geo):
    partial = _nominal(geo)
    partial.pop(next(iter(partial)))
    fell_back = dag_geometry(NODES, EDGES, order=_rows(geo), row_y=partial)
    assert [n["cy"] for n in fell_back["nodes"]] == [n["cy"] for n in geo["nodes"]]


def test_a_lane_floor_moves_the_gutter_and_not_the_shape(geo):
    wide = dag_geometry(NODES, EDGES, min_lanes=6)
    assert wide["width"] > geo["width"]
    shift = wide["nodes"][0]["cx"] - geo["nodes"][0]["cx"]
    assert shift > 0
    for a, b in zip(geo["nodes"], wide["nodes"]):
        assert a["lane"] == b["lane"] and a["row"] == b["row"]
        assert b["cx"] - a["cx"] == pytest.approx(shift)


def test_a_marker_shape_reaches_the_wire(geo):
    kinds = {n["id"]: n["kind"] for n in geo["nodes"]}
    assert kinds["1 assemble"] == "transform"
    assert kinds["ns::reads"] == "data"
    assert kinds["ns::report"] == "target"
    by_id = {n["id"]: n for n in geo["nodes"]}
    assert by_id["1 assemble"]["marker_h"] < by_id["1 assemble"]["marker_w"]
    assert by_id["ns::reads"]["marker_h"] == by_id["ns::reads"]["marker_w"]


def test_colour_is_asked_for_rather_than_assumed(geo):
    assert all("hue" not in n for n in geo["nodes"])
    coloured = dag_geometry(NODES, EDGES, colour="module")
    assert any("hue" in n for n in coloured["nodes"])
