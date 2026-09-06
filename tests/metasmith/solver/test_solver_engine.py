from __future__ import annotations

import math
import stat
import sys

import pytest

from metasmith.models import solver_engine as engine_module
from metasmith.models.solver_backend import Backend, solve_with_engine
from metasmith.models.solver_engine import (
    ENGINE_NAME,
    SOLVER_WIRE_VERSION,
    CallEngine,
    EngineError,
    EngineFor,
    GetEngine,
    ResetEngineCache,
    packaged_engine_path,
    platform_slot,
    probe_engine,
)
from metasmith.models.solver_rng import SOLVER_RNG_VERSION, SOLVER_VALUE_TOLERANCE_ULP
from metasmith.testing.rng_trace import execute_ops, execute_ops_via_engine, generate_ops


@pytest.fixture(autouse=True)
def _isolate_engine_cache():
    ResetEngineCache()
    yield
    ResetEngineCache()


def _fake_engine(tmp_path, body: str, name: str|None=None):
    p = tmp_path/(name or f"{ENGINE_NAME}.{platform_slot()}")
    p.write_text(f"#!{sys.executable}\nimport json, sys\n{body}\n", encoding="utf-8")
    p.chmod(p.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    return p


def _staged(tmp_path, monkeypatch, body: str):
    monkeypatch.setattr(engine_module, "ENGINE_DIR", tmp_path)
    p = _fake_engine(tmp_path, body)
    ResetEngineCache()
    return p


def _handshake(**overrides) -> str:
    reply = dict(
        engine=ENGINE_NAME,
        engine_version="0.1.0",
        wire_version=SOLVER_WIRE_VERSION,
        rng_version=SOLVER_RNG_VERSION,
        capabilities=["rng"],
    )
    reply.update(overrides)
    return (
        f"REPLY = {reply!r}\n"
        "if sys.argv[1] == 'version': print(json.dumps(REPLY))\n"
        "else: print(json.dumps({'echo': json.loads(sys.stdin.read() or '{}')}))\n"
    )


def test_the_platform_slot_uses_the_relays_naming():
    assert platform_slot("x86_64", "Linux") == "x86_64-linux"
    assert platform_slot("aarch64", "Linux") == "arm64-linux"
    assert platform_slot("arm64", "Darwin") == "arm64-darwin"
    assert platform_slot("AMD64", "Linux") == "x86_64-linux"


def test_a_missing_binary_is_the_normal_case(tmp_path, monkeypatch):
    monkeypatch.setattr(engine_module, "ENGINE_DIR", tmp_path)
    assert packaged_engine_path(tmp_path) is None
    assert GetEngine() is None
    assert Backend("rng") == "none"


@pytest.mark.parametrize(
    "overrides,why",
    [
        ({"wire_version": SOLVER_WIRE_VERSION + 1}, "envelope moved"),
        ({"rng_version": SOLVER_RNG_VERSION + 1}, "decision contract moved"),
        ({"engine": "something_else"}, "not our binary"),
    ],
    ids=["wire", "rng", "identity"],
)
def test_a_mismatched_handshake_is_refused(tmp_path, monkeypatch, overrides, why):
    p = _fake_engine(tmp_path, _handshake(**overrides))
    assert probe_engine(p) is None, why


@pytest.mark.parametrize(
    "body,ids",
    [
        ("sys.exit(3)", "nonzero"),
        ("print('not json at all')", "garbage"),
        ("print(json.dumps({'engine': 'msm_solver'}))", "truncated"),
    ],
    ids=["nonzero", "garbage", "truncated"],
)
def test_a_broken_binary_is_refused_rather_than_trusted(tmp_path, body, ids):
    assert probe_engine(_fake_engine(tmp_path, body)) is None


def test_a_capability_it_does_not_advertise_is_not_claimed(tmp_path, monkeypatch):
    _staged(tmp_path, monkeypatch, _handshake())
    assert EngineFor("rng") is not None
    assert EngineFor("solve") is None
    assert Backend("solve") == "none"


def test_a_believed_engine_that_then_fails_raises(tmp_path, monkeypatch):
    body = _handshake() + (
        "\nif sys.argv[1] != 'version':\n"
        "    sys.stderr.write('boom'); sys.exit(1)\n"
    )
    _staged(tmp_path, monkeypatch, body)
    info = GetEngine()
    assert info is not None
    with pytest.raises(EngineError):
        CallEngine(info, "rng-trace", {"seed": 1})


def test_a_believed_engine_round_trips_a_payload(tmp_path, monkeypatch):
    _staged(tmp_path, monkeypatch, _handshake())
    info = GetEngine()
    assert info is not None
    assert CallEngine(info, "rng-trace", {"seed": 7}) == {"echo": {"seed": 7}}


def test_a_solve_with_no_usable_engine_raises(tmp_path, monkeypatch):
    # There is no second implementation to quietly serve instead, so the only
    # honest answer is the refusal plus which of the three things went wrong.
    _staged(tmp_path, monkeypatch, _handshake())
    with pytest.raises(EngineError, match="does not advertise"):
        solve_with_engine([set()], [], None)


def test_the_search_runs_on_the_engine_when_there_is_one():
    if packaged_engine_path() is None:
        pytest.skip("no msm_solver staged for this platform")
    assert Backend("solve") == "rust"


@pytest.fixture(scope="module")
def rust_engine():
    path = packaged_engine_path()
    if path is None:
        pytest.skip("no msm_solver staged for this platform (./dev.sh -be)")
    ResetEngineCache()
    info = GetEngine()
    if info is None:
        pytest.fail(f"a binary is staged at [{path}] but could not be resolved or failed its handshake")
    return info


def test_the_staged_engine_agrees_about_the_contract(rust_engine):
    assert rust_engine.wire_version == SOLVER_WIRE_VERSION
    assert rust_engine.rng_version == SOLVER_RNG_VERSION
    assert "rng" in rust_engine.capabilities


def test_the_two_streams_are_the_same_stream(rust_engine):
    ops = [{"op": "raw_words", "n": 64}]
    assert execute_ops_via_engine(rust_engine, 42, ops)["results"] == \
        execute_ops(42, ops)["results"]


def _ulp_distance(a: float, b: float) -> int:
    lo, hi = (a, b) if a < b else (b, a)
    n = 0
    while lo < hi and n <= SOLVER_VALUE_TOLERANCE_ULP:
        lo = math.nextafter(lo, math.inf)
        n += 1
    return n


def _assert_values_agree(mine, theirs, seed: int, index: int) -> None:
    if isinstance(mine, list):
        assert isinstance(theirs, list) and len(mine) == len(theirs)
        for a, b in zip(mine, theirs):
            _assert_values_agree(a, b, seed, index)
        return
    if isinstance(mine, float) and isinstance(theirs, float) and mine != theirs:
        assert _ulp_distance(mine, theirs) <= SOLVER_VALUE_TOLERANCE_ULP, (
            f"seed {seed} op {index}: {mine!r} and {theirs!r} are further apart "
            f"than {SOLVER_VALUE_TOLERANCE_ULP} ULP"
        )
        return
    assert mine == theirs, f"seed {seed} op {index}: {mine!r} != {theirs!r}"


@pytest.mark.parametrize("seed", [0, 1, 42, 2**31, 2**63 - 1])
def test_a_generated_script_agrees_draw_for_draw(rust_engine, seed):
    # The draw stream is exact; derived values carry libm's last digit. See
    # SOLVER_VALUE_TOLERANCE_ULP for why the tolerance is one ULP and not zero.
    ops = generate_ops(seed, 2000)
    mine = execute_ops(seed, ops)
    theirs = execute_ops_via_engine(rust_engine, seed, ops)
    assert theirs["rng_version"] == mine["rng_version"]
    assert theirs["draws"] == mine["draws"]

    assert len(theirs["results"]) == len(mine["results"])
    for i, (a, b) in enumerate(zip(mine["results"], theirs["results"])):
        assert a.keys() == b.keys()
        assert a.get("draws") == b.get("draws"), f"seed {seed} op {i}: draw count moved"
        for k in a:
            if k == "draws":
                continue
            _assert_values_agree(a[k], b[k], seed, i)


def test_the_hard_cases_agree(rust_engine):
    ops = [
        {"op": "argmax", "scores": [1.0, 3.0, 3.0, 3.0, 2.0]},
        {"op": "argmin", "values": [2.0, 1.0, 1.0, 5.0]},
        {"op": "argmax", "scores": ["nan", "nan", "nan"]},
        {"op": "argmin", "values": ["nan", "nan", "nan"]},
        {"op": "argmax", "scores": ["nan", 1.0, "nan", "-inf"]},
        {"op": "argmin", "values": ["nan", 1.0, "nan", "inf"]},
        {"op": "top_k", "scores": ["nan", 1.0, 1.0, "inf", "-inf"], "k": 5},
        {"op": "top_k", "scores": [0.0, -0.0, 0.0, -0.0], "k": 4},
        {"op": "argmax", "scores": [-0.0, 0.0]},
        {"op": "argmin", "values": [0.0, -0.0]},
        {"op": "pick_top_k", "scores": [-0.0, 0.0, -0.0], "k": 3},
        {"op": "top_k", "scores": [1.0, 2.0], "k": 0},
        {"op": "top_k", "scores": [1.0, 2.0], "k": 9},
        {"op": "bounded_int", "n": 0},
        {"op": "bounded_int", "n": 1},
        {"op": "weighted_index", "weights": [5]},
        {"op": "raw_words", "n": 1},
        {"op": "pick_top_k", "scores": [3.0, 3.0, 3.0], "k": 3},
        {"op": "weighted_index", "weights": [1, 1000000, 1]},
        {"op": "bounded_int", "n": 2**32},
        {"op": "bounded_int", "n": 3},
    ]
    assert execute_ops_via_engine(rust_engine, 12345, ops) == execute_ops(12345, ops)


def test_a_float_survives_the_crossing(rust_engine):
    values = [
        1 - 2**-53,
        1.0, 0.5, 0.1, 2/3,
        5e-324,
        1e-300, 1e300,
        0.10669708251953125,
        0.9640369415283203,
    ]
    ops = [
        {"op": "log2", "values": values},
        {"op": "top_k", "scores": [1.0, 1 - 2**-53, 1.0, 1 - 2**-52], "k": 4},
    ]
    assert execute_ops_via_engine(rust_engine, 3, ops) == execute_ops(3, ops)


def test_the_entropy_agrees_where_numpy_would_not(rust_engine):
    ops = [{"op": "entropy", "counts": c} for c in [
        [], [1], [0, 0], [1, 1, 1, 1], [3, 3],
        list(range(1, 10)),
        list(range(1, 40)),
        [1]*200,
        [7, 1, 1, 1, 1, 1, 1, 1, 1000000],
    ]]
    assert execute_ops_via_engine(rust_engine, 5, ops) == execute_ops(5, ops)
