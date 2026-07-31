"""Finding the Rust engine, refusing the wrong one, and living without it.

Two halves, and they are deliberately independent.

The first half needs no Rust at all. Every branch of the resolution logic is
driven with a *fake* engine -- a script that prints whatever handshake the test
wants -- so the refusals get exercised on every machine and in CI, including the
ones that only ever fire when someone ships a mismatched build. The fallback in
particular is a real path here, not a flag nobody runs: the suite that follows
this file has always run with no binary present, and
`test_a_solve_is_identical_with_the_engine_forced_off` makes that an assertion
instead of an accident.

The second half is the differential gate and skips when no binary is staged. It
replays generated scripts of decisions through both implementations and compares
them draw for draw. See `metasmith.testing.rng_trace` for why the draw counter is
the part that matters.
"""

from __future__ import annotations

import stat
import sys

import pytest

from metasmith.models import solver_engine as engine_module
from metasmith.models.solver_engine import (
    ENGINE_NAME,
    ENV_OVERRIDE,
    SOLVER_WIRE_VERSION,
    Backend,
    CallEngine,
    EngineError,
    EngineFor,
    GetEngine,
    ResetEngineCache,
    packaged_engine_path,
    platform_slot,
    probe_engine,
)
from metasmith.models.solver_rng import SOLVER_RNG_VERSION
from metasmith.testing.rng_trace import execute_ops, execute_ops_via_engine, generate_ops


@pytest.fixture(autouse=True)
def _isolate_engine_cache(monkeypatch):
    """The probe is cached per process; every test here changes what it'd find."""
    monkeypatch.delenv(ENV_OVERRIDE, raising=False)
    ResetEngineCache()
    yield
    ResetEngineCache()


def _fake_engine(tmp_path, body: str, name: str="fake_engine"):
    """A binary that behaves however the test needs it to.

    `sys.executable` rather than `python` in the shebang: the test env is a
    conda env that may not put a bare `python` on PATH, and a fake that fails to
    launch would pass the refusal tests for the wrong reason.
    """
    p = tmp_path/name
    p.write_text(f"#!{sys.executable}\nimport json, sys\n{body}\n", encoding="utf-8")
    p.chmod(p.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
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


# --------------------------------------------------------------------------
# resolution


def test_the_platform_slot_uses_the_relays_naming():
    """One convention in the repo, not two -- `bootstrap.py` picked it first."""
    assert platform_slot("x86_64", "Linux") == "x86_64-linux"
    assert platform_slot("aarch64", "Linux") == "arm64-linux"   # linux says aarch64
    assert platform_slot("arm64", "Darwin") == "arm64-darwin"   # macos says arm64
    assert platform_slot("AMD64", "Linux") == "x86_64-linux"


def test_a_missing_binary_is_the_normal_case(tmp_path, monkeypatch):
    """A source checkout ships no binaries and must not complain about it."""
    monkeypatch.setattr(engine_module, "ENGINE_DIR", tmp_path)
    assert packaged_engine_path(tmp_path) is None
    assert GetEngine() is None
    assert Backend("rng") == "python"


def test_the_env_var_can_force_the_python_path(tmp_path, monkeypatch):
    """The fallback is only a real path if something exercises it on purpose."""
    good = _fake_engine(tmp_path, _handshake())
    monkeypatch.setattr(engine_module, "ENGINE_DIR", tmp_path)
    monkeypatch.setenv(ENV_OVERRIDE, str(good))
    assert GetEngine() is not None

    ResetEngineCache()
    monkeypatch.setenv(ENV_OVERRIDE, "python")
    assert GetEngine() is None
    assert Backend("rng") == "python"


def test_the_env_var_pointing_nowhere_falls_back(monkeypatch, tmp_path):
    monkeypatch.setenv(ENV_OVERRIDE, str(tmp_path/"not-here"))
    assert GetEngine() is None


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
    """The `LIN_PAYLOAD_VERSION` lesson, as a test.

    A version constant that is emitted but never checked buys nothing -- that
    desync failed every containerized task while the fast suite stayed green.
    Two implementations that disagree here do not crash, they quietly return
    different plans, so the disagreement has to be refused where it is visible.
    """
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


def test_a_capability_it_does_not_advertise_falls_back(tmp_path, monkeypatch):
    """The port lands one piece at a time, and says which piece it has landed."""
    monkeypatch.setattr(engine_module, "ENGINE_DIR", tmp_path)
    monkeypatch.setenv(ENV_OVERRIDE, str(_fake_engine(tmp_path, _handshake())))
    assert EngineFor("rng") is not None
    assert EngineFor("solve") is None
    assert Backend("solve") == "python"


def test_a_believed_engine_that_then_fails_raises(tmp_path, monkeypatch):
    """Past the handshake, a failure is a bug in one of the two sides.

    Falling back here would hide it: the binary has already claimed the right
    versions and the right capability, so silence would turn a real defect into
    a mysterious slowdown.
    """
    body = _handshake() + (
        "\nif sys.argv[1] != 'version':\n"
        "    sys.stderr.write('boom'); sys.exit(1)\n"
    )
    monkeypatch.setenv(ENV_OVERRIDE, str(_fake_engine(tmp_path, body)))
    info = GetEngine()
    assert info is not None
    with pytest.raises(EngineError):
        CallEngine(info, "rng-trace", {"seed": 1})


def test_a_believed_engine_round_trips_a_payload(tmp_path, monkeypatch):
    monkeypatch.setenv(ENV_OVERRIDE, str(_fake_engine(tmp_path, _handshake())))
    info = GetEngine()
    assert info is not None
    assert CallEngine(info, "rng-trace", {"seed": 7}) == {"echo": {"seed": 7}}


# --------------------------------------------------------------------------
# the fallback, as a gate rather than an assumption


def test_the_search_is_still_python_today():
    """T5b ships the delivery path, not the search. When this fails, T5c landed."""
    assert Backend("solve") == "python"


def test_a_solve_is_identical_with_the_engine_forced_off(monkeypatch):
    """The fallback gate. Trivially true while no engine solves; not later.

    It is written now so that the day the Rust search is switched on, the
    question "does turning it off still give the same plan" is already being
    asked on every run rather than being remembered.
    """
    from metasmith.testing.solver_bench import CORPUS
    from metasmith.testing.solver_verification import generate_problem, plan_fingerprint

    seed, dials = CORPUS[0][1], CORPUS[0][2]
    problem = generate_problem(seed, dials, name=CORPUS[0][0])

    ResetEngineCache()
    with_engine = plan_fingerprint(problem.solve())
    monkeypatch.setenv(ENV_OVERRIDE, "python")
    ResetEngineCache()
    assert Backend("solve") == "python"
    assert plan_fingerprint(problem.solve()) == with_engine


# --------------------------------------------------------------------------
# the differential gate


@pytest.fixture(scope="module")
def rust_engine():
    """The staged binary, or a skip. Built by `./dev.sh -be` (or `-bel`)."""
    path = packaged_engine_path()
    if path is None:
        pytest.skip("no msm_solver staged for this platform (./dev.sh -be)")
    info = probe_engine(path)
    if info is None:
        pytest.fail(f"a binary is staged at [{path}] but failed its handshake")
    return info


def test_the_staged_engine_agrees_about_the_contract(rust_engine):
    assert rust_engine.wire_version == SOLVER_WIRE_VERSION
    assert rust_engine.rng_version == SOLVER_RNG_VERSION
    assert "rng" in rust_engine.capabilities


def test_the_two_streams_are_the_same_stream(rust_engine):
    """Localises a failure: if this passes and the scripts below fail, the
    disagreement is in a decision rule, not in ChaCha8."""
    ops = [{"op": "raw_words", "n": 64}]
    assert execute_ops_via_engine(rust_engine, 42, ops)["results"] == \
        execute_ops(42, ops)["results"]


@pytest.mark.parametrize("seed", [0, 1, 42, 2**31, 2**63 - 1])
def test_a_generated_script_agrees_draw_for_draw(rust_engine, seed):
    ops = generate_ops(seed, 2000)
    mine = execute_ops(seed, ops)
    theirs = execute_ops_via_engine(rust_engine, seed, ops)
    assert theirs["rng_version"] == mine["rng_version"]
    # Compared as a whole rather than op by op: the first differing entry is
    # what pytest reports, and its `draws` says whether the streams had already
    # drifted before the value went wrong.
    assert theirs["results"] == mine["results"]
    assert theirs["draws"] == mine["draws"]


def test_the_hard_cases_agree(rust_engine):
    """The three places a reasonable implementation diverges, written by hand.

    Ties (first extremum, not "whichever the partition left there"), NaN (worst
    in *both* directions, which needs two mappings rather than one), and
    degenerate choices (which must consume no word at all -- a stream that
    drifts by one word diverges completely from there on).
    """
    ops = [
        {"op": "argmax", "scores": [1.0, 3.0, 3.0, 3.0, 2.0]},
        {"op": "argmin", "values": [2.0, 1.0, 1.0, 5.0]},
        {"op": "argmax", "scores": ["nan", "nan", "nan"]},
        {"op": "argmin", "values": ["nan", "nan", "nan"]},
        {"op": "argmax", "scores": ["nan", 1.0, "nan", "-inf"]},
        {"op": "argmin", "values": ["nan", 1.0, "nan", "inf"]},
        {"op": "top_k", "scores": ["nan", 1.0, 1.0, "inf", "-inf"], "k": 5},
        # -0.0 vs 0.0 is the one tie a Rust `total_cmp` would break and python
        # would not; both must call them equal and fall through to the index.
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
    """The wire itself, before anything computes with what it carried.

    This is the one that caught `serde_json`'s default float parsing, which is
    documented as best-effort and read `1 - 2**-53` as exactly `1.0`. Nothing
    about that is visible at the call site: the engine answers confidently, with
    a number derived from an input it was never sent. `log2` is the readout
    rather than the subject -- it turns a one-ulp difference in the argument into
    a difference the eye can see, and near 1.0 it turns it into a chasm.
    """
    values = [
        1 - 2**-53,            # the double just below 1.0
        1.0, 0.5, 0.1, 2/3,
        5e-324,                # the smallest denormal
        1e-300, 1e300,
        0.10669708251953125,   # and two the best-effort parser got wrong by
        0.9640369415283203,    # a handful of ulps rather than by everything
    ]
    ops = [
        {"op": "log2", "values": values},
        # a ranking of values that differ only in the last bit: if one side
        # parsed them differently, the two are no longer even tied
        {"op": "top_k", "scores": [1.0, 1 - 2**-53, 1.0, 1 - 2**-52], "k": 4},
    ]
    assert execute_ops_via_engine(rust_engine, 3, ops) == execute_ops(3, ops)


def test_the_entropy_agrees_where_numpy_would_not(rust_engine):
    """`solver_math.entropy`, at the sizes where a "better" sum diverges.

    Nine terms is where numpy's pairwise summation stops agreeing with a
    left-to-right one, so the counts run well past nine. The empty and singleton
    cases are here because they are the common ones -- most plans carry no
    lineage constraints at all.
    """
    ops = [{"op": "entropy", "counts": c} for c in [
        [], [1], [0, 0], [1, 1, 1, 1], [3, 3],
        list(range(1, 10)),            # exactly nine
        list(range(1, 40)),            # past numpy's 8-way unrolled block
        [1]*200,                       # past its 128-element recursion threshold
        [7, 1, 1, 1, 1, 1, 1, 1, 1000000],
    ]]
    assert execute_ops_via_engine(rust_engine, 5, ops) == execute_ops(5, ops)
