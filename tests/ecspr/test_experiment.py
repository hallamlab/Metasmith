import numpy as np
import pandas as pd
import pytest

from ecspr.model import conditions as cond_mod
from ecspr.model import nulls, probes, scoring
from ecspr.model import tests as test_mod
from ecspr.model.evidence import POOL_LAM0, nomination_contributions
from ecspr.model.gpr import _normalise, load_gpr, resolve_weights

HOST = "iML1515"


# What a curated assertion is worth, in nats -- `fabfos_evidence.ASSERTION_LLR`. Every
# row here is on an assertion channel, so they all carry the one declared constant.
ASSERTION_LLR = 2.99


def _gpr_rows(unit, feature, mnxrs, *, condition=None, channel="gem_gpr", gene=None):
    return [dict(source=unit, orf=feature, gene=gene or feature, channel=channel,
                 mnxr=r, intermediate_id=r, intermediate_name=r,
                 llr=ASSERTION_LLR, llr_route="asserted", projection_via="x",
                 evidence_quality="reviewed", lane_set="curated",
                 build_id="t", host="h", genome_unit=unit,
                 feature_kind="gene", feature_name=feature, gpr_rule=None,
                 in_atom_universe=True, condition_id=condition)
            for r in mnxrs]


@pytest.fixture
def gpr_path(tmp_path):
    rows = []
    rows += _gpr_rows(HOST, "b1", ["R1"])
    rows += _gpr_rows(HOST, "b2", ["R2"])
    rows += _gpr_rows(HOST, "b3", ["R3"])
    rows += _gpr_rows("clone", "cA", ["R2"], condition="C_over", channel="manual_gpr")
    rows += _gpr_rows("clone", "cB", ["R4"], condition="C_new", channel="manual_gpr")
    rows += _gpr_rows("clone", "cC", ["RX"], condition="C_ctrl", channel="manual_gpr")
    p = tmp_path / "gpr.parquet"
    pd.DataFrame(rows).to_parquet(p, index=False)
    return p


@pytest.fixture
def pairs_path(tmp_path):
    rows = [("R1", "S", "M"), ("R2", "M", "P1"), ("R3", "M", "P2"), ("R4", "M", "P2")]
    df = pd.DataFrame([dict(mnxr=r, element="C", substrate=s, product=p_, sub_idx=0,
                            prod_idx=0, pair_w=1.0) for r, s, p_ in rows])
    p = tmp_path / "pairs.parquet"
    df.to_parquet(p, index=False)
    return p


GPR_TABLE = "gpr.parquet"

# The host, every time. A condition is the background plus whatever it adds.
BACKGROUND = cond_mod.Step(table=GPR_TABLE, type="genome_unit", set=(HOST,),
                           operation="union")
BASELINE = cond_mod.Condition(condition_id="baseline", steps=(BACKGROUND,))


def _cond(cid, values, **_meta):
    return cond_mod.Condition(
        condition_id=cid,
        steps=(BACKGROUND, cond_mod.Step(table=GPR_TABLE, type="condition_id",
                                         set=tuple(values), operation="union")))


def _test(cid, **meta):
    """The measurement every condition here is put under: one ground, two sinks."""
    return test_mod.Test(
        test_id=cid, network=cid, arm="ecspr",
        read_against="" if cid == "baseline" else "baseline",
        settings=dict(element="C", source_hub="S", sink_hub="P1|P2"),
        meta=dict(n_units=1) | meta)


def _corpus(gpr_path):
    return {GPR_TABLE: load_gpr(gpr_path)}


def test_background_alone_is_the_host(gpr_path):
    w, cov = resolve_weights(BASELINE, _corpus(gpr_path), weighting="uniform")
    assert w == {"R1": 1.0, "R2": 1.0, "R3": 1.0}
    assert cov["n_units"] == 1


def test_an_overexpression_is_a_sum_not_a_policy(gpr_path):
    w, cov = resolve_weights(_cond("C_over", ("C_over",)), _corpus(gpr_path),
                             weighting="uniform")
    assert w == {"R1": 1.0, "R2": 2.0, "R3": 1.0}
    assert cov["n_units"] == 2, "the host copy and the clone copy are two units"


def test_a_deletion_is_a_subtract_step(gpr_path):
    c = cond_mod.Condition(
        condition_id="C_del",
        steps=(BACKGROUND, cond_mod.Step(table=GPR_TABLE, type="mnxr", set=("R3",),
                                         operation="subtract")))
    w, _ = resolve_weights(c, _corpus(gpr_path), weighting="uniform")
    assert w == {"R1": 1.0, "R2": 1.0}


def _belief(rows, table="rows.parquet"):
    """The host's whole table, weighted by belief. The properties below are about the
    allocation, so the construction is the simplest one that has all the rows in it."""
    c = cond_mod.Condition("x", (cond_mod.Step(table=table, type="", set=(),
                                               operation="union"),))
    w, _ = resolve_weights(c, {table: pd.DataFrame(rows)}, weighting="belief")
    return w


def _lam(E):
    """The pooled log-odds behind a conductance -- `pool_logodds` inverted.

    THE ADDITIVE AXIS, and the one these properties are claims about. Evidence pools in
    log-odds; a probability compresses as it approaches the ceiling, so a ratio taken on
    `E` measures how saturated the pool is rather than how much the evidence was worth.
    """
    return float(np.log(E / (1.0 - E))) - POOL_LAM0


def test_belief_weighting_conserves_per_feature(gpr_path):
    # Each feature's nominations still sum to 1.0 BEFORE pooling, so a promiscuous
    # annotation cannot out-vote a specific one. That ledger is `belief_mass`.
    df = load_gpr(gpr_path)
    extra = pd.DataFrame(_gpr_rows(HOST, "b4", ["R1", "R2"]))
    rows = _normalise(pd.concat([df, extra], ignore_index=True))
    rows = rows[rows.genome_unit == HOST]
    mass = nomination_contributions(rows).groupby("mnxr")["contrib"].sum()
    assert mass["R3"] == pytest.approx(1.0)
    assert mass["R1"] == pytest.approx(1.5)   # b1's whole 1.0, plus half of b4's
    assert mass["R2"] == pytest.approx(1.5)
    assert mass.sum() == pytest.approx(rows.orf.nunique())


def test_belief_weighting_is_a_bounded_probability(gpr_path):
    rows = [r for f in range(40) for r in _gpr_rows(HOST, f"b{f}", ["R1"])]
    w = _belief(rows)
    assert 0.0 < w["R1"] < 1.0


def test_repeating_one_assertion_saturates(gpr_path):
    # ONE ASSERTION IS WORTH ITS OWN `llr` AND NO MORE, however many genes repeat it.
    # That ceiling is the whole reason `pool_logodds` exists: 65 ORFs asserting one EC
    # through one channel used to out-rank a reaction three methods confirmed.
    one = _lam(_belief(_gpr_rows(HOST, "b1", ["R1"]))["R1"])
    two = _lam(_belief([r for f in range(2)
                        for r in _gpr_rows(HOST, f"b{f}", ["R1"])])["R1"])
    forty = _lam(_belief([r for f in range(40)
                          for r in _gpr_rows(HOST, f"b{f}", ["R1"])])["R1"])
    assert one < two < forty <= ASSERTION_LLR
    # 40 repeats REACH the ceiling exactly: `1 - exp(-40/0.5)` is 1.0 in float64, so
    # this is the saturation being attained rather than approached.
    assert forty == pytest.approx(ASSERTION_LLR)
    # The 38 further repeats buy under a fifth of what the second one did.
    assert forty - two < 0.2 * (two - one)


def test_two_channels_agreeing_beat_one_channel_repeated(gpr_path):
    # THE CEILING IS THE TEST. Two genes on one channel are ONE assertion, so no number
    # of them can pass what that assertion is worth. Two channels are TWO assertions and
    # add, so one gene on two channels passes it with room to spare.
    #
    # Stated in log-odds, and without a ratio: on the probability axis the repeated pair
    # is already 97% of the way to its own ceiling, so a ratio there measures saturation
    # rather than evidence. A ratio in log-odds is no better -- belief conservation
    # splits the ORF's 1.0 across its two lanes, so each of the agreeing channels
    # carries half the mass and the ratio lands at 1.29. The ceiling does not move.
    repeated = _lam(_belief([r for f in range(2)
                             for r in _gpr_rows(HOST, f"b{f}", ["R1"])])["R1"])
    agreeing = _lam(_belief(_gpr_rows(HOST, "b1", ["R1"], channel="ch_a")
                            + _gpr_rows(HOST, "b1", ["R1"], channel="ch_b"))["R1"])
    assert repeated < ASSERTION_LLR < agreeing


def test_a_second_unit_is_a_second_assertion(gpr_path):
    host = _gpr_rows(HOST, "b2", ["R2"])
    clone = _gpr_rows("clone", "cA", ["R2"], condition="C_over")
    alone = _belief(host)["R2"]
    together = _belief(host + clone)["R2"]
    assert together > 1.5 * alone


def test_an_unknown_selector_column_is_refused(gpr_path):
    bad = cond_mod.Condition("x", (BACKGROUND, cond_mod.Step(
        table=GPR_TABLE, type="nope", set=("x",), operation="union")))
    with pytest.raises(ValueError, match="not a column"):
        resolve_weights(bad, _corpus(gpr_path))


@pytest.mark.parametrize("ext", [".parquet", ".tsv"])
def test_conditions_round_trip(tmp_path, ext):
    c = _cond("C_over", ("C_over",))
    p = cond_mod.write([BASELINE, c], tmp_path / f"conds{ext}")
    back = cond_mod.read(p)
    assert [x.condition_id for x in back] == ["baseline", "C_over"]
    assert back[1].steps[1].set == ("C_over",)
    assert back[1].steps[0].set == (HOST,)
    assert back[1].tables == (GPR_TABLE,)


def test_the_explicit_form_is_one_whole_table():
    c = cond_mod.single("whole_table", GPR_TABLE)
    assert len(c.steps) == 1
    assert c.steps[0].type == "" and c.steps[0].operation == "union"


def _run(pairs_path, gpr_path, conds, probe, tmp_path, tests=None, **kw):
    basis = probes.Basis(pairs_path, None, element="C")
    ts = tests if tests is not None else [_test(c.condition_id) for c in conds]
    return probes.run(basis, gpr_path, conds, ts, probe=probe, weighting="uniform",
                      shard_dir=tmp_path / f"shards_{probe}", log=lambda m: None, **kw)


def _value(df, tid, readout):
    r = df[(df.test_id == tid) & (df.readout == readout)]
    return float(r.value.iloc[0])


@pytest.mark.parametrize("probe", ["two-point", "ground"])
def test_both_probes_emit_one_schema(pairs_path, gpr_path, tmp_path, probe):
    df = _run(pairs_path, gpr_path, [BASELINE], probe, tmp_path)
    assert list(df.columns) == list(probes.RESULT_COLUMNS)
    assert set(df.readout) >= {"total", "P1", "P2", "_n_edges", "_converged"}


def test_an_added_route_raises_the_total(pairs_path, gpr_path, tmp_path):
    conds = [BASELINE, _cond("C_new", ("C_new",)), _cond("C_ctrl", ("C_ctrl",))]
    df = _run(pairs_path, gpr_path, conds, "two-point", tmp_path)
    base = _value(df, "baseline", "total")
    assert _value(df, "C_new", "total") > base
    assert _value(df, "C_ctrl", "total") == pytest.approx(base, rel=0, abs=0.0)


def test_a_control_returns_the_baseline_exactly(pairs_path, gpr_path, tmp_path):
    conds = [BASELINE, _cond("C_ctrl", ("C_ctrl",))]
    df = _run(pairs_path, gpr_path, conds, "ground", tmp_path,
              tests=[_test("baseline"), _test("C_ctrl", is_control=True)])
    for readout in ("total", "P1", "P2"):
        assert _value(df, "C_ctrl", readout) == _value(df, "baseline", readout)


def test_resume_skips_what_is_on_disk(pairs_path, gpr_path, tmp_path):
    conds = [BASELINE, _cond("C_new", ("C_new",))]
    ts = [_test(c.condition_id) for c in conds]
    a = _run(pairs_path, gpr_path, conds, "ground", tmp_path)
    seen = []
    basis = probes.Basis(pairs_path, None, element="C")
    b = probes.run(basis, gpr_path, conds, ts, probe="ground", weighting="uniform",
                   shard_dir=tmp_path / "shards_ground", log=seen.append)
    assert any("resumed 2" in m for m in seen)
    pd.testing.assert_frame_equal(a, b)


def test_a_missing_source_abstains_rather_than_scoring_zero(pairs_path, gpr_path,
                                                            tmp_path):
    c = cond_mod.Condition(condition_id="nowhere", steps=(BACKGROUND,))
    t = test_mod.Test(test_id="nowhere", network="nowhere", arm="ecspr",
                      settings=dict(element="C", source_hub="ABSENT", sink_hub="P1"))
    df = _run(pairs_path, gpr_path, [c], "two-point", tmp_path, tests=[t])
    assert _value(df, "nowhere", "_abstained") == 1.0
    assert "total" not in set(df.readout)


def test_orientation_is_inert_on_a_symmetric_reference(pairs_path, gpr_path, tmp_path):
    d = pd.DataFrame([dict(mnxr=r, ratio=1.0) for r in ("R1", "R2", "R3", "R4")])
    dp = tmp_path / "direction.parquet"
    d.to_parquet(dp, index=False)
    out = {}
    for o in ("as_written", "reversed"):
        basis = probes.Basis(pairs_path, dp, element="C", orientation=o)
        out[o] = probes.run(basis, gpr_path, [BASELINE], [_test("baseline")],
                            probe="ground", weighting="uniform", log=lambda m: None)
    a, b = out["as_written"], out["reversed"]
    keep = ~a.readout.str.startswith("_")
    assert np.allclose(a[keep].value.to_numpy(float), b[keep].value.to_numpy(float))


def _draw(gpr_path, like_tests, **kw):
    like_conds = [_cond(t.network, (t.network,)) for t in like_tests]
    return nulls.draw(gpr_path, like_conds, like_tests, background=(BACKGROUND,),
                      pool_table=GPR_TABLE, log=lambda m: None, **kw)


def test_draw_is_deterministic_and_emits_both_tables(gpr_path, tmp_path):
    like = [_test("C_new")]
    ca, ta = _draw(gpr_path, like, n=8, seed=7, draw_column="orf")
    cb, tb = _draw(gpr_path, like, n=8, seed=7, draw_column="orf")
    assert [c.steps[-1].set for c in ca] == [c.steps[-1].set for c in cb]
    pa = cond_mod.write(ca, tmp_path / "n1.parquet")
    pb = cond_mod.write(cb, tmp_path / "n2.parquet")
    assert pa.read_bytes() == pb.read_bytes(), "the same seed must write one pool"
    back = cond_mod.read(pa)
    assert len(back) == 8
    # THE BACKGROUND IS CARRIED, not re-authored: a null on a graph with no host in it
    # abstains, and would read as "no effect" rather than as a broken draw.
    assert all(c.steps[0] == BACKGROUND for c in back)
    assert all(c.steps[-1].type == "orf" for c in back)
    assert all(t.sinks == ("P1", "P2") and t.source_hub == "S" for t in ta), \
        "a drawn test inherits the measurement of the arm it is the null for"
    assert {t.network for t in ta} == {c.condition_id for c in ca}


def test_a_null_carries_a_multi_step_background_including_its_subtracts(gpr_path):
    # THE ASKA PANEL'S SHAPE, and what replaced a script that copied two columns onto
    # every drawn row by hand. Its background is TWO whole tables -- the host GEM and the
    # plasmid -- minus a constant set of the host's reactions, because every strain in
    # that paper is a deletion mutant. A null drawn on the plain host is a null for a
    # different experiment, and it comes back looking like a smaller effect rather than
    # like a broken draw.
    deleted = cond_mod.Step(table=GPR_TABLE, type="intermediate_id", set=("R3",),
                            operation="subtract")
    baseline = cond_mod.Condition("baseline", (BACKGROUND, deleted))
    like = [_test("C_new")]
    conds, _ts = nulls.draw([gpr_path], [baseline], like, n=5, seed=11,
                            background=baseline.steps, draw_column="orf",
                            pool_table=GPR_TABLE, log=lambda _m: None)
    assert all(c.steps[:2] == (BACKGROUND, deleted) for c in conds)
    # and the subtract still bites: R3 is out of every drawn construction
    w, _cov = resolve_weights(conds[0], _corpus(gpr_path))
    assert "R3" not in w


def test_draw_refuses_an_unmatched_null(gpr_path):
    bare = test_mod.Test(test_id="C_new", network="C_new", arm="ecspr",
                         settings=dict(element="C", source_hub="S", sink_hub="P1"))
    with pytest.raises(ValueError, match="n_units"):
        _draw(gpr_path, [bare], n=4, seed=1)


def test_the_null_arm_is_the_same_command(pairs_path, gpr_path, tmp_path):
    like = [_test("C_new")]
    like_c = [_cond("C_new", ("C_new",))]
    pool_c, pool_t = _draw(gpr_path, like, n=6, seed=3)
    obs = _run(pairs_path, gpr_path, [BASELINE] + like_c, "two-point", tmp_path,
               tests=[_test("baseline")] + like)
    nul = _run(pairs_path, gpr_path, pool_c, "two-point", tmp_path, tests=pool_t)
    assert set(nul.probe) == {"two-point"} and len(set(nul.test_id)) == 6

    scored = scoring.score(obs, nul, tests=[_test("baseline")] + like + pool_t)
    row = scored[(scored.test_id == "C_new") & (scored.readout == "total")]
    assert len(row) == 1
    assert row.delta.iloc[0] > 0
    assert row.null_n.iloc[0] == 6
    assert 0.0 <= row.p_emp.iloc[0] <= 1.0
    assert row.p_floor.iloc[0] == pytest.approx(1.0 / 7.0)


def test_the_gate_reports_both_spreads(pairs_path, gpr_path, tmp_path):
    like = [_test("C_new"), _test("C_ctrl", is_control=True),
            _test("C_over", is_control=True)]
    like_c = [_cond(t.network, (t.network,)) for t in like]
    pool_c, pool_t = _draw(gpr_path, like, n=6, seed=11)
    obs = _run(pairs_path, gpr_path, [BASELINE] + like_c, "two-point", tmp_path,
               tests=[_test("baseline")] + like)
    nul = _run(pairs_path, gpr_path, pool_c, "two-point", tmp_path, tests=pool_t)
    g = scoring.gate(scoring.score(obs, nul,
                                   tests=[_test("baseline")] + like + pool_t))
    tot = g[g.readout == "total"].iloc[0]
    assert tot.n_controls == 2
    assert tot.null_n == 6
    assert "null_over_control" in g.columns


def test_the_baseline_flag_and_the_column_may_not_disagree(pairs_path, gpr_path,
                                                           tmp_path):
    # `read_against` is per test, so it can say two things in one run and a single flag
    # cannot. Honouring both silently would make the deltas depend on which a reader
    # happened to look at.
    like = [_test("C_new")]
    like_c = [_cond("C_new", ("C_new",))]
    obs = _run(pairs_path, gpr_path, [BASELINE] + like_c, "two-point", tmp_path,
               tests=[_test("baseline")] + like)
    with pytest.raises(ValueError, match="disagrees with the read_against"):
        scoring.score(obs, obs, baseline="C_new", tests=[_test("baseline")] + like)


@pytest.mark.parametrize("ext", [".parquet", ".tsv"])
def test_results_round_trip_exactly(pairs_path, gpr_path, tmp_path, ext):
    df = _run(pairs_path, gpr_path, [BASELINE], "ground", tmp_path)
    p = probes.write_results(df, tmp_path / f"r{ext}")
    back = probes.read_results(p)
    a = df[df.readout == "total"].value.iloc[0]
    b = back[back.readout == "total"].value.iloc[0]
    assert b == a, f"{b!r} != {a!r}"


def test_responders_cover_what_they_claim_and_leave_the_rest_as_floor():
    import numpy as np
    from ecspr.model.scoring import responders

    v = np.array([10.0, 5.0, 1.0, 0.5, 0.1, 0.0])
    keep = responders(v, 0.90)
    assert v[keep].sum() / v.sum() >= 0.90
    assert v[responders(v, 0.90) & ~responders(v, 0.5)].size >= 1
    assert not responders(np.zeros(4)).any()
    assert responders(v, 1.0).sum() == 5          # the exact zero never joins
