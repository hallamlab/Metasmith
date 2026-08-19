import numpy as np
import pandas as pd
import pytest

from ecspr.model import conditions as cond_mod
from ecspr.model import nulls, probes, scoring
from ecspr.model.evidence import nomination_contributions
from ecspr.model.gpr import _normalise, condition_weights, load_gpr

HOST = "iML1515"


def _gpr_rows(unit, feature, mnxrs, *, condition=None, channel="gem_gpr"):
    return [dict(source=unit, orf=feature, channel=channel, mnxr=r,
                 intermediate_id=r, intermediate_name=r, raw_score=1.0,
                 score_kind="presence", projection_via="x",
                 evidence_quality="reviewed", lane_set="curated",
                 build_id="t", host="h", unit_id=unit,
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


def _cond(cid, values, **meta):
    return cond_mod.Condition(
        condition_id=cid, element="C", source_hub="S", sinks=("P1", "P2"),
        background_column="unit_id", background_values=(HOST,),
        mask_column="condition_id", mask_values=values, meta=meta)


BASELINE = cond_mod.Condition(
    condition_id="baseline", element="C", source_hub="S", sinks=("P1", "P2"),
    background_column="unit_id", background_values=(HOST,), meta=dict(n_units=1))


def test_background_alone_is_the_host(gpr_path):
    w, cov = condition_weights(load_gpr(gpr_path), weighting="uniform",
                               **BASELINE.mask_kwargs())
    assert w == {"R1": 1.0, "R2": 1.0, "R3": 1.0}
    assert cov["n_units"] == 1


def test_an_overexpression_is_a_sum_not_a_policy(gpr_path):
    w, _ = condition_weights(load_gpr(gpr_path), weighting="uniform",
                             **_cond("C_over", ("C_over",)).mask_kwargs())
    assert w == {"R1": 1.0, "R2": 2.0, "R3": 1.0}


def test_a_deletion_is_the_drop_mask(gpr_path):
    c = cond_mod.Condition(
        condition_id="C_del", source_hub="S", background_column="unit_id",
        background_values=(HOST,), drop_column="mnxr", drop_values=("R3",))
    w, _ = condition_weights(load_gpr(gpr_path), weighting="uniform", **c.mask_kwargs())
    assert w == {"R1": 1.0, "R2": 1.0}


def _belief(rows, **mask):
    w, _ = condition_weights(pd.DataFrame(rows), weighting="belief",
                             background_column="unit_id", background_values=(HOST,),
                             **mask)
    return w


def test_belief_weighting_conserves_per_feature(gpr_path):
    """Each feature's nominations still sum to 1.0 BEFORE pooling, so a promiscuous
    annotation cannot out-vote a specific one. That ledger is `belief_mass`."""
    df = load_gpr(gpr_path)
    extra = pd.DataFrame(_gpr_rows(HOST, "b4", ["R1", "R2"]))
    rows = _normalise(pd.concat([df, extra], ignore_index=True))
    rows = rows[rows.unit_id == HOST]
    mass = nomination_contributions(rows).groupby("mnxr")["contrib"].sum()
    assert mass["R3"] == pytest.approx(1.0)
    assert mass["R1"] == pytest.approx(1.5)   # b1's whole 1.0, plus half of b4's
    assert mass["R2"] == pytest.approx(1.5)
    assert mass.sum() == pytest.approx(rows.orf.nunique())


def test_belief_weighting_is_a_bounded_probability(gpr_path):
    """The conductance a mask hands the builder is in (0, 1) however much evidence
    piles onto one reaction. An unbounded sum is what made a paralog family read as
    strength of evidence."""
    rows = [r for f in range(40) for r in _gpr_rows(HOST, f"b{f}", ["R1"])]
    w = _belief(rows)
    assert 0.0 < w["R1"] < 1.0


def test_repeating_one_assertion_saturates(gpr_path):
    """N genes asserting the SAME (unit, channel, evidence) is not N times the
    evidence. Two genes and forty land within a hair of each other, and neither
    reaches what a single second, independent assertion would buy."""
    one = _belief(_gpr_rows(HOST, "b1", ["R1"]))["R1"]
    two = _belief([r for f in range(2) for r in _gpr_rows(HOST, f"b{f}", ["R1"])])["R1"]
    forty = _belief([r for f in range(40) for r in _gpr_rows(HOST, f"b{f}", ["R1"])])["R1"]
    assert one < two < forty
    assert forty - two < 0.1 * (two - one)


def test_two_channels_agreeing_beat_one_channel_repeated(gpr_path):
    """The point of pooling. The same total belief mass, arriving through two distinct
    annotation methods rather than one repeated, is worth strictly and substantially
    more."""
    repeated = _belief([r for f in range(2) for r in _gpr_rows(HOST, f"b{f}", ["R1"])])
    agreeing = _belief(_gpr_rows(HOST, "b1", ["R1"], channel="ch_a")
                       + _gpr_rows(HOST, "b1", ["R1"], channel="ch_b"))
    assert agreeing["R1"] > 1.5 * repeated["R1"]


def test_a_second_unit_is_a_second_assertion(gpr_path):
    """The unit is inside the assertion key, so an overexpression is not a no-op even
    when the clone asserts the very same evidence through the very same channel.

    Two HOST paralogs saying this would pool as one assertion; a physically separate
    copy is separate evidence. (The feature ids differ because belief conservation is
    per-ORF and a shared id would merge the two rows before pooling ever sees them.)
    """
    host = _gpr_rows(HOST, "b2", ["R2"])
    clone = _gpr_rows("clone", "cA", ["R2"], condition="C_over")
    alone = _belief(host)["R2"]
    together = _belief(host + clone, mask_column="condition_id",
                       mask_values=("C_over",))["R2"]
    assert together > 1.5 * alone


def test_an_unknown_mask_column_is_refused(gpr_path):
    with pytest.raises(ValueError):
        condition_weights(load_gpr(gpr_path), mask_column="nope", mask_values=("x",))


@pytest.mark.parametrize("ext", [".parquet", ".tsv"])
def test_conditions_round_trip(tmp_path, ext):
    c = _cond("C_over", ("C_over",), n_units=1, is_control=False)
    p = cond_mod.write([BASELINE, c], tmp_path / f"conds{ext}")
    back = cond_mod.read(p)
    assert [x.condition_id for x in back] == ["baseline", "C_over"]
    assert back[1].mask_values == ("C_over",)
    assert back[1].background_values == (HOST,)
    assert back[1].sinks == ("P1", "P2")


def test_the_explicit_form_takes_no_mask():
    c = cond_mod.single("whole_table", element="C", source="S", sinks=["P1"])
    assert c.mask_column is None and c.mask_values == ()
    assert c.background_column is None and c.drop_column is None


def _run(pairs_path, gpr_path, conds, probe, tmp_path, **kw):
    basis = probes.Basis(pairs_path, None, element="C")
    return probes.run(basis, gpr_path, conds, probe=probe, weighting="uniform",
                      shard_dir=tmp_path / f"shards_{probe}", log=lambda m: None, **kw)


def _value(df, cid, readout):
    r = df[(df.condition_id == cid) & (df.readout == readout)]
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
    conds = [BASELINE, _cond("C_ctrl", ("C_ctrl",), is_control=True)]
    df = _run(pairs_path, gpr_path, conds, "ground", tmp_path)
    for readout in ("total", "P1", "P2"):
        assert _value(df, "C_ctrl", readout) == _value(df, "baseline", readout)


def test_resume_skips_what_is_on_disk(pairs_path, gpr_path, tmp_path):
    conds = [BASELINE, _cond("C_new", ("C_new",))]
    a = _run(pairs_path, gpr_path, conds, "ground", tmp_path)
    seen = []
    basis = probes.Basis(pairs_path, None, element="C")
    b = probes.run(basis, gpr_path, conds, probe="ground", weighting="uniform",
                   shard_dir=tmp_path / "shards_ground", log=seen.append)
    assert any("resumed 2" in m for m in seen)
    pd.testing.assert_frame_equal(a, b)


def test_a_missing_source_abstains_rather_than_scoring_zero(pairs_path, gpr_path,
                                                            tmp_path):
    c = cond_mod.Condition(condition_id="nowhere", element="C", source_hub="ABSENT",
                           sinks=("P1",), background_column="unit_id",
                           background_values=(HOST,))
    df = _run(pairs_path, gpr_path, [c], "two-point", tmp_path)
    assert _value(df, "nowhere", "_abstained") == 1.0
    assert "total" not in set(df.readout)


def test_orientation_is_inert_on_a_symmetric_reference(pairs_path, gpr_path, tmp_path):
    d = pd.DataFrame([dict(mnxr=r, ratio=1.0) for r in ("R1", "R2", "R3", "R4")])
    dp = tmp_path / "direction.parquet"
    d.to_parquet(dp, index=False)
    out = {}
    for o in ("as_written", "reversed"):
        basis = probes.Basis(pairs_path, dp, element="C", orientation=o)
        out[o] = probes.run(basis, gpr_path, [BASELINE], probe="ground",
                            weighting="uniform", log=lambda m: None)
    a, b = out["as_written"], out["reversed"]
    keep = ~a.readout.str.startswith("_")
    assert np.allclose(a[keep].value.to_numpy(float), b[keep].value.to_numpy(float))


def test_draw_is_deterministic_and_emits_a_conditions_table(gpr_path, tmp_path):
    like = [_cond("C_new", ("C_new",), n_units=1)]
    a = nulls.draw(gpr_path, like, n=8, seed=7, draw_column="orf",
                   log=lambda m: None)
    b = nulls.draw(gpr_path, like, n=8, seed=7, draw_column="orf",
                   log=lambda m: None)
    assert [c.mask_values for c in a] == [c.mask_values for c in b]
    pa = cond_mod.write(a, tmp_path / "n1.parquet")
    pb = cond_mod.write(b, tmp_path / "n2.parquet")
    assert pa.read_bytes() == pb.read_bytes(), "the same seed must write one pool"
    back = cond_mod.read(pa)
    assert len(back) == 8
    assert all(c.mask_column == "orf" for c in back)
    assert all(c.source_hub == "S" and c.sinks == ("P1", "P2") for c in back), \
        "a drawn condition inherits the terminals of the arm it is the null for"


def test_draw_refuses_an_unmatched_null(gpr_path):
    with pytest.raises(ValueError, match="n_units"):
        nulls.draw(gpr_path, [_cond("C_new", ("C_new",))], n=4, seed=1,
                   log=lambda m: None)


def test_the_null_arm_is_the_same_command(pairs_path, gpr_path, tmp_path):
    like = [_cond("C_new", ("C_new",), n_units=1)]
    pool = nulls.draw(gpr_path, like, n=6, seed=3, log=lambda m: None)
    obs = _run(pairs_path, gpr_path, [BASELINE] + like, "two-point", tmp_path)
    nul = _run(pairs_path, gpr_path, pool, "two-point", tmp_path)
    assert set(nul.probe) == {"two-point"} and len(set(nul.condition_id)) == 6

    scored = scoring.score(obs, nul, baseline="baseline", conditions=like + pool)
    row = scored[(scored.condition_id == "C_new") & (scored.readout == "total")]
    assert len(row) == 1
    assert row.delta.iloc[0] > 0
    assert row.null_n.iloc[0] == 6
    assert 0.0 <= row.p_emp.iloc[0] <= 1.0
    assert row.p_floor.iloc[0] == pytest.approx(1.0 / 7.0)


def test_the_gate_reports_both_spreads(pairs_path, gpr_path, tmp_path):
    like = [_cond("C_new", ("C_new",), n_units=1),
            _cond("C_ctrl", ("C_ctrl",), n_units=1, is_control=True),
            _cond("C_over", ("C_over",), n_units=1, is_control=True)]
    pool = nulls.draw(gpr_path, like, n=6, seed=11, log=lambda m: None)
    obs = _run(pairs_path, gpr_path, [BASELINE] + like, "two-point", tmp_path)
    nul = _run(pairs_path, gpr_path, pool, "two-point", tmp_path)
    g = scoring.gate(scoring.score(obs, nul, baseline="baseline",
                                   conditions=like + pool))
    tot = g[g.readout == "total"].iloc[0]
    assert tot.n_controls == 2
    assert tot.null_n == 6
    assert "null_over_control" in g.columns


@pytest.mark.parametrize("ext", [".parquet", ".tsv"])
def test_results_round_trip_exactly(pairs_path, gpr_path, tmp_path, ext):
    df = _run(pairs_path, gpr_path, [BASELINE], "ground", tmp_path)
    p = probes.write_results(df, tmp_path / f"r{ext}")
    back = probes.read_results(p)
    a = df[df.readout == "total"].value.iloc[0]
    b = back[back.readout == "total"].value.iloc[0]
    assert b == a, f"{b!r} != {a!r}"


def test_responders_cover_what_they_claim_and_leave_the_rest_as_floor():
    """The responder set is the smallest one carrying the stated share, and a run of zeros
    has no responders rather than an arbitrary first one."""
    import numpy as np
    from ecspr.model.scoring import responders

    v = np.array([10.0, 5.0, 1.0, 0.5, 0.1, 0.0])
    keep = responders(v, 0.90)
    assert v[keep].sum() / v.sum() >= 0.90
    assert v[responders(v, 0.90) & ~responders(v, 0.5)].size >= 1
    assert not responders(np.zeros(4)).any()
    assert responders(v, 1.0).sum() == 5          # the exact zero never joins
