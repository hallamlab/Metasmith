"""The flux balance arm: what a condition can say to an LP, and what it cannot."""
from __future__ import annotations

import pandas as pd
import pytest

cobra = pytest.importorskip("cobra")

from ecspr.model import conditions as C  # noqa: E402
from ecspr.model import fba as F  # noqa: E402
from ecspr.model import tests as T  # noqa: E402


def _frame():
    """glc_e -> glc_c -> bio_c -> biomass. Two governed steps and one ungoverned sink."""
    m = cobra.Model("toy")
    glc_e = cobra.Metabolite("glc_e", compartment="e")
    glc_c = cobra.Metabolite("glc_c", compartment="c")
    bio_c = cobra.Metabolite("bio_c", compartment="c")
    m.add_metabolites([glc_e, glc_c, bio_c])

    ex = cobra.Reaction("EX_glc_e", lower_bound=-10.0, upper_bound=1000.0)
    ex.add_metabolites({glc_e: -1.0})
    t = cobra.Reaction("GLCt", lower_bound=0.0, upper_bound=1000.0)
    t.add_metabolites({glc_e: -1.0, glc_c: 1.0})
    t.gene_reaction_rule = "gT"
    r1 = cobra.Reaction("R1", lower_bound=0.0, upper_bound=1000.0)
    r1.add_metabolites({glc_c: -1.0, bio_c: 1.0})
    r1.gene_reaction_rule = "g1"
    bm = cobra.Reaction("BIOMASS", lower_bound=0.0, upper_bound=1000.0)
    bm.add_metabolites({bio_c: -1.0})
    m.add_reactions([ex, t, r1, bm])
    m.objective = "BIOMASS"
    return m


@pytest.fixture
def frame(tmp_path):
    p = tmp_path / "toy.json"
    cobra.io.save_json_model(_frame(), str(p))
    return p


def _row(unit, orf, rid, mnxr, llr, channel="manual_gpr"):
    return dict(source=unit, orf=orf, gene=orf, channel=channel, mnxr=mnxr,
                intermediate_id=rid, intermediate_name=rid, llr=llr,
                llr_route="asserted", projection_via="x", evidence_quality="reviewed",
                lane_set="curated", build_id="b", host="h", genome_unit=unit,
                in_atom_universe=True)


# GLCt rests on one weakly-supported ORF, R1 on a strongly-supported one. That is the
# separation `carve_threshold` reads and `min_lanes` cannot see.
CORPUS = {"host.parquet": pd.DataFrame([
    _row("dh1", "bT", "GLCt", "MNXR1", 1.0),
    _row("dh1", "b1", "R1", "MNXR2", 6.0),
])}

BOTH = C.Condition("both", (C.Step(table="host.parquet", type="", set=(),
                                   operation="union"),))
MEDIUM = {"EX_glc_e": (-10.0, 1000.0)}


def _build(cond, frame, **kw):
    kw.setdefault("do_gapfill", False)
    return F.build(frame, cond, CORPUS, MEDIUM, **kw)


def test_an_ungoverned_reaction_survives_a_carve_that_keeps_nothing(frame):
    # Exchanges, demands, biomass and maintenance carry no gene rule, so no annotation
    # lane can speak to them. Dropping them would be a claim the evidence never made.
    empty = C.Condition("none", (C.Step(table="host.parquet", type="mnxr",
                                        set=("MNXR9",), operation="union",
                                        allow_empty=True),))
    model, census = _build(empty, frame)
    assert {r.id for r in model.reactions} == {"EX_glc_e", "BIOMASS"}
    assert census["carved_away"] == 2


def test_the_carve_keeps_what_the_condition_asserts(frame):
    model, census = _build(BOTH, frame)
    assert {"GLCt", "R1"} <= {r.id for r in model.reactions}
    assert census["growth_final"] == pytest.approx(10.0)


def test_min_lanes_counts_channels_and_reads_no_weight(frame):
    # THE POINT OF THE SECOND THRESHOLD. Every row here is one channel, so a two-lane
    # carve deletes both reactions whatever their evidence is worth -- which is why an
    # evidence-weighting change scored through `min_lanes` alone comes back as a null.
    model, census = _build(BOTH, frame, min_lanes=2)
    assert census["n_mnxr"] == 0
    assert census["growth_final"] == 0.0


def test_carve_threshold_reads_the_calibrated_weight(frame):
    # GLCt's single ORF at 1.0 nats pools to 0.106; R1's at 6.0 nats to 0.899. A floor
    # between them keeps the transporter out and the pathway dead.
    model, census = _build(BOTH, frame, carve_threshold=0.5)
    assert census["n_mnxr"] == 1
    assert "GLCt" not in {r.id for r in model.reactions}
    assert census["growth_final"] == 0.0


def test_dosage_can_put_a_reaction_into_the_model(frame):
    # THE ONLY ROUTE BY WHICH `factor` REACHES AN LP. Four more copies of the weak ORF
    # take its pooled belief across the same floor. It never moves a bound, because
    # raising a bound that was not binding is a bit-exact no-op.
    dosed = C.Condition("dosed", (
        C.Step(table="host.parquet", type="", set=(), operation="union"),
        C.Step(table="host.parquet", type="mnxr", set=("MNXR1",), operation="union",
               factor=4.0),
    ))
    model, census = _build(dosed, frame, carve_threshold=0.5)
    assert census["n_mnxr"] == 2
    assert census["growth_final"] == pytest.approx(10.0)


def test_a_medium_closes_the_exchanges_it_does_not_name():
    m = _frame()
    missing = F.apply_medium(m, {})
    assert missing == 0
    assert m.reactions.get_by_id("EX_glc_e").lower_bound == 0.0
    assert m.slim_optimize() == pytest.approx(0.0)
    assert F.apply_medium(m, {"EX_nope_e": (-1.0, 1.0)}) == 1


def test_the_growth_pin_is_a_fraction_of_the_model_s_own_maximum(frame):
    # An absolute pin would compare a carved model and its frame at different fractions
    # of their capacity, which is the comparison this arm exists to avoid.
    model, census = _build(BOTH, frame)
    t = T.Test(test_id="t", network="both", arm="gem",
               settings=dict(frame="toy", media="m", target_hub="bio_c",
                             growth_pin=0.5))
    rows = {r["readout"]: r["value"] for r in F.measure(model, t, MEDIUM, census)}
    assert rows["growth"] == pytest.approx(10.0)
    assert rows["_growth_pin"] == pytest.approx(5.0)
    # Every unit of glucose goes to biomass or to the demand, so pinning half the growth
    # leaves exactly half for the target.
    assert rows["target"] == pytest.approx(5.0)


def test_a_test_naming_an_unstaged_frame_is_refused(frame, tmp_path):
    ts = [T.Test(test_id="t", network="both", arm="gem",
                 settings=dict(frame="somewhere_else", media="m"))]
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    with pytest.raises(ValueError, match="not staged"):
        F.run([frame], [gpr], [BOTH], ts, {"m": MEDIUM}, log=lambda _m: None)


def test_a_test_naming_an_unknown_medium_is_refused(frame, tmp_path):
    ts = [T.Test(test_id="t", network="both", arm="gem",
                 settings=dict(frame="toy", media="lb"))]
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    with pytest.raises(ValueError, match="media table does not carry"):
        F.run([frame], [gpr], [BOTH], ts, {"m": MEDIUM}, log=lambda _m: None)


def test_one_build_serves_every_test_that_shares_it(frame, tmp_path):
    # A pin sweep is three tests and one carve. Rebuilding per test would triple a step
    # that is minutes on a real frame.
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    ts = [T.Test(test_id=f"pin{p}", network="both", arm="gem",
                 settings=dict(frame="toy", media="m", target_hub="bio_c",
                               growth_pin=p))
          for p in (0.9, 0.5, 0.1)]
    seen = []
    df = F.run([frame], [gpr], [BOTH], ts, {"m": MEDIUM}, do_gapfill=False,
               census_out=tmp_path / "census.json", log=seen.append)
    assert seen[-1].endswith("(1 model(s) built)")
    got = {(r.test_id, r.readout): r.value for r in df.itertuples()}
    assert got[("pin0.9", "target")] == pytest.approx(1.0)
    assert got[("pin0.1", "target")] == pytest.approx(9.0)


def test_two_conditions_that_carve_alike_share_one_build(frame, tmp_path):
    # THE EYDALLIN SHAPE. Every clone is a chromosomal E. coli ORF the host already
    # carries, so a perturbed condition changes the network and not the carve. Keying the
    # build on the condition would run 86 identical carves, insertions and gapfills.
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    clone = tmp_path / "clone.parquet"
    pd.DataFrame([_row("clone_g1", "c1", "R1", "MNXR2", 6.0)]).to_parquet(
        clone, index=False)
    # A second copy of a reaction the host already carries: a different network, the
    # same carve.
    dosed = C.Condition("clone", (
        C.Step(table="host.parquet", type="", set=(), operation="union"),
        C.Step(table="clone.parquet", type="", set=(), operation="union"),
    ))
    ts = [T.Test(test_id=c, network=c, arm="gem",
                 settings=dict(frame="toy", media="m", target_hub="bio_c",
                               growth_pin=0.1))
          for c in ("both", "clone")]
    seen = []
    df = F.run([frame], [gpr, clone], [BOTH, dosed], ts, {"m": MEDIUM}, do_gapfill=False,
               log=seen.append)
    assert seen[-1].endswith("(1 model(s) built)")
    # ...and the per-condition counts still come from the condition, so a shared build
    # cannot hide which rows went into it.
    rows = {(r.test_id, r.readout): r.value for r in df.itertuples()}
    assert rows[("both", "_n_units")] == 1 and rows[("clone", "_n_units")] == 2


def test_the_media_table_round_trips(tmp_path):
    p = F.write_media([("kornberg", "EX_glc_e", -10.0, 1000.0),
                       ("kornberg", "EX_ala__L_e", -0.35, 1000.0),
                       ("m9", "EX_glc_e", -10.0, 1000.0)],
                      tmp_path / "media.parquet")
    back = F.load_media(p)
    assert set(back) == {"kornberg", "m9"}
    assert back["kornberg"]["EX_ala__L_e"] == (-0.35, 1000.0)


def test_a_media_table_missing_a_column_is_refused(tmp_path):
    p = tmp_path / "bad.tsv"
    pd.DataFrame({"media": ["m"], "mnxr": ["MNXR1"]}).to_csv(p, sep="\t", index=False)
    with pytest.raises(ValueError, match="exchange"):
        F.load_media(p)


def test_a_curated_table_outranks_the_frame_s_own_annotation():
    # AN MNXR IS ONLY MEANINGFUL INSIDE ONE MNXref RELEASE. A BiGG model's annotation is
    # whichever release BiGG used, and on iECDH1ME8569_1439 against MetaNetX 4.5 the two
    # disagree on 1,231 of 2,751 reactions. Trusting the frame carved 625 reactions away
    # that belonged in the model, and nothing raised.
    m = _frame()
    m.reactions.get_by_id("GLCt").annotation = {F.MNXR_ANNOTATION: ["MNXR_STALE"]}
    m.reactions.get_by_id("BIOMASS").annotation = {F.MNXR_ANNOTATION: ["MNXR_ONLY"]}
    x, census = F.crosswalk(m, CORPUS)
    assert x["GLCt"] == "MNXR1", "the pinned-release table wins"
    assert x["BIOMASS"] == "MNXR_ONLY", "and the frame fills in what no table covers"
    assert census["crosswalk_disagree"] == 1


def test_a_de_novo_corpus_with_no_crosswalk_is_refused():
    # WHAT THIS GUARD IS FOR. Keeping only the ungoverned reactions is not a deep carve,
    # it is an empty crosswalk -- and gapfill then repairs the wreckage back into
    # something that reports plausible numbers.
    m = _frame()
    denovo = {"lanes.parquet": pd.DataFrame([
        _row("dh1", "b1", "K00001", "MNXR2", 6.0, channel="kofam")])}
    a = F.assertions(C.Condition("x", (C.Step(table="lanes.parquet", type="", set=(),
                                              operation="union"),)), denovo)
    with pytest.raises(ValueError, match="not one of them reaches this frame"):
        F.carve(m, a, F.crosswalk(m, denovo)[0])


def test_results_mirror_the_conductance_schema(frame, tmp_path):
    gpr = tmp_path / "host.parquet"
    CORPUS["host.parquet"].to_parquet(gpr, index=False)
    ts = [T.Test(test_id="t", network="both", arm="gem",
                 settings=dict(frame="toy", media="m", target_hub="bio_c",
                               growth_pin=0.1))]
    df = F.run([frame], [gpr], [BOTH], ts, {"m": MEDIUM}, do_gapfill=False,
               log=lambda _m: None)
    assert list(df.columns) == list(F.RESULT_COLUMNS)
    assert (df.readout.str.startswith("_")).any(), \
        "a result must carry its own diagnostics, so it cannot be read without them"
